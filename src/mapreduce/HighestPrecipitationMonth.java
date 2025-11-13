import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * MapReduce job to find the month and year with the highest total precipitation
 * in the full dataset.
 *
 * Output format: Year-Month\tTotal Precipitation (hours)
 * Example: 2019-02    1234.5
 */
public class HighestPrecipitationMonth {

    // Mapper: Extract year-month and precipitation hours
    public static class PrecipitationMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text yearMonth = new Text();
        private DoubleWritable precipHours = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // Skip header
            if (line.startsWith("location_id")) {
                return;
            }

            String[] fields = line.split(",");
            if (fields.length >= 13) {
                try {
                    String date = fields[1].trim(); // Format: M/D/YYYY
                    double precip = Double.parseDouble(fields[12].trim());

                    // Extract year and month from date
                    String[] dateParts = date.split("/");
                    if (dateParts.length == 3) {
                        String month = String.format("%02d", Integer.parseInt(dateParts[0]));
                        String year = dateParts[2];
                        String ym = year + "-" + month;

                        yearMonth.set(ym);
                        precipHours.set(precip);

                        // Emit: yearMonth -> precipitationHours
                        context.write(yearMonth, precipHours);
                    }
                } catch (NumberFormatException e) {
                    // Skip malformed records
                }
            }
        }
    }

    // Combiner: Sum precipitation hours for each year-month locally
    public static class PrecipitationCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    // Reducer: Sum all precipitation hours for each year-month and find the maximum
    public static class PrecipitationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private Text maxYearMonth = new Text();
        private double maxPrecip = Double.MIN_VALUE;

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            // Track the maximum
            if (sum > maxPrecip) {
                maxPrecip = sum;
                maxYearMonth.set(key);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit only the maximum result
            context.write(maxYearMonth, new DoubleWritable(maxPrecip));
        }
    }

    /**
     * Check if a directory exists and contains actual data files
     */
    private static boolean isDirectoryNotEmpty(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            System.err.println("ERROR: Path does not exist: " + path);
            return false;
        }

        if (!fs.isDirectory(path)) {
            System.err.println("ERROR: Path is not a directory: " + path);
            return false;
        }

        // Check if directory has any files (recursively)
        RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(path, true);
        if (!fileIterator.hasNext()) {
            System.err.println("ERROR: Directory exists but contains no files: " + path);
            return false;
        }

        // Check if there's at least one non-empty data file
        int fileCount = 0;
        long totalSize = 0;
        while (fileIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileIterator.next();
            if (fileStatus.isFile() && fileStatus.getLen() > 0) {
                fileCount++;
                totalSize += fileStatus.getLen();
                System.out.println("  Found file: " + fileStatus.getPath().getName() +
                                 " (" + fileStatus.getLen() + " bytes)");
            }
        }

        if (fileCount == 0) {
            System.err.println("ERROR: Directory contains no valid data files: " + path);
            return false;
        }

        System.out.println("  Total: " + fileCount + " file(s), " + totalSize + " bytes");
        return true;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // args[0] = weather data HDFS path: /user/data/kafka_ingested/weather
        // args[1] = output path

        Path weatherPath = new Path(args[0]);

        // Validate input directory
        System.out.println("Validating input directory...");
        boolean weatherValid = isDirectoryNotEmpty(fs, weatherPath);

        if (!weatherValid) {
            System.err.println("\n========================================");
            System.err.println("ERROR: Input validation failed!");
            System.err.println("========================================");
            System.err.println("  - Weather data is missing or empty: " + args[0]);
            System.err.println("\nJob aborted. No output will be created.");
            System.err.println("========================================");
            fs.close();
            System.exit(1);
        }

        System.out.println("[OK] Weather data directory is valid: " + args[0]);
        System.out.println("\nProceeding with MapReduce job...\n");

        Job job = Job.getInstance(conf, "Highest Precipitation Month");

        job.setJarByClass(HighestPrecipitationMonth.class);

        job.setMapperClass(PrecipitationMapper.class);
        job.setCombinerClass(PrecipitationCombiner.class);
        job.setReducerClass(PrecipitationReducer.class);

        // Set single reducer to find global maximum
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, weatherPath);

        // Create timestamped output path with format: hpm_YYYYMMDDHHmmss
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String timestamp = dateFormat.format(new Date());
        String outputPath = args[1] + "/hpm_" + timestamp;
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        fs.close();
        System.exit(success ? 0 : 1);
    }
}
