import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * MapReduce job to calculate total precipitation and mean temperature
 * for each district per month over the past decade.
 *
 * Output format: District\tYear-Month\tTotal Precipitation (hours)\tMean Temperature (C)
 * Example: Colombo    2010-01    30.5    26.3
 */
public class DistrictMonthlyWeather {

    // Mapper for location data - builds location_id to city_name mapping
    public static class LocationMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // Skip header
            if (line.startsWith("location_id")) {
                return;
            }

            String[] fields = line.split(",");
            if (fields.length >= 8) {
                String locationId = fields[0].trim();
                String cityName = fields[7].trim();

                // Emit: location_id -> L:city_name (L prefix for location data)
                context.write(new Text(locationId), new Text("L:" + cityName));
            }
        }
    }

    // Mapper for weather data
    public static class WeatherMapper extends Mapper<Object, Text, Text, Text> {

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
                    String locationId = fields[0].trim();
                    String date = fields[1].trim(); // Format: M/D/YYYY
                    double meanTemp = Double.parseDouble(fields[5].trim());
                    double precipHours = Double.parseDouble(fields[12].trim());

                    // Extract year and month from date
                    String[] dateParts = date.split("/");
                    if (dateParts.length == 3) {
                        String month = String.format("%02d", Integer.parseInt(dateParts[0]));
                        String year = dateParts[2];
                        String yearMonth = year + "-" + month;

                        // Emit: location_id -> W:yearMonth:precipHours:meanTemp (W prefix for weather data)
                        String weatherData = String.format("W:%s:%.2f:%.2f",
                                yearMonth, precipHours, meanTemp);
                        context.write(new Text(locationId), new Text(weatherData));
                    }
                } catch (NumberFormatException e) {
                    // Skip malformed records
                }
            }
        }
    }

    // Reducer to join location and weather data, then aggregate by district-month
    public static class WeatherReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String cityName = null;
            Map<String, Double> totalPrecip = new HashMap<>();
            Map<String, Double> totalTemp = new HashMap<>();
            Map<String, Integer> count = new HashMap<>();

            // Process all values for this location_id
            for (Text val : values) {
                String value = val.toString();

                if (value.startsWith("L:")) {
                    // Location data
                    cityName = value.substring(2);
                } else if (value.startsWith("W:")) {
                    // Weather data: W:yearMonth:precipHours:meanTemp
                    String[] parts = value.substring(2).split(":");
                    if (parts.length == 3) {
                        String yearMonth = parts[0];
                        double precipHours = Double.parseDouble(parts[1]);
                        double meanTemp = Double.parseDouble(parts[2]);

                        totalPrecip.put(yearMonth,
                                totalPrecip.getOrDefault(yearMonth, 0.0) + precipHours);
                        totalTemp.put(yearMonth,
                                totalTemp.getOrDefault(yearMonth, 0.0) + meanTemp);
                        count.put(yearMonth,
                                count.getOrDefault(yearMonth, 0) + 1);
                    }
                }
            }

            // Emit results for each district-month combination
            if (cityName != null) {
                for (String yearMonth : totalPrecip.keySet()) {
                    double avgTemp = totalTemp.get(yearMonth) / count.get(yearMonth);
                    double precip = totalPrecip.get(yearMonth);

                    String outputKey = cityName + "\t" + yearMonth;
                    String outputValue = String.format("%.2f\t%.2f", precip, avgTemp);

                    context.write(new Text(outputKey), new Text(outputValue));
                }
            }
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

        // Multiple input paths with different mappers
        // args[0] = location HDFS path: /user/data/kafka_ingested/location
        // args[1] = weather HDFS path: /user/data/kafka_ingested/weather
        // args[2] = output path

        Path locationPath = new Path(args[0]);
        Path weatherPath = new Path(args[1]);

        // Validate input directories
        System.out.println("Validating input directories...");
        boolean locationValid = isDirectoryNotEmpty(fs, locationPath);
        boolean weatherValid = isDirectoryNotEmpty(fs, weatherPath);

        if (!locationValid || !weatherValid) {
            System.err.println("\n========================================");
            System.err.println("ERROR: Input validation failed!");
            System.err.println("========================================");
            if (!locationValid) {
                System.err.println("  - Location data is missing or empty: " + args[0]);
            }
            if (!weatherValid) {
                System.err.println("  - Weather data is missing or empty: " + args[1]);
            }
            System.err.println("\nJob aborted. No output will be created.");
            System.err.println("========================================");
            System.exit(1);
        }

        System.out.println("[OK] Location data directory is valid: " + args[0]);
        System.out.println("[OK] Weather data directory is valid: " + args[1]);
        System.out.println("\nProceeding with MapReduce job...\n");

        Job job = Job.getInstance(conf, "District Monthly Weather Analysis");

        job.setJarByClass(DistrictMonthlyWeather.class);

        // Set reducer
        job.setReducerClass(WeatherReducer.class);

        // Set output key-value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, locationPath,
                TextInputFormat.class, LocationMapper.class);
        MultipleInputs.addInputPath(job, weatherPath,
                TextInputFormat.class, WeatherMapper.class);

        // Create timestamped output path with format: mean_temp_YYYYMMDDHHmmss
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String timestamp = dateFormat.format(new Date());
        String outputPath = args[2] + "/mean_temp_" + timestamp;
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        fs.close();
        System.exit(success ? 0 : 1);
    }
}
