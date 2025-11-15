"""
Kafka Consumer: HDFS Writer
Consumes messages from Kafka and writes them to HDFS
"""

import json
import subprocess
import tempfile
import os
from kafka import KafkaConsumer
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
# KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9093')
# KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'weather-data-stream')
# KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'hdfs-writer-group')
# HDFS_PATH = os.getenv('HDFS_PATH', '/user/data/kafka_ingested')

class HDFSWriter:
    """Handles writing data to HDFS"""

    def __init__(self, hdfs_base_path='/user/data/kafka_ingested'):
        self.hdfs_base_path = hdfs_base_path
        self.current_file = None
        self.current_filename = None
        self.buffer = []
        self.current_hdfs_path = None
        self.weather_files_written = []  # Track weather files for automation

    def start_file(self, filename):
        """Start buffering a new file"""
        self.current_filename = filename
        self.buffer = []

        # Determine HDFS path based on filename
        if 'location' in filename.lower():
            self.current_hdfs_path = f"{self.hdfs_base_path}/location"
            logger.info(f"Started buffering LOCATION file: {filename} -> {self.current_hdfs_path}")
        elif 'weather' in filename.lower():
            self.current_hdfs_path = f"{self.hdfs_base_path}/weather"
            logger.info(f"Started buffering WEATHER file: {filename} -> {self.current_hdfs_path}")
        else:
            # Default to weather folder for backward compatibility
            self.current_hdfs_path = f"{self.hdfs_base_path}/weather"
            logger.warning(f"File type not detected, using default weather path: {filename}")

    def add_line(self, content):
        """Add a line to the buffer"""
        self.buffer.append(content)

    def write_to_hdfs(self):
        """Write buffered content to HDFS"""
        if not self.buffer or not self.current_filename or not self.current_hdfs_path:
            return

        logger.info(f"Writing {len(self.buffer)} lines to HDFS for {self.current_filename}")

        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_file:
            tmp_path = tmp_file.name
            for line in self.buffer:
                tmp_file.write(line + '\n')

        try:
            # Copy to HDFS via docker exec
            hdfs_dest = f"{self.current_hdfs_path}/{self.current_filename}"

            # First, copy file to namenode container
            docker_copy_cmd = [
                'docker', 'cp',
                tmp_path,
                f'namenode:/tmp/{self.current_filename}'
            ]

            subprocess.run(docker_copy_cmd, check=True)

            # Then, copy from namenode container to HDFS
            # Use proper quoting for filenames with spaces
            hdfs_put_cmd = [
                'docker', 'exec', 'namenode', 'bash', '-c',
                f'hdfs dfs -mkdir -p {self.current_hdfs_path} && '
                f'hdfs dfs -put -f "/tmp/{self.current_filename}" "{hdfs_dest}"'
            ]

            subprocess.run(hdfs_put_cmd, check=True)

            logger.info(f"Successfully wrote {self.current_filename} to HDFS: {hdfs_dest}")

            # Track weather files for automation
            if 'weather' in self.current_hdfs_path.lower():
                self.weather_files_written.append(self.current_filename)
                logger.info(f"Weather file tracked for processing: {self.current_filename}")

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to write to HDFS: {e}")
        finally:
            # Clean up temporary file
            os.unlink(tmp_path)

        # Clear buffer
        self.buffer = []
        self.current_filename = None
        self.current_hdfs_path = None

    def run_hive(self):
        """Run Hive analysis after data ingestion"""
        logger.info("="*60)
        logger.info("Starting Hive Analysis Pipeline")
        logger.info("="*60)

        # Get project root directory (2 levels up from src/kafka/)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

        try:
            # Run Hive analysis script
            logger.info("Executing Hive analysis...")
            hive_script = os.path.join(project_root, 'run_hive_analysis_simple.py')

            result = subprocess.run(
                ['python', hive_script],
                cwd=project_root,
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                logger.info("Hive analysis completed successfully")
                logger.info(result.stdout)
                logger.info("="*60)
                logger.info("HIVE ANALYSIS COMPLETED!")
                logger.info("="*60)
            else:
                logger.error(f"Hive analysis failed: {result.stderr}")

        except Exception as e:
            logger.error(f"Error running Hive analysis: {e}")

        logger.info("Resuming Kafka consumption...")

    def run_processing_pipeline(self):
        """Run MapReduce and ClickHouse loading pipeline after weather data ingestion"""
        if not self.weather_files_written:
            logger.info("No weather files to process")
            return

        logger.info("="*60)
        logger.info(f"Starting automated processing pipeline for {len(self.weather_files_written)} weather file(s)")
        logger.info("="*60)

        # Get project root directory (2 levels up from src/kafka/)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

        print("project_root********",project_root)

        try:
            # Step 1: Run MapReduce jobs
            # logger.info("[Step 1/2] Running MapReduce jobs...")
            mapreduce_script = os.path.join(project_root, '.\compile_and_run_mapreduce.ps1')

            result1 = subprocess.run(
                ['powershell', '-ExecutionPolicy', 'Bypass', '-File', mapreduce_script],
                cwd=project_root,
                capture_output=True,
                text=True
            )

            if result1.returncode == 0:
                logger.info("[Step 1/2] MapReduce jobs completed successfully")
                logger.info(result1.stdout)

                # Step 2: Load results to ClickHouse
                logger.info("[Step 2/2] Loading results to ClickHouse...")
                loader_script = os.path.join(project_root, '.\load_mapreduce_output.py')

                result2 = subprocess.run(
                    ['python', loader_script],
                    cwd=project_root,
                    capture_output=True,
                    text=True
                )

                if result2.returncode == 0:
                    logger.info("[Step 2/2] Data loading completed successfully")
                    logger.info(result2.stdout)
                    logger.info("="*60)
                    logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
                    logger.info("="*60)
                else:
                    logger.error(f"[Step 2/2] Data loading failed: {result2.stderr}")
            else:
                logger.error(f"[Step 1/2] MapReduce jobs failed: {result1.stderr}")

            # Clear tracked files after processing
            self.weather_files_written = []

        except Exception as e:
            logger.error(f"Error running processing pipeline: {e}")

        logger.info("Resuming Kafka consumption...")


def main():
    """Main function to start Kafka consumer"""

        # Configuration
    KAFKA_BROKER = 'localhost:9093'
    KAFKA_TOPIC = 'weather-data-stream'
    KAFKA_GROUP_ID = 'hdfs-writer-group'
    HDFS_PATH = '/user/data/kafka_ingested'

    logger.info("Starting Kafka to HDFS Consumer")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"HDFS Path: {HDFS_PATH}")

    # Create Kafka consumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logger.info("Kafka consumer created successfully")
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return

    # Create HDFS writer
    hdfs_writer = HDFSWriter(HDFS_PATH)

    logger.info("Listening for messages... Press Ctrl+C to stop")

    try:
        for message in consumer:
            data = message.value

            msg_type = data.get('type')
            filename = data.get('filename')

            if msg_type == 'metadata':
                logger.info(f"Received metadata for {filename}: {data['total_lines']} lines")
                hdfs_writer.start_file(filename)

            elif msg_type == 'data':
                content = data.get('content')
                hdfs_writer.add_line(content)

                # Log progress
                line_num = data.get('line_number', 0)
                if (line_num + 1) % 1000 == 0:
                    logger.info(f"Buffered {line_num + 1} lines from {filename}")

            elif msg_type == 'end':
                logger.info(f"Received end marker for {filename}")
                hdfs_writer.write_to_hdfs()

                # If this was a weather file, trigger the processing pipelines
                if 'weather' in filename.lower():
                    logger.info(f"Weather file ingestion completed: {filename}")

                    # Step 1: Run Hive analysis first
                    logger.info("Triggering Hive analysis pipeline...")
                    hdfs_writer.run_hive()

                    # Step 2: Run MapReduce pipeline
                    logger.info("Triggering MapReduce processing pipeline...")
                    hdfs_writer.run_processing_pipeline()

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
        logger.info("Consumer stopped")


if __name__ == "__main__":
    main()
