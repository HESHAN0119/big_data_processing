"""
Kafka Producer: File Watcher
Monitors the data/ folder for new CSV files and sends them to Kafka
"""

import os
import time
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka import KafkaProducer
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
# WATCH_FOLDER = os.getenv('WATCH_FOLDER', './data')

class FileWatcherHandler(FileSystemEventHandler):
    """Handler for file system events"""

    def __init__(self, producer, topic, data_folder):
        self.producer = producer
        self.topic = topic
        self.data_folder = data_folder
        self.processed_files = set()

    def on_created(self, event):
        """Called when a file is created"""
        if event.is_directory:
            return

        file_path = event.src_path

        # Only process CSV files
        if not file_path.endswith('.csv'):
            logger.info(f"Ignoring non-CSV file: {file_path}")
            return

        # Avoid duplicate processing
        if file_path in self.processed_files:
            return

        logger.info(f"New file detected: {file_path}")

        # Wait a moment to ensure file is fully written
        time.sleep(1)

        try:
            self.send_file_to_kafka(file_path)
            self.processed_files.add(file_path)
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")

    def send_file_to_kafka(self, file_path):
        """Read CSV file and send to Kafka line by line"""
        filename = os.path.basename(file_path)

        logger.info(f"Reading file: {filename}")

        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Send metadata first
        metadata = {
            'type': 'metadata',
            'filename': filename,
            'total_lines': len(lines),
            'timestamp': time.time()
        }

        self.producer.send(
            self.topic,
            key=filename.encode('utf-8'),
            value=json.dumps(metadata).encode('utf-8')
        )

        logger.info(f"Sent metadata for {filename} ({len(lines)} lines)")

        # Send each line as a separate message
        for i, line in enumerate(lines):
            message = {
                'type': 'data',
                'filename': filename,
                'line_number': i,
                'content': line.strip(),
                'timestamp': time.time()
            }

            self.producer.send(
                self.topic,
                key=filename.encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
            )

            # Log progress every 1000 lines
            if (i + 1) % 1000 == 0:
                logger.info(f"Sent {i + 1}/{len(lines)} lines from {filename}")

        # Send end marker
        end_marker = {
            'type': 'end',
            'filename': filename,
            'total_lines': len(lines),
            'timestamp': time.time()
        }

        self.producer.send(
            self.topic,
            key=filename.encode('utf-8'),
            value=json.dumps(end_marker).encode('utf-8')
        )

        self.producer.flush()
        logger.info(f"Completed sending {filename} to Kafka topic '{self.topic}'")


def main():
    """Main function to start file watcher"""

        # Configuration
    KAFKA_BROKER = 'localhost:9093'
    KAFKA_TOPIC = 'weather-data-stream'
    DATA_FOLDER = './data'

    logger.info("Starting File Watcher Producer")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"Watching folder: {DATA_FOLDER}")

    # Create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=None,  # We'll handle serialization manually
            max_request_size=10485760,  # 10 MB max message size
            compression_type='gzip'
        )
        logger.info("Kafka producer created successfully")
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return

    # Create file watcher handler
    event_handler = FileWatcherHandler(producer, KAFKA_TOPIC, DATA_FOLDER)

    # Create observer
    observer = Observer()
    observer.schedule(event_handler, DATA_FOLDER, recursive=False)
    observer.start()

    logger.info(f"File watcher started. Monitoring {DATA_FOLDER} for new CSV files...")
    logger.info("Press Ctrl+C to stop")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping file watcher...")
        observer.stop()
        observer.join()
        producer.close()
        logger.info("File watcher stopped")


if __name__ == "__main__":
    main()
