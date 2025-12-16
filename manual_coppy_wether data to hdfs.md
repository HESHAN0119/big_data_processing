docker cp "f:\MSC BIG DATA\sep sem\big data processing\assignment\project\data\locationData_3.csv" namenode:/tmp/locationData_3.csv
docker exec namenode hadoop fs -put -f /tmp/locationData_3.csv /user/data/kafka_ingested/location/locationData_3.csv
docker exec namenode hadoop fs -ls /user/data/kafka_ingested/location/
