0) Build:

 mvn clean
 mvn install -DskipTests=true

 It's important to skip test because it tests hbase with coprocessor,
 which is not created and assigned yet.

1) Implementation with coprocessor:

 sudo -u hdfs hadoop fs -put /<path>/target/hbase-task.jar /<hdfs path>

 create 'stats', 'data'
 alter 'stats', 'Coprocessor' => '/<hdfs path>/hbase-task.jar|hbase.coprocessor.StatisticsObserver|'

See StatisticCoprocessorTest - it shows insert and get process

2) Implementation with spark:

!! Change checkpointDir  in SparkEventProcessorto yours hdfs path

spark-submit --master local --class hbase.SparkEventProcessor target/hbase-task.jar
It will run random generation values.

It's possible to send commands from server:
- run 'nc -l localhost 9999' from terminal
- run spark-submit --master local --class hbase.SparkEventProcessor target/hbase-task.jar network
- type in terminal tuples like '1 10', '1 20'

Each 3 seconds (configurable, val emitInterval = Seconds(3) in SparkEventProcessor)
spark will write updated or new values to hbase and print them to console