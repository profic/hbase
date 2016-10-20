 Implementation with coprocessor:

 mvn clean
 mvn install -DskipTests=true


 sudo -u hdfs hadoop fs -rm /hbase-task.jar
 sudo -u hdfs hadoop fs -put /<path>/target/hbase-task.jar /<hdfs path>


 create 'stats', 'data'
 alter 'stats', 'Coprocessor' => '/<hdfs path>/hbase-task.jar|hbase.coprocessor.StatisticsObserver|'

