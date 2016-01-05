echo 'reassembling and uploading'
# reassembling
mvn -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests package
cd target/scala-2.10/classes
zip -u -r /home/w/dist/spark-1.5.2-bin-hadoop2.6/lib/spark-assembly-1.5.2-hadoop2.6.0.jar org/apache/spark/sql/execution org/apache/spark/sql/jdbc
#zip -u -r /home/w/dist/spark-1.5.2-bin-hadoop2.6/lib/spark-assembly-1.5.2-hadoop2.6.0.jar org/apache/spark/sql/execution/datasources/DefaultSoure\* eorg/apache/spark/sql/execution/jdbc/JDBCRDD\* org/apache/spark/sql/jdbc/JdbcDialec\*

cd ../../../

scp /home/w/dist/spark-1.5.2-bin-hadoop2.6/lib/spark-assembly-1.5.2-hadoop2.6.0.jar root@m108:/home/liudepeng/spark-1.5.2-bin-hadoop2.6/lib/
scp /home/w/dist/spark-1.5.2-bin-hadoop2.6/lib/spark-assembly-1.5.2-hadoop2.6.0.jar root@m107:/home/liudepeng/spark-1.5.2-bin-hadoop2.6/lib/

echo 'thrift server restarting...'

echo 'testing with beeline'

#ssh root@m108 "cd /home/liudepeng/spark-1.5.2-bin-hadoop2.6/sbin && ./stop-thriftserver.sh && ./start-thriftserver.sh --master yarn-client --executor-memory 4g --executor-cores 1 --num-executors 4 --queue default --driver-class-path /tmp/sciue-connector-java-3.0.bin.jar:/usr/share/java/mysql-connector-java.jar"

echo 'beeline running on backend ---'
#ssh root@m108 "beeline -f /home/liudepeng/beeline.sh"

#scp root@m108:/home/liudepeng/spark-1.5.2-bin-hadoop2.6/sbin/../logs/spark-root-org.apache.spark.sql.hive.thriftserver.HiveThriftServer2-1-m108.out logs/

echo 'Check report please.'
