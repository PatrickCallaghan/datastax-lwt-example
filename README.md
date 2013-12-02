Light Weight Transaction Example
====================

NOTE - this example requires apache cassandra version > 2.0 and the cassandra-driver-core version > 2.0.0

The current pom.xml uses the beta2 version and works fine with cassandra 2.0.1. 

To test run

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.lwt.LwtExample"
