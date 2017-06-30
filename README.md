GridDB connector for Apache Spark

## Overview

GridDB connector for [Apache Spark](https://spark.apache.org/) is a module supporting connection between GridDB and Apache Spark. 
This uses GridDB server, GridDB Java client, and GridDB connector for [Apache Hadoop](http://hadoop.apache.org/) MapReduce.
We can create DataFrame from an existing GridDB container and operate with it.

## Operating environment

Library building and program execution are checked in the environment below.

    OS:             CentOS6.7(x64)
    Java:           JDK 1.8.0_101
	Apache Hadoop:  Version 2.6.5
	Apache Spark:   Version 2.1.0
	Scala:          Version 2.11.8
	
    GridDB server and Java client:                3.0 CE
    GridDB connector for Apache Hadoop MapReduce: 1.0

## QuickStart
### Preparations
1. Install Hadoop and Spark

		$ cd [INSTALL_FOLDER]
		$ wget http://archive.apache.org/dist/hadoop/core/hadoop-2.6.5/hadoop-2.6.5.tar.gz
		$ tar xvfz hadoop-2.6.5.tar.gz
		$ wget http://archive.apache.org/dist/spark/spark-2.1.0/spark-2.1.0-bin-hadoop2.6.tgz
		$ tar xvfz spark-2.1.0-bin-hadoop2.6.tgz

    Note: [INSTALL_FOLDER] means the folder installed for Spark, Hadoop and GridDB connector for Spark.

2. Please add the following environment variables to .bashrc
		
		$ vi ~/.bashrc
		export JAVA_HOME=/usr/lib/jvm/[JDK folder]
		export HADOOP_HOME=[INSTALL_FOLDER]/hadoop-2.6.5
		export SPARK_HOME=[INSTALL_FOLDER]/spark-2.1.0-bin-hadoop2.6
		export GRIDDB_SPARK=[INSTALL_FOLDER]/griddb_spark
		export GRIDDB_SPARK_PROPERTIES=$GRIDDB_SPARK/gd-config.xml
		
		export PATH=$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH
		
		export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
		export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"

		$ source ~/.bashrc

3. Please modify file "gd-config.xml"

		$ cd [INSTALL_FOLDER]/griddb_spark
		$ vi gd-config.xml
		
		<!-- GridDB properties -->
		<property>
			<name>gs.user</name>
			<value>[GridDB user]</value>
		</property>
		<property>
			<name>gs.password</name>
			<value>[GridDB password]</value>
		</property>
		<property>
			<name>gs.cluster.name</name>
			<value>[GridDB cluster name]</value>
		</property>
		<!-- Define address and port for multicast method, leave it blank if using other method -->
		<property>
			<name>gs.notification.address</name>
			<value>[GridDB notification address(default is 239.0.0.1)]</value>
		</property>
		<property>
			<name>gs.notification.port</name>
			<value>[GridDB notification port(default is 31999)]</value>
		</property>

Please refer to [Configuration](Configuration.md) for GridDB properties.

4. Build a GridDB Java client and a GridDB connector for Hadoop MapReduce,  
   place the following files under the griddb_spark/gs-spark-datasource/lib directory.

    gridstore.jar  
    gs-hadoop-mapreduce-client-1.0.0.jar

5. Add SPARK_CLASSPATH to "spark-env.sh"
		
		$ cd [INSTALL_FOLDER]/spark-2.1.0-bin-hadoop2.6
		$ vi conf/spark-env.sh
		SPARK_CLASSPATH=.:$GRIDDB_SAPRK/gs-spark-datasource/target/gs-spark-datasource.jar:
			$GRIDDB_SAPRK/gs-spark-datasource/lib/gridstore.jar:
			$GRIDDB_SAPRK/gs-spark-datasource/lib/gs-hadoop-mapreduce-client-1.0.0.jar

### Build the connector and an example

Run the mvn command like the following:

	$ cd [INSTALL_FOLDER]/griddb_spark
	$ mvn package

and create the following jar files. 

	gs-spark-datasource/target/gs-spark-datasource.jar
	gs-spark-datasource-example/target/example.jar

### Run the example program

GridDB cluster needs to be started in advance.

1. Put data to server with GridDB Java client

		$ cd [INSTALL_FOLDER]/griddb_spark
		$ java -cp ./gs-spark-datasource-example/target/example.jar:gs-spark-datasource/lib/gridstore.jar 
			Init <GridDB notification address> <GridDB notification port>
						<GridDB cluster name> <GridDB user> <GridDB password>

2. Run some queries with GridDB connector for Spark

		$ spark-submit --class Query ./gs-spark-datasource-example/target/example.jar

## API

With a SparkSession, applications can create DataFrames from an existing GridDB container in the form as bellow.

    var df = session.read.format("com.toshiba.mwcloud.gs.spark.datasource").load(containerName)

## Community

  * Issues  
    Use the GitHub issue function if you have any requests, questions, or bug reports. 
  * PullRequest  
    Use the GitHub pull request function if you want to contribute code.
    You'll need to agree GridDB Contributor License Agreement(CLA_rev1.1.pdf).
    By using the GitHub pull request function, you shall be deemed to have agreed to GridDB Contributor License Agreement.

## License
  
  The GridDB connector source is licensed under the Apache License, version 2.0.
  
## Trademarks
  
  Apache Spark, Apache Hadoop, Spark, and Hadoop are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
