/*
   Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.toshiba.mwcloud.gs.spark

// Java Lib
import java.util.Properties
import java.io.FileInputStream
import java.io.File

import scala.collection.immutable.Seq

// Apache Spark Lib
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.catalyst.analysis.Analyzer

// Apache Hadoop Lib
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job

// GridDB Connector for Apache Hadoop MapReduce
import com.toshiba.mwcloud.gs.hadoop.io.GSRowWritable
import com.toshiba.mwcloud.gs.hadoop.io.GSColumnKeyWritable
import com.toshiba.mwcloud.gs.hadoop.mapreduce.GSRowInputFormat
import com.toshiba.mwcloud.gs.hadoop.mapreduce.GSRowOutputFormat

// GridDB Java Client
import com.toshiba.mwcloud.gs.GridStore
import com.toshiba.mwcloud.gs.GridStoreFactory
import com.toshiba.mwcloud.gs.GSType
import com.toshiba.mwcloud.gs.GSType._
import com.toshiba.mwcloud.gs.ContainerInfo
import com.toshiba.mwcloud.gs.ContainerType
import com.toshiba.mwcloud.gs.ColumnInfo

@DeveloperApi
class GridDBUtils[K, V](sc: SparkContext) extends RDD[(K, V)](sc,Nil) {
  
  val ENV_PROPERTIES = "GRIDDB_SPARK_PROPERTIES"
    
  /**
   * createRDD
   *   containerName : String
   */
  def createRDD(containerName: String) : RDD[GSRowWritable] = {
    val griddbConf = new Configuration()

    val env = System.getenv(ENV_PROPERTIES)
    if (env != null) {
        griddbConf.addResource(new Path(env))
    }
    System.out.println("griddbConf=" + griddbConf.toString())
    griddbConf.set("gs.input.container.name.regex", containerName)
    if (sc.getConf.contains("gs.database")) {
      griddbConf.set("gs.database", sc.getConf.get("gs.database"))
    }
    sc.newAPIHadoopRDD(griddbConf, classOf[GSRowInputFormat], classOf[GSColumnKeyWritable], classOf[GSRowWritable]).values
  }

  /**
   * createDataFrame
   *   sqlContext : SQLContext
   *   containerName : String
   */
  def createDataFrame(sparkSession: SparkSession, containerName: String) : DataFrame = {
     val griddbDF = sparkSession.read.format("com.toshiba.mwcloud.gs.spark.datasource").load(containerName)
     griddbDF.createOrReplaceTempView(containerName)
     griddbDF
  }
  
  /**
   * save
   *   containerName : String
   *   saveDataFrame : DataFrame
   */
  def save(containerName: String, saveDataFrame: DataFrame) : Unit = {
    putContainer(containerName, saveDataFrame, false)
  }
  
  /**
   * save
   *   containerName : String
   *   saveDataFrame : DataFrame
   *   rowKey : Boolean
   */
  def save(containerName: String, saveDataFrame: DataFrame, rowKey: Boolean) : Unit = {
    putContainer(containerName, saveDataFrame, rowKey)
  }
  
  /**
   * delete
   *   containerName : String
   */
  def delete(containerName: String) : Unit = {
    val griddb = connectGridDB
    griddb.dropContainer(containerName)
    griddb.close()
  }
  
  /**
   * createTable
   *   sql : String
   *   sparkSession : SparkSession
   */
  def createTable(sql: String, sparkSession: SparkSession): String = {
    try {
      val plan = CatalystSqlParser.parsePlan(sql)
      plan resolveOperators {
        case i @ InsertIntoTable(u: UnresolvedRelation, parts, child, _, _) =>
          val table = u.tableIdentifier.table
          val datasource = u.tableIdentifier.database
        
          if ((datasource == null || datasource == None) && table != null && table != None) {
            createDataFrame(sparkSession, table)
          }
          i
          
        case u: UnresolvedRelation =>
          val table = u.tableIdentifier.table
          val datasource = u.tableIdentifier.database
        
          if ((datasource == null || datasource == None) && table != null && table != None) {
            createDataFrame(sparkSession, table)
          }
          u
      }
      "Success"
    } catch {
      case e:Exception =>
        e.getMessage
    }
  }
  
  /**
   * putContainer
   *   containerName : String
   *   saveDataFrame : DataFrame
   *   rowKey : Boolean
   */
  private def putContainer(containerName: String, saveDataFrame: DataFrame, rowKey: Boolean) {
    val griddbConf = new Configuration()

    val env = System.getenv(ENV_PROPERTIES)
    if (env != null) {
        griddbConf.addResource(new Path(env))
    }

    if (sc.getConf.contains("gs.database")) {
      griddbConf.set("gs.database", sc.getConf.get("gs.database"))
    }
    griddbConf.set("gs.output.container.name", containerName)
    
    val dtypes = saveDataFrame.dtypes
    var types:Array[GSType] = Array[GSType](getGsType(dtypes(0)._2))
    var columnNames:String = dtypes(0)._1
    var columnTypes:String = String.valueOf(getGsType(dtypes(0)._2))
    for(i <- 1 to dtypes.length - 1) {
      types = types :+  getGsType(dtypes(i)._2)
      columnNames += "," + dtypes(i)._1
      columnTypes += "," + String.valueOf(getGsType(dtypes(i)._2))
    }

    if (types(0) == GSType.TIMESTAMP) {
      griddbConf.set("gs.container.type", "TIME_SERIES")
      griddbConf.set("gs.schema.column.key", dtypes(0)._1)
    } else {
      griddbConf.set("gs.container.type", "COLLECTION")
      if (rowKey) {
        griddbConf.set("gs.schema.column.key", dtypes(0)._1)
      }
    }
    
    griddbConf.set("gs.schema.column.names", columnNames)
    griddbConf.set("gs.schema.column.types", columnTypes)
    
    val job = new Job(griddbConf)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[GSRowWritable])
    job.setOutputFormatClass(classOf[GSRowOutputFormat])

    val rdd = saveDataFrame.rdd.map(row => {
        val gsRow = new GSRowWritable(types)
        for (i <- 0 to types.length - 1) {
          gsRow.setValue(i, row.get(i))
        }
        gsRow
      }
    )
    
    rdd.keyBy(key => NullWritable.get()).saveAsNewAPIHadoopDataset(job.getConfiguration())
  }
  
  /**
   * connectGridDB
   */
  def connectGridDB() : GridStore = {
    val griddbConf = new Configuration()
    var props = new Properties()

    val env = System.getenv(ENV_PROPERTIES)
    if (env != null) {
        griddbConf.addResource(new Path(env))
    }

    if(griddbConf.get("gs.host") != null){
        props.setProperty("host", griddbConf.get("gs.host"))
    }
    if(griddbConf.get("gs.port") != null){
        props.setProperty("port", griddbConf.get("gs.port"))
    }
    if(griddbConf.get("gs.notification.address") != null){
        props.setProperty("notificationAddress", griddbConf.get("gs.notification.address"))
    }
    if(griddbConf.get("gs.notification.port") != null){
        props.setProperty("notificationPort", griddbConf.get("gs.notification.port"))
    }
    if(griddbConf.get("gs.notification.member") != null){
        props.setProperty("notificationMember", griddbConf.get("gs.notification.member"))
    }
    if(griddbConf.get("gs.notification.provider") != null){
        props.setProperty("notificationProvider", griddbConf.get("gs.notification.provider"))
    }
    if(griddbConf.get("gs.consistency") != null){
        props.setProperty("consistency", griddbConf.get("gs.consistency"))
    }
    if(griddbConf.get("gs.transaction.timeout") != null){
        props.setProperty("transactionTimeout", griddbConf.get("gs.transaction.timeout"))
    }
    if(griddbConf.get("gs.failover.timeout") != null){
        props.setProperty("failoverTimeout", griddbConf.get("gs.failover.timeout"))
    }
    if(griddbConf.get("gs.container.cache.size") != null){
        props.setProperty("containerCacheSize", griddbConf.get("gs.container.cache.size"))
    }
    if(griddbConf.get("gs.data.affinity.pattern") != null){
        props.setProperty("dataAffinityPattern", griddbConf.get("gs.data.affinity.pattern"))
    }

    props.setProperty("clusterName", griddbConf.get("gs.cluster.name"))
    props.setProperty("user", griddbConf.get("gs.user"))
    props.setProperty("password", griddbConf.get("gs.password"))

    if (sc.getConf.contains("gs.database")) {
        props.setProperty("database", sc.getConf.get("gs.database"))
    }
    GridStoreFactory.getInstance().getGridStore(props)
  }
    
  /**
   * getGsType
   *   dataType : String
   */
  private def getGsType(dataType: String) : GSType = {
    if (dataType == "BinaryType") {
      GSType.BLOB
    }
    else if (dataType == "BooleanType") {
      GSType.BOOL
    }
    else if (dataType.contains("ArrayType(BooleanType")) {
      GSType.BOOL_ARRAY
    }
    else if (dataType == "ByteType") {
      GSType.BYTE
    }
    else if (dataType.contains("ArrayType(ByteType")) {
      GSType.BYTE_ARRAY
    }
    else if (dataType == "DoubleType") {
      GSType.DOUBLE
    }
    else if (dataType.contains("ArrayType(DoubleType")) {
      GSType.DOUBLE_ARRAY
    }
    else if (dataType == "FloatType") {
      GSType.FLOAT
    }
    else if (dataType.contains("ArrayType(FloatType")) {
      GSType.FLOAT_ARRAY
    }
    else if (dataType == "IntegerType") {
      GSType.INTEGER
    }
    else if (dataType.contains("ArrayType(IntegerType")) {
      GSType.INTEGER_ARRAY
    }
    else if (dataType == "LongType") {
      GSType.LONG
    }
    else if (dataType.contains("ArrayType(LongType")) {
      GSType.LONG_ARRAY
    }
    else if (dataType == "ShortType") {
      GSType.SHORT
    }
    else if (dataType.contains("ArrayType(ShortType")) {
      GSType.SHORT_ARRAY
    }
    else if (dataType == "StringType") {
      GSType.STRING
    }
    else if (dataType.contains("ArrayType(StringType")) {
      GSType.STRING_ARRAY
    }
    else if (dataType == "TimestampType") {
      GSType.TIMESTAMP
    }
    else if (dataType.contains("ArrayType(TimestampType")) {
      GSType.TIMESTAMP_ARRAY
    }
    else {
      GSType.STRING
    }
  }
  
  override def compute(split: org.apache.spark.Partition,context: org.apache.spark.TaskContext) : Iterator[(K, V)] = ???
  
  override def getPartitions: Array[org.apache.spark.Partition] = ???
}
