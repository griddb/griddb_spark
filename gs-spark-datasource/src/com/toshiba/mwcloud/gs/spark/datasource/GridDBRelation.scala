/*
   Copyright (c) 2017 TOSHIBA CORPORATION.

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
package com.toshiba.mwcloud.gs.spark.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types._

import com.toshiba.mwcloud.gs.GridStore
import com.toshiba.mwcloud.gs.GSType
import com.toshiba.mwcloud.gs.GSType._

import com.toshiba.mwcloud.gs.spark.GridDBUtils

class GridDBRelation(containerName: String, griddb:GridStore)
(@transient val sqlContext: SQLContext)
  extends BaseRelation
  	with TableScan
  	with InsertableRelation
  	with Serializable {
  
  override def schema: StructType = {
    val containerInfo = griddb.getContainerInfo(containerName)
    
    if (containerInfo == null) {
      println("Not Found " + containerName)
      val colmnName = "Error"
      val fields = new Array[StructField](1)
      val metadata = new MetadataBuilder().putString("name", colmnName)
      val columnType = StringType
      fields(0) = StructField(colmnName, columnType, true, metadata.build())
      return StructType(fields)
    }
    
    import sqlContext.implicits._
    
    val fields = new Array[StructField](containerInfo.getColumnCount())

    for (i <- 0 to containerInfo.getColumnCount() - 1) {
      val columnName = containerInfo.getColumnInfo(i).getName()
      val metadata = new MetadataBuilder().putString("name", columnName)
      val columnType = getCatalystType(containerInfo.getColumnInfo(i).getType())
      fields(i) = StructField(columnName, columnType, true, metadata.build())
    }
    
    return StructType(fields)
  }
  
  override def buildScan(): RDD[Row] = {
    val rdd = new GridDBUtils(sqlContext.sparkContext).createRDD(containerName)
    if (rdd == null || rdd == None) {
      return sqlContext.sparkContext.makeRDD(Seq("Not Found " + containerName)).map { str => Row.fromSeq(Seq(str)) }
    }
    rdd.map{ column => {
        var seq = Seq(column.getValue(0))
        if (column.getValue(0).getClass.toString().equals("class java.util.Date")) {
          val times = new java.sql.Timestamp(column.getTimestamp(0).getTime)
          seq = Seq(times)
        }
        
        for (i <- 1 to column.toString().split("\t").length - 1) {
          if (column.getValue(i).getClass.toString().equals("class java.util.Date")) {
            val times = new java.sql.Timestamp(column.getTimestamp(i).getTime)
            seq = seq :+ times
          } else {
            seq = seq :+ column.getValue(i)
          }
        }
        seq
      }
    }.map(seq => Row.fromSeq(seq))
  }
  
  override def insert(df: DataFrame, overwrite: Boolean): Unit = {
    val griddb = new GridDBUtils(df.sparkSession.sparkContext)
    if (overwrite) {
      griddb.save(containerName, df, overwrite)
    } else {
      griddb.save(containerName, df)
    }
  }
  
  private def getCatalystType(gsType: GSType): DataType = {
    val answer = gsType match {
      case BLOB => BinaryType
      case BOOL => BooleanType
      case BOOL_ARRAY => ArrayType(BooleanType)
      case BYTE => ByteType
      case BYTE_ARRAY => ArrayType(ByteType)
      case DOUBLE => DoubleType
      case DOUBLE_ARRAY => ArrayType(DoubleType)
      case FLOAT => FloatType
      case FLOAT_ARRAY => ArrayType(FloatType)
      case GEOMETRY => BinaryType
      case INTEGER => IntegerType
      case INTEGER_ARRAY => ArrayType(IntegerType)
      case LONG => LongType
      case LONG_ARRAY => ArrayType(LongType)
      case SHORT => ShortType
      case SHORT_ARRAY => ArrayType(ShortType)
      case STRING => StringType
      case STRING_ARRAY => ArrayType(StringType)
      case TIMESTAMP => TimestampType
      case TIMESTAMP_ARRAY => ArrayType(TimestampType)
      case _ => null
    }
    answer
  }
  
  
}
