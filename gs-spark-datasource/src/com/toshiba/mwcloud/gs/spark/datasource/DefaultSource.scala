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
package com.toshiba.mwcloud.gs.spark.datasource

import java.util.Properties
import java.io.FileInputStream
import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame

import com.toshiba.mwcloud.gs.GridStore
import com.toshiba.mwcloud.gs.GridStoreFactory

import com.toshiba.mwcloud.gs.spark.GridDBUtils

class DefaultSource extends RelationProvider
	with SchemaRelationProvider
	with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }
  
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val griddb = (new GridDBUtils(sqlContext.sparkContext)).connectGridDB()
    return new GridDBRelation(parameters.get("path").get, griddb)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode:SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val griddb = new GridDBUtils(sqlContext.sparkContext)
    
    if (parameters != null && parameters != None &&
        parameters.get("rowkey") != null && parameters.get("rowkey") != None &&
        parameters.get("rowkey").get.equalsIgnoreCase("true")) {
      griddb.save(parameters.get("path").get, data, true)
    } else {
      griddb.save(parameters.get("path").get, data)
    }
    
    createRelation(sqlContext, parameters, null)
  }
  
}
