/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.sql.execution.datasources

import java.io.{PrintWriter, File}
import java.util.Properties

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

import scala.collection.mutable.ArrayBuffer


class DefaultSource extends RelationProvider with DataSourceRegister with Logging{

  override def shortName(): String = "jdbc"

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {

    val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))
    val driver = parameters.getOrElse("driver", null)
    val table = parameters.getOrElse("dbtable", sys.error("Option 'dbtable' not specified"))
    val partitionColumn = parameters.getOrElse("partitionColumn", null)
    val lowerBound = parameters.getOrElse("lowerBound", null)
    val upperBound = parameters.getOrElse("upperBound", null)
    val numPartitions = parameters.getOrElse("numPartitions", null)
    val wherePartitions = parameters.getOrElse("wherePartitions", null)

    val file = new File("/tmp/sql.log")
    val w = new PrintWriter(file)
    w.write("  url: " + url + "\n")
    w.write("  driver: " + driver + "\n")
    w.write("  table: " + table + "\n")
    w.write("  partitionCOlumn: " + partitionColumn + "\n")
    w.write("  lowerBound: " + lowerBound + "\n")
    w.write("  upperBound: " + upperBound + "\n")
    w.write("  numPartitions: " + numPartitions + "\n")
    w.write("  wherePartitions: " + wherePartitions + "\n")

    if (driver != null) DriverRegistry.register(driver)

    if (partitionColumn != null
      && (lowerBound == null || upperBound == null || numPartitions == null)) {
      sys.error("Partitioning incompletely specified")
    }

    val partitionInfo = if (partitionColumn == null) {
      null
    } else {
      JDBCPartitioningInfoS(
        partitionColumn,
        lowerBound,
        upperBound,
        numPartitions.toInt)
    }

    // TODO try to get partitions manually if partiitonColumn is null
    val parts = if (partitionInfo == null && wherePartitions != null) {
      w.write(s"  wherePartitions is $wherePartitions" + "\n")
      var ans = new ArrayBuffer[org.apache.spark.Partition]()
      wherePartitions.split(",").zipWithIndex.map {
        case (wp, index) =>
          ans += JDBCPartition(wp.trim, index)
      }
      ans.toArray
    } else if(partitionInfo!=null) {
      log.warn("  partitionInfo is not null, use columnPartition")
      log.warn("  partitioninfo is l:" + partitionInfo.lowerBound + " u:" +partitionInfo.upperBound + " n:" + partitionInfo.numPartitions + " c:" + partitionInfo.column)
      JDBCRelation.columnPartitionWithType(partitionInfo)
    }else{
      log.warn("  no way to get partitions partitionInfo and wherePartitions is all null.")
      null
    }
    val properties = new Properties() // Additional properties that we will pass to getConnection
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    w.write("  parts size is " + parts.length+"\n")
    parts.foreach(p => {
      p.isInstanceOf[JDBCPartition] match {
        case true => w.write("  partition(" + p.asInstanceOf[JDBCPartition].whereClause + ")"+"\n")
        case false => w.write(" partition is not JDBCPartition instance. index is " + p.index+"\n")
        case _ => w.write("  partition not Partition or JDBCPartition instance"+"\n")
      }
    })
    w.write("  parts length is " + parts.length+"\n")
    w.flush()
    w.close()
    JDBCRelation(url, table, parts, properties)(sqlContext)
  }
}
