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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.commons.lang.ArrayUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation.PColumnType.PColumnType
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{Logging, Partition}

import scala.collection.mutable.ArrayBuffer

/**
 * Instructions on how to partition the table among workers.
 */
private[sql] case class JDBCPartitioningInfo(
                                              column: String,
                                              lowerBound: Long,
                                              upperBound: Long,
                                              numPartitions: Int)

private[sql] case class JDBCPartitioningInfoS(
                                               column: String,
                                               lowerBound: String,
                                               // to deal with timestamp range, e.g. '20150101100000'
                                               upperBound: String,
                                               numPartitions: Int)

private[sql] object JDBCRelation extends Logging {

  object PColumnType extends Enumeration {
    type PColumnType = Value
    val TIMESTAMP, BIGINT, NONE = Value

    def parse(name: String): PColumnType = name match {
      case "TIMESTAMP" | "timestamp" => TIMESTAMP
      case "INTEGER" | "integer" => BIGINT
      case "BIGINT" | "bigint" => BIGINT
      case _ => NONE
    }
  }

  def columnPartitionWithType(partitioning: JDBCPartitioningInfoS): Array[Partition] = {
    if (partitioning == null || partitioning.column.isEmpty) return Array[Partition](JDBCPartition(null, 0))
    val tmpA: Array[String] = partitioning.column.trim.replaceAll("\\s", " ").split(" ")
    log.warn("  column is _" + partitioning.column + "_")
    log.warn("  column is _" + ArrayUtils.toString(tmpA) + "_")

    val (column: String,
    colType: PColumnType,
    simpleFormat: SimpleDateFormat,
    step: Long) =
      partitioning.column.trim.replaceAll("\\s", " ").split(" ") match {
        case Array(colName, colTypeStr, sStep) =>
          log.warn("  step is " + sStep)
          Tuple4(colName,
            if (PColumnType.parse(colTypeStr.toUpperCase) == PColumnType.BIGINT)
              PColumnType.BIGINT
            else PColumnType.NONE,
            new SimpleDateFormat,
            if (PColumnType.parse(colTypeStr.toUpperCase) == PColumnType.BIGINT)
              sStep.toLong
            else 0
          )
        case Array(colName, colTypeStr, sFormat, sStep) =>
          log.warn("  format is " + sFormat)
          Tuple4(colName,
          if (PColumnType.parse(colTypeStr.toUpperCase) == PColumnType.TIMESTAMP)
            PColumnType.TIMESTAMP
          else PColumnType.NONE, {
            log.warn("  columnPartitionWithType sFormat " + sFormat)
            val realFormatStr = sFormat match {
              case "yyyyMMddHHmmss" => "yyyyMMddHHmmss"
              case _ => "yyyyMMddHHmmss"
            }
            log.warn("  columnPartitionWithType real format " + realFormatStr)
            new SimpleDateFormat(realFormatStr)
          },
          if (PColumnType.parse(colTypeStr.toUpperCase) == PColumnType.TIMESTAMP)
            sStep.toLong * 1000 // after this all is in mill seconds
          else 0
          )
        case _ =>
          Tuple4("", PColumnType.NONE, new SimpleDateFormat, 0)
      }

    if (step == 0 || colType == PColumnType.NONE)
      return Array[Partition](JDBCPartition(null, 0))

    val (lLowerBound: Long, lUpperBound: Long) = colType match {
      case PColumnType.TIMESTAMP =>
        (simpleFormat.parse(partitioning.lowerBound).getTime,
          simpleFormat.parse(partitioning.upperBound).getTime)
      case PColumnType.BIGINT =>
        (partitioning.lowerBound.toLong,
          partitioning.upperBound.toLong)
    }
    log.warn("  columnPartitionWithType lower " + lLowerBound)
    log.warn("  columnPartitionWithType upper " + lUpperBound)

    if (lUpperBound <= lLowerBound)
      return Array[Partition](JDBCPartition(null, 0))

    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    var ans = new ArrayBuffer[Partition]()
    var resigned = false
    var i: Int = 0
    var currentValue: Long = lLowerBound
    while (currentValue < lUpperBound) {
      val lowerBound = colType match {
        case PColumnType.BIGINT =>
          s"$column >= $currentValue"
        case PColumnType.TIMESTAMP =>
          column + " >= " + simpleFormat.format(new Date(currentValue))
      }
      log.warn(s"  lowerBound is $lowerBound")
      currentValue = if (resigned)
        Math.min(currentValue + step, lUpperBound)
      else {
        resigned = true
        Math.min(((currentValue + step) / step).toInt * step, lUpperBound)
      }
      log.warn("  columnPartitionWithType currentValue " + currentValue)
      //TODO why i-1 and 0 is different?
      val upperBound =
        colType match {
          case PColumnType.BIGINT => s"$column < $currentValue"
          case PColumnType.TIMESTAMP =>
            column + " < " + simpleFormat.format(new Date(currentValue))
        }
      log.warn(s"  upperBound is $upperBound")
      val whereClause =
        if (upperBound == null)
          lowerBound
        else if (lowerBound == null)
          upperBound
        else
          s"$lowerBound AND $upperBound"
      log.warn(s"  whereClause is $whereClause")
      ans += JDBCPartition(whereClause, i)
      i = i + 1
    }
    log.warn(" partition num is " + ans.size)
    if (ans.size > 10000)
      throw new SQLException("too many partitions bigger than 1000")

    ans.toArray
  }

  /**
   * Given a partitioning schematic (a column of integral type, a number of
   * partitions, and upper and lower bounds on the column's value), generate
   * WHERE clauses for each partition so that each row in the table appears
   * exactly once.  The parameters minValue and maxValue are advisory in that
   * incorrect values may cause the partitioning to be poor, but no data
   * will fail to be represented.
   */
  def columnPartition(partitioning: JDBCPartitioningInfo): Array[Partition] = {
    if (partitioning == null) return Array[Partition](JDBCPartition(null, 0))

    val numPartitions = partitioning.numPartitions
    val column = partitioning.column
    if (numPartitions == 1) return Array[Partition](JDBCPartition(null, 0))
    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    val stride: Long = (partitioning.upperBound / numPartitions
      - partitioning.lowerBound / numPartitions)
    var i: Int = 0
    var currentValue: Long = partitioning.lowerBound
    var ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lowerBound = if (i != 0) s"$column >= $currentValue" else null
      currentValue += stride
      val upperBound = if (i != numPartitions - 1) s"$column < $currentValue" else null
      val whereClause =
        if (upperBound == null) {
          lowerBound
        } else if (lowerBound == null) {
          upperBound
        } else {
          s"$lowerBound AND $upperBound"
        }
      ans += JDBCPartition(whereClause, i)
      i = i + 1
    }
    ans.toArray
  }
}

private[sql] case class JDBCRelation(
                                      url: String,
                                      table: String,
                                      parts: Array[Partition],
                                      properties: Properties = new Properties())(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with Logging
  with PrunedFilteredScan
  with InsertableRelation {

  override val needConversion: Boolean = false

  override val schema: StructType = JDBCRDD.resolveTable(url, table, properties)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val driver: String = DriverRegistry.getDriverClassName(url)
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    log.warn("  UE JDBCRelation.buildScan filters size is " + filters.length)
    log.warn("  UE JDBCRelation.buildScan parts size is " + parts.length)
    parts.foreach(p => {
      p.isInstanceOf[JDBCPartition] match {
        case true => log.warn(" partition whereClause is " + p.asInstanceOf[JDBCPartition].whereClause)
        case false => log.warn("  partition is Partition instance")
      }
    })
    JDBCRDD.scanTable(
      sqlContext.sparkContext,
      schema,
      driver,
      url,
      properties,
      table,
      requiredColumns,
      filters,
      parts).asInstanceOf[RDD[Row]]
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .jdbc(url, table, properties)
  }
}
