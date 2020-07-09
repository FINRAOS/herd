/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.sql.herd

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import org.finra.herd.sdk.model.{PartitionValueFilter, PartitionValueRange}

private object PartitionFilter {

  implicit def toPartitionValueFilter(in: PartitionFilter): PartitionValueFilter = in match {
    case PartitionValuesFilter(key, values) =>
      val filter = new PartitionValueFilter()
      filter.setPartitionKey(key)
      filter.setPartitionValues(values.toList.asJava)

      filter

    case PartitionRangeFilter(key, (start, end)) =>
      val range = new PartitionValueRange()
      range.setStartPartitionValue(start)
      range.setEndPartitionValue(end)

      val filter = new PartitionValueFilter()
      filter.setPartitionKey(key)
      filter.setPartitionValueRange(range)

      filter
  }

}
private trait PartitionFilter {

  val key: String

}

private case class PartitionValuesFilter(override val key: String, values: Array[String]) extends PartitionFilter

private case class PartitionRangeFilter(override val key: String, range: (String, String)) extends PartitionFilter

private case class HerdOptions(
  namespace: String,
  businessObjectName: String,
  dataProvider: String,
  formatUsage: String,
  fileTypes: Array[String],
  registerNewFormat: Boolean,
  delimiter: String,
  nullValue: String,
  escapeCode: String,
  partitionKey: String,
  partitionKeyGroup: Option[String],
  subPartitionKeys: Array[String],
  partitionFilter: Option[PartitionFilter],
  partitionValue: Option[String],
  subPartitions: Array[String],
  storageName: String,
  storagePathPrefix: String
)

// todo: massive clean up needed here, remove defaults
private object HerdOptions {

  private val logger = LoggerFactory.getLogger(getClass)

  def apply(input: Map[String, String])(sparkSession: SparkSession): HerdOptions = {
    val namespace = input.getOrElse("namespace", {
      logger.error("Namespace is a required field")
      throw new IllegalArgumentException("Namespace is a required field")
    })

    val businessObjectName = input.getOrElse("businessObjectName", {
      logger.error("businessObjectName is a required field")
      throw new IllegalArgumentException("businessObjectName is a required field")
    })

    val dataProvider = input.get("dataProvider")
      .orElse(sparkSession.conf.getOption("spark.herd.default.dataProvider"))
      .getOrElse("FINRA")

    val formatUsage = input.get("businessObjectFormatUsage")
      .orElse(sparkSession.conf.getOption("spark.herd.default.usage"))
      .getOrElse("PRC")

    val formatFileType = input.get("businessObjectFormatFileType")
      .orElse(sparkSession.conf.getOption("spark.herd.default.preferred-file-types"))
      .map(_.split(",").map(_.trim))
      .getOrElse(Array("PARQUET", "BZ", "GZ", "CSV", "TXT", "JSON"))

    val registerNewFormat = input.getOrElse("registerNewFormat", "false").toBoolean

    val delimiter = input.getOrElse("delimiter", ",")

    val nullValue = input.getOrElse("nullValue", "")

    val escape = input.getOrElse("escape", """\\""")

    val partitionKey = input.getOrElse("partitionKey", "partition")

    val partitionKeyGroup = input.get("partitionKeyGroup")

    val partitionFilters = input.get("partitionFilter") map {
      case values if values.contains("--") =>
        val Array(start, end) = values.split("--").map(_.trim)
        PartitionRangeFilter("", (start, end))

      case values => PartitionValuesFilter("", values.split(",").map(_.trim))
    }

    val partitionValue = input.get("partitionValue")

    val subPartitionKeys = input.get("subPartitionKeys") match {
      case Some(value) => value.split("\\|").map(_.trim).filter(_.nonEmpty)
      case None => Array.empty[String]
    }

    val subPartitions = input.get("subPartitionValues") match {
      case Some(value) => value.split("\\|").map(_.trim).filter(_.nonEmpty)
      case None => Array.empty[String]
    }

    if (partitionValue.isEmpty && !subPartitions.isEmpty) {
      logger.error("partitionValue is required when specifying subPartitionValues")
      throw new IllegalArgumentException("partitionValue is required when specifying subPartitionValues")
    }

    if (subPartitionKeys.isEmpty ^ subPartitions.isEmpty) {
      logger.error("subPartitionKeys and subPartitionValues are both required when either one is specified")
      throw new IllegalArgumentException("subPartitionKeys and subPartitionValues are both required when either one is specified")
    }

    if (subPartitionKeys.length != subPartitions.length) {
      logger.error("An equal number of subPartitionKeys and subPartitionValues should be specified. \n" +
        s"subPartitionKeys: $subPartitionKeys, subPartitionValues: $subPartitions")
      throw new IllegalArgumentException("An equal number of subPartitionKeys and subPartitionValues should be specified. \n" +
        s"subPartitionKeys: $subPartitionKeys, subPartitionValues: $subPartitions")
    }

    val storageName = input.get("storage")
      .orElse(sparkSession.conf.getOption("spark.herd.default.storage"))
      .getOrElse("S3_MANAGED")

    val storagePathPrefix = input.get("storagePathPrefix")
      .orElse(sparkSession.conf.getOption("spark.herd.default.storagePathPrefix"))
      .getOrElse("s3a://")

    HerdOptions(
      namespace,
      businessObjectName,
      dataProvider,
      formatUsage,
      formatFileType,
      registerNewFormat,
      delimiter,
      nullValue,
      escape,
      partitionKey,
      partitionKeyGroup,
      subPartitionKeys,
      partitionFilters,
      partitionValue,
      subPartitions,
      storageName,
      storagePathPrefix
    )
  }
}
