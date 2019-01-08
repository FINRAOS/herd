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

private object HerdOptions {
  def apply(input: Map[String, String])(sparkSession: SparkSession): HerdOptions = {
    val namespace = input.get("namespace")
      .getOrElse(sys.error("Must specify `namespace` option"))

    val businessObjectName = input.get("businessObjectName")
      .getOrElse(sys.error("Must specify `businessObjectName`"))

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

    val subPartitionKeys = input.get("subPartitionKeys")
      .map(_.split(",").map(_.trim)).getOrElse(Array.empty)

    val partitionFilters = input.get("partitionFilter") map {
      case values if values.contains("-") =>
        val Array(start, end) = values.split("-").map(_.trim)
        PartitionRangeFilter("", (start, end))

      case values => PartitionValuesFilter("", values.split(",").map(_.trim))
    }

    val partitionValue = input.get("partitionValue")

    val subPartitions = input.get("subPartitionValues") match {
      case Some(value) => value.split(",").map(_.trim)
      case None => Array.empty[String]
    }

    if (partitionValue.isEmpty && !subPartitions.isEmpty) {
      sys.error("Cannot specify `subPartitions` without `partitionValue`")
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
