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

import java.util.TimeZone

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.unescapePathName
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionPath, PartitionSpec}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import org.finra.herd.sdk.model.Partition

/** A custom [[org.apache.spark.sql.execution.datasources.FileIndex]] to use the partition paths provided by Herd, vs Spark's auto-discovery
 *
 * The custom data source abstracts the logic of querying Herd and defining DataFrames from the source data, or writing and creating
 * data sets. From an end-user's perspective using the Herd data source would be very similar to standard Spark data sources, using the DataFrameReader/Write
 * interfaces.
 *
 * @param sparkSession        The spark session
 * @param api                 The ApiClient instance needed by Herd SDK
 * @param herdPartitions      The list of partitions
 * @param namespace           The namespace
 * @param businessObjectName  The business object definition name
 * @param formatUsage         The business object format usage (e.g. PRC).
 * @param formatFileType      The business object format file type (e.g. GZ).
 * @param partitionKey        The business object format partition key.
 * @param herdPartitionSchema The schema associated with the business object format
 */
private[sql] abstract class HerdFileIndexBase(
                                             sparkSession: SparkSession,
                                             api: () => HerdApi,
                                             herdPartitions: Seq[(Integer, String, Seq[String], Integer)],
                                             namespace: String,
                                             businessObjectName: String,
                                             formatUsage: String,
                                             formatFileType: String,
                                             partitionKey: String,
                                             herdPartitionSchema: StructType,
                                             storagePathPrefix: String) extends FileIndex with Logging {

  import HerdFileIndexBase._

  protected val hadoopConf = sparkSession.sessionState.newHadoopConf()

  protected val partitionSpec = {
    val partitions = herdPartitions.map {
      case (formatVersion, partitionValue, subPartitionValues, dataVersion) =>
        val row = if (herdPartitionSchema.nonEmpty) {
          val partValues = partitionValue +: subPartitionValues
          val values = partValues.zipWithIndex.map {
            case (rawValue, index) =>
              val field = herdPartitionSchema(index)

              Cast(Literal.create(unescapePathName(rawValue), StringType), field.dataType, Option(TimeZone.getDefault.getID)).eval()
          }

          InternalRow.fromSeq(values)
        } else {
          InternalRow.empty
        }

        val pathSettings = Array(
          s"namespace=$namespace",
          s"businessObjectName=$businessObjectName",
          s"formatUsage=$formatUsage",
          s"formatFileType=$formatFileType",
          s"formatVersion=$formatVersion",
          s"partitionKey=$partitionKey",
          s"partitionValue=$partitionValue",
          s"subPartitionValues=${subPartitionValues.mkString(",")}",
          s"dataVersion=$dataVersion"
        )

        val path = pathSettings.mkString("/")

        PartitionPath(row, path)
    }

    PartitionSpec(herdPartitionSchema, partitions)
  }

  @transient protected val cachedAllFiles = mutable.LinkedHashMap[Path, Array[FileStatus]]()

  override def rootPaths: Seq[Path] = partitionSpec.partitions.map(_.path)

  /**
   * List all files for the specified herd paths
   *
   * @param paths list of paths
   * @return The list of files under herd paths
   */
  protected def bulkListLeafFiles(paths: Seq[Path], formatFileType: String): Seq[(Path, Array[FileStatus])] = {
    val localApiFactory = api
    val fileStatuses = if (paths.size < sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold) {
      listS3KeyPrefixes(localApiFactory(), paths.map(_.toString), storagePathPrefix)
        .map {
          case (path, s3KeyPrefixes) => (path, getAllFilesUnderS3KeyPrefixes(hadoopConf, s3KeyPrefixes, formatFileType).toArray)
        }
        .map {
          case (path, statuses) => (path, statuses.map { s => (s.getPath.toString, s.getLen) })
        }
        .toArray
    } else {
      val serializableConfiguration = new SerializableConfiguration(hadoopConf)
      val parallelPartitionDiscoveryParallelism = sparkSession.sessionState.conf.parallelPartitionDiscoveryParallelism
      val numParallelism = Math.min(paths.size, parallelPartitionDiscoveryParallelism)

      sparkSession.sparkContext
        .parallelize(paths.map(_.toString), numParallelism)
        .mapPartitions { pathStrings => listS3KeyPrefixes(localApiFactory(), pathStrings.toList, storagePathPrefix).iterator }
        .map {
          case (path, s3KeyPrefixes) => (path, getAllFilesUnderS3KeyPrefixes(serializableConfiguration.value, s3KeyPrefixes, formatFileType).toArray)
        }
        .map {
          case (path, statuses) => (path, statuses.map { s => (s.getPath.toString, s.getLen) })
        }
        .collect()
    }

    fileStatuses.map {
      case (path, statuses) =>
        val newPath = new Path(path)
        val newStatuses = statuses.map {
          case (filePath, size) => new FileStatus(size, false, 0, 0, 0, new Path(filePath))
        }.toArray

        (newPath, newStatuses)
    }
  }

  override def inputFiles: Array[String] = Array.empty

  override def refresh(): Unit = {
    cachedAllFiles.clear()
  }

  override def sizeInBytes: Long = Long.MaxValue

  override def partitionSchema: StructType = herdPartitionSchema

  override def toString: String = {
    s"HerdFileIndex[" +
      s"namespace=$namespace," +
      s"businessObjectName=$businessObjectName," +
      s"formatUsage=$formatUsage," +
      s"formatFileType=$formatFileType" +
      "]"
  }

  def filterPartitions(filters: Seq[Expression]): FileIndex
}

private object HerdFileIndexBase extends Logging {

  def parsePartitionPath(path: String): Map[String, Option[String]] = {
    path.split("/").map(_.split("=")).map(i => i.head -> i.drop(1).headOption).toMap
  }

  /**
   * Find all S3 directories(aka s3 key prefixes) specified by the paths
   *
   * @param api   The ApiClient instance needed by Herd SDK
   * @param paths List of herd paths
   * @return list of s3 key prefixes
   */
  def listS3KeyPrefixes(api: HerdApi, paths: Seq[String], storagePathPrefix: String): Seq[(String, Seq[String])] = {
    if (paths.isEmpty) {
      return Seq.empty
    }

    val parts = parsePartitionPath(paths(0))
    val partitionKey = parts("partitionKey").get
    val partitionValues = paths.map(path => parsePartitionPath(path)("partitionValue").get).toList

    Try(api.getBusinessObjectDataPartitions(
      parts("namespace").get,
      parts("businessObjectName").get,
      parts("formatUsage").get,
      parts("formatFileType").get,
      parts("formatVersion").get.toInt,
      partitionKey,
      partitionValues.distinct,
      parts("dataVersion").get.toInt
    )) match {
      case Success(objectDataDdl) => getS3KeyPrefixes(objectDataDdl.getPartitions, paths, storagePathPrefix)
      case Failure(error) =>
        log.error(s"Could not fetch object data DDL request for $partitionValues", error)
        throw new RuntimeException(error)
    }
  }

  /**
   * Retrieve all the S3 directories(aka S3 key prefixes) from the business object data DDL
   *
   * @param generatedPartitions The business object data DDL
   * @param paths           The list of herd paths
   * @return list of s3 key prefixes
   */
  private def getS3KeyPrefixes(generatedPartitions: java.util.List[Partition], paths: Seq[String], storagePathPrefix: String): Seq[(String, Seq[String])] = {
    val partitionKey = parsePartitionPath(paths(0))("partitionKey").get
    val partitionValueTuples = paths.map(path => {
      val parts = parsePartitionPath(path)
      val primaryPartition = parts("partitionValue").get
      val subPartitions = parts("subPartitionValues").getOrElse("")
      if (!subPartitions.isEmpty) {
        (path, primaryPartition + "," + subPartitions, new ArrayBuffer[String]())
      } else {
        (path, primaryPartition, new ArrayBuffer[String]())
      }
    }).toList

    val emptySubPartitionRegex = "subPartitionValues=/"
    val partitionValueTuplesMap = mutable.Map[String, ArrayBuffer[String]]()
    var containUnregisteredSubpartition = false
    var s3KeyPrefixes = new ArrayBuffer[String]()
    if (partitionKey.equalsIgnoreCase("partition")) {
      // Handling non-partitioned
      for (partition <- generatedPartitions.asScala) {
        if (StringUtils.isNotEmpty(storagePathPrefix) && !storagePathPrefix.toLowerCase().startsWith("s3a")) {
          s3KeyPrefixes += "/" + storagePathPrefix + "/" + partition.getPartitionLocation
        }
        else {
          s3KeyPrefixes += "s3a://" + partition.getPartitionLocation
        }
      }

      return List((paths(0), s3KeyPrefixes))
    } else {
      // Parse the DDL, and grab the partition values and their S3 key prefixes
      for (partition <- generatedPartitions.asScala) {

        var partitionList = new ArrayBuffer[String]()
        for (partitionColumn <- partition.getPartitionColumns.asScala) {
          partitionList += partitionColumn.getPartitionColumnValue
        }
        val ddlPartitionValue = partitionList.mkString(",")

        // Only add the S3 Prefixes when the partition values match
        var done = false
        var index = 0
        while (index < partitionValueTuples.length && !done) {
          var partitionValueTuple = partitionValueTuples(index)
          if (ddlPartitionValue.startsWith(partitionValueTuple._2)) {
            // use mount point if storage path prefix is not empty and it does not start with s3a
            var filePath = ""
            if (StringUtils.isNotEmpty(storagePathPrefix) && !storagePathPrefix.toLowerCase().startsWith("s3a")) {
              filePath = "/" + storagePathPrefix + "/" + partition.getPartitionLocation
            }
            else {
              filePath = "s3a://" + partition.getPartitionLocation
            }

            partitionValueTuple._3 += filePath
            // ddlPartitionValue is longer than partition value, which means there is unregistered subpartition from ddl
            if (ddlPartitionValue.length > partitionValueTuple._2.length) {
              containUnregisteredSubpartition = true
              var path = partitionValueTuple._1
              val subPartitionValue = ddlPartitionValue.substring(ddlPartitionValue.indexOf(",") + 1)
              path = StringUtils.replaceIgnoreCase(path, emptySubPartitionRegex, "subPartitionValues=" + subPartitionValue + "/")

              val s3filePathList = partitionValueTuplesMap.getOrElse(path, null) //
              if (s3filePathList != null) {
                partitionValueTuplesMap.updated(path, s3filePathList + filePath)
              }
              else {
                partitionValueTuplesMap.put(path, ArrayBuffer(filePath))
              }
            }
            done = true
          }
          index += 1
        }
      }

      if (containUnregisteredSubpartition) {
        partitionValueTuplesMap.toList
      }
      else {
        partitionValueTuples.map(p => {
          (p._1, p._3)
        })
      }
    }
  }

  /**
   * List all files under the s3 directories(aka S3 key prefixes)
   *
   * @param hadoopConf    hadoop configuration
   * @param s3KeyPrefixes all s3 key prefixes
   * @return list of files
   */
  private def getAllFilesUnderS3KeyPrefixes(hadoopConf: Configuration, s3KeyPrefixes: Seq[String], formatFileType: String): Seq[FileStatus] = {
    s3KeyPrefixes.flatMap {
      s3KeyPrefix => {
        val s3Path = new Path(s3KeyPrefix)
        val fs = s3Path.getFileSystem(hadoopConf)
        var iterator = fs.listFiles(s3Path, true)
        var fileStatusList = new ArrayBuffer[FileStatus]()

        // Find all files under each directory
        while (iterator.hasNext) {
          val file = iterator.next()
          // ignore _committed_ file
          if (!file.getPath.getName.matches("^_committed_.*$")) {
            fileStatusList += file
          }
        }
        fileStatusList.toList
      }
    }
  }

}
