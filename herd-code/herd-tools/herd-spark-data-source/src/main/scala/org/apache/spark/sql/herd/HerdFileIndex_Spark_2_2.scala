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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.unescapePathName
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Cast, Expression, InterpretedPredicate, Literal}
import org.apache.spark.sql.execution.datasources.{PartitionDirectoryShim, _}
import org.apache.spark.sql.types.{StringType, StructType}
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

private[sql] class HerdFileIndex(
                                  sparkSession: SparkSession,
                                  api: () => HerdApi,
                                  herdPartitions: Seq[(Integer, String, Seq[String], Integer)],
                                  namespace: String,
                                  businessObjectName: String,
                                  formatUsage: String,
                                  formatFileType: String,
                                  partitionKey: String,
                                  herdPartitionSchema: StructType,
                                  storagePathPrefix: String)
  extends HerdFileIndexBase(
    sparkSession,
    api,
    herdPartitions,
    namespace,
    businessObjectName,
    formatUsage,
    formatFileType,
    partitionKey,
    herdPartitionSchema,
    storagePathPrefix) {

  override def listFiles(
                          filters: Seq[Expression],
                          dataFilters: scala.Seq[org.apache.spark.sql.catalyst.expressions.Expression]
                        ): Seq[PartitionDirectory] =
  {

    val prunedPartitions = if (partitionSpec.partitionColumns.isEmpty) {
      partitionSpec.partitions
    }
    else {
      prunePartitions(filters, partitionSpec)
    }

    // val pathsToFetch = prunedPartitions.filter(p => cachedAllFiles.get(p.path).isEmpty).map(_.path)
    val partitionDirectories: ArrayBuffer[PartitionDirectory] = new ArrayBuffer[PartitionDirectory]()

    for (partitionPath <- prunedPartitions) {
      cachedAllFiles ++= bulkListLeafFiles(Seq(partitionPath.path), formatFileType)
      val partitionPattern = new Regex("(?i)/partitionValue=(.*?)/")
      val subPartitionPattern = new Regex("(?i)/subPartitionValues=(.*?)/")
      // there is unregistered subpartition here
      if (cachedAllFiles.size != 0 && cachedAllFiles.get(partitionPath.path) == None) {
        for (elem <- cachedAllFiles) {
          val pathString = elem._1.toString
          val partitionValue = partitionPattern.findFirstMatchIn(pathString) match {
            case Some(i) => i.group(1)
            case None => ""
          }

          val subPartitionValuesString = subPartitionPattern.findFirstMatchIn(pathString) match {
            case Some(i) => i.group(1)
            case None => ""
          }

          val subPartitionValues = subPartitionValuesString.split(",")
          val row = if (herdPartitionSchema.nonEmpty) {
            val partValues = partitionValue +: subPartitionValues
            val values = partValues.zipWithIndex.map { case (rawValue, index) => val field = herdPartitionSchema(index)
              Cast(Literal.create(unescapePathName(rawValue), StringType), field.dataType).eval()
            }
            InternalRow.fromSeq(values)
          }
          else {
            InternalRow.empty
          }
          partitionDirectories += PartitionDirectoryShim(row, elem._2)
        }
      }
      else {
        partitionDirectories += PartitionDirectoryShim(partitionPath.values, cachedAllFiles(partitionPath.path))
      }
    }

    partitionDirectories
  }

  protected def prunePartitions(
                                 predicates: Seq[Expression],
                                 partitionSpec: PartitionSpec): Seq[PartitionPath] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = InterpretedPredicate(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      val selected = partitions.filter {
        case PartitionPath(values, _) => boundPredicate.eval(values)
      }

      logInfo {
        val total = partitions.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Selected $selectedSize partitions out of $total, pruned $percentPruned% partitions."
      }

      selected
    } else {
      partitions
    }
  }

  override def filterPartitions(filters: Seq[Expression]): FileIndex = {
    val files = listFiles(filters, Seq.empty).toArray
    new PrunedHerdFileIndex(files, partitionSchema)
  }

}

class PrunedHerdFileIndex(files: Array[PartitionDirectory], override val partitionSchema: StructType) extends FileIndex {

  override def sizeInBytes: Long = files
    .flatMap(_.files.asInstanceOf[Seq[Any]])
    .map(FileStatusShim.getLen).sum

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    files
  }

  override def refresh(): Unit = Unit

  override def rootPaths: Seq[Path] = files
    .map(_.files.asInstanceOf[Seq[Any]]
      .map(FileStatusShim.getPath).head.getParent)

  override def inputFiles: Array[String] = files
    .flatMap(_.files.asInstanceOf[Seq[Any]])
    .map(FileStatusShim.getPath(_).toString)
}
