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
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.execution.datasources.{PartitionDirectoryShim, _}
import org.apache.spark.sql.types.StructType

private[sql] class HerdFileIndex(
                                  sparkSession: SparkSession,
                                  api: () => HerdApi,
                                  herdPartitions: Seq[(Integer, String, Seq[String], Integer, String)],
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
                        ): Seq[PartitionDirectory] = {

    val prunedPartitions = if (partitionSpec.partitionColumns.isEmpty) {
      partitionSpec.partitions
    } else {
      prunePartitions(filters, partitionSpec)
    }

    val selectedPartitions = {

      cachedAllFiles ++= bulkListLeafFiles(prunedPartitions.map(partitionPath => Seq(partitionPath.path)).flatten, formatFileType)

      prunedPartitions.map {
        case PartitionPath(values, path) =>
          PartitionDirectoryShim(values, cachedAllFiles(path))
      }

    }

    selectedPartitions
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
