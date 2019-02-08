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
package org.apache.spark.sql.hive.orc

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/** Wrapper for [[org.apache.spark.sql.hive.orc.OrcFileFormat]] that properly maps Herd schema to ORC field names */
private[sql] class HerdOrcFileFormat extends OrcFileFormat with Logging {

  /* We have to disable batch scan since bad files from HIVE won't work
  due to this issue https://issues.apache.org/jira/browse/HIVE-7189
   */
  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = false

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    buildReader(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    // we cannot use vectorized reader in some cases due to
    // this HIVE issue https://issues.apache.org/jira/browse/HIVE-7189
    val nonVectorizedSession = sparkSession.newSession()
    nonVectorizedSession.conf.set("spark.databricks.io.orc.fastreader.enabled", false)

    val hiveSchema = StructType(dataSchema.zipWithIndex.map {
      case (field, index) => field.copy(name = s"_col$index")
    })

    val readSchema = StructType(requiredSchema
      .map(f => hiveSchema(dataSchema.fieldIndex(f.name))))

    val emptyPartitionSchema = new StructType()

    val hiveReader = super.buildReader(nonVectorizedSession, hiveSchema, emptyPartitionSchema, readSchema, filters, options, hadoopConf)

    // in some cases the ORC field names do not have the same case as Herd schema
    val lowerCaseSchema = StructType(dataSchema.map(f => f.copy(name = f.name.toLowerCase)))
    val lowerCaseRequiredSchema = StructType(requiredSchema
      .map(f => lowerCaseSchema(dataSchema.fieldIndex(f.name))))

    val herdLowerCaseReader = super.buildReader(nonVectorizedSession, lowerCaseSchema, emptyPartitionSchema,
      lowerCaseRequiredSchema, filters, options, hadoopConf)

    val herdReader = super.buildReader(nonVectorizedSession, dataSchema, emptyPartitionSchema, requiredSchema, filters, options, hadoopConf)

    val broadcastedHadoopConf =
      nonVectorizedSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    new (PartitionedFile => Iterator[InternalRow]) with Serializable {
      override def apply(file: PartitionedFile) = {
        val conf = broadcastedHadoopConf.value.value

        val physicalSchema = OrcFileOperator.readSchema(Seq(file.filePath), Some(conf))

        val partitionValueLiterals = partitionSchema.map(_.dataType).zipWithIndex.map {
          case (dt, i) => Literal(file.partitionValues.get(i, dt), dt)
        }

        if (physicalSchema.isEmpty) {
          Iterator.empty
        } else if (physicalSchema.get.fieldNames.deep == hiveSchema.fieldNames.deep) {
          log.info("Using reader with Hive schema")

          val inputSchema = readSchema.toAttributes
          val projectionSchema = requiredSchema.toIndexedSeq
          val projection = inputSchema.zipWithIndex.map {
            case (field, index) => field.withName(projectionSchema(index).name)
          } ++ partitionValueLiterals

          log.info(s"Mapping [${inputSchema.mkString(",")}] to [${projection.mkString(",")}]")

          val project = GenerateUnsafeProjection.generate(projection, inputSchema)
          hiveReader(file).map(project)
        } else if (physicalSchema.get.fieldNames.deep == dataSchema.fieldNames.map(_.toLowerCase).deep) {
          log.info("Using lower case native reader")

          val inputSchema = lowerCaseRequiredSchema.toAttributes
          val projectionSchema = requiredSchema.toIndexedSeq
          val projection = inputSchema.zipWithIndex.map {
            case (field, index) => field.withName(projectionSchema(index).name)
          } ++ partitionValueLiterals

          log.info(s"Mapping [${inputSchema.mkString(",")}] to [${projection.mkString(",")}]")

          val project = GenerateUnsafeProjection.generate(projection, inputSchema)

          herdLowerCaseReader(file).map(project)
        } else {
          log.info("Using native reader")

          val inputSchema = requiredSchema.toAttributes
          val projectionSchema = requiredSchema.toIndexedSeq
          val projection = inputSchema.zipWithIndex.map {
            case (field, index) => field.withName(projectionSchema(index).name)
          } ++ partitionValueLiterals

          log.info(s"Mapping [${inputSchema.mkString(",")}] to [${projection.mkString(",")}]")

          val project = GenerateUnsafeProjection.generate(projection, inputSchema)

          herdReader(file).map(project)
        }
      }
    }
  }
}
