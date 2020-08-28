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
package org.finra.catalog

/**
 *  'GoF' Builder with phantom-types: verifies that static properties are declared using type evidences.
 *  This provides the added advantage of compile-time static property checks and has no effect on the runtime.
 */

sealed trait AvailableHerdOptions

case class BaseHerdOptions(namespace: String, objectName: String, usage: String, fileType: String, partitionKey: String,
                           partitionValue: String, partitionKeyGroup: String, subPartitionKeys: String, subPartitionValues: String,
                           delimiter: String, escapeChar: String, nullValue: String) {

  override def toString: String = s"BaseHerdOptions(namespace=$namespace, objectName=$objectName, usage=$usage, fileType=$fileType, " +
    s"partitionKey=$partitionKey, partitionValue=$partitionValue, partitionKeyGroup=$partitionKeyGroup, subPartitionKeys=$subPartitionKeys, " +
    s"subPartitionValues=$subPartitionValues, delimiter=$delimiter, escapeChar=$escapeChar, nullValue=$nullValue)"
}

object AvailableHerdOptions {
  sealed trait Namespace extends AvailableHerdOptions
  sealed trait ObjectName extends AvailableHerdOptions
  sealed trait Usage extends AvailableHerdOptions
  sealed trait FileType extends AvailableHerdOptions
  sealed trait PartitionKey extends AvailableHerdOptions
  sealed trait PartitionValue extends AvailableHerdOptions
  sealed trait PartitionKeyGroup extends AvailableHerdOptions

  type MandatoryInfo = Namespace with ObjectName with Usage with FileType with PartitionKey with PartitionValue with PartitionKeyGroup
}

case class HerdOptionsBuilder[I <: AvailableHerdOptions](namespace: String = null, objectName: String = null,
                                                         usage: String = null, fileType: String = null,
                                                         partitionKey: String = null, partitionValue: String = null,
                                                         partitionKeyGroup: String = null, subPartitionKeys: String = null,
                                                         subPartitionValues: String = null, delimiter: String = null,
                                                         escapeChar: String = null, nullValue: String = null) {

  // Required parameters
  def withNamespace(namespace: String): HerdOptionsBuilder[I with AvailableHerdOptions.Namespace] =
    this.copy(namespace = namespace)

  def withObjectName(objectName: String): HerdOptionsBuilder[I with AvailableHerdOptions.ObjectName] =
    this.copy(objectName = objectName)

  def withUsage(usage: String): HerdOptionsBuilder[I with AvailableHerdOptions.Usage] =
    this.copy(usage = usage)

  def withFileType(fileType: String): HerdOptionsBuilder[I with AvailableHerdOptions.FileType] =
    this.copy(fileType = fileType)

  def withPartitionKey(partitionKey: String): HerdOptionsBuilder[I with AvailableHerdOptions.PartitionKey] =
    this.copy(partitionKey = partitionKey)

  def withPartitionValue(partitionValue: String): HerdOptionsBuilder[I with AvailableHerdOptions.PartitionValue] =
    this.copy(partitionValue = partitionValue)

  def withPartitionKeyGroup(partitionKeyGroup: String): HerdOptionsBuilder[I with AvailableHerdOptions.PartitionKeyGroup] =
    this.copy(partitionKeyGroup = partitionKeyGroup)

  // Optional parameters
  def withSubPartitionKeys(subPartitionKeys: String): HerdOptionsBuilder[I] =
    this.copy(subPartitionKeys = subPartitionKeys)

  def withSubPartitionValues(subPartitionValues: String): HerdOptionsBuilder[I] =
    this.copy(subPartitionValues = subPartitionValues)

  def withDelimiter(delimiter: String): HerdOptionsBuilder[I] =
    this.copy(delimiter = delimiter)

  def withEscapeChar(escapeChar: String): HerdOptionsBuilder[I] =
    this.copy(escapeChar = escapeChar)

  def withNullValue(nullValue: String): HerdOptionsBuilder[I] =
    this.copy(nullValue = nullValue)

  def build(implicit ev: I =:= AvailableHerdOptions.MandatoryInfo): BaseHerdOptions =
    BaseHerdOptions(namespace, objectName, usage, fileType, partitionKey, partitionValue, partitionKeyGroup,
      subPartitionKeys, subPartitionValues, delimiter, escapeChar, nullValue)
}
