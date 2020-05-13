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

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}

class HerdOptionsSuite extends FunSuite with ShouldMatchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
     this.spark = SparkSession
      .builder()
      .appName("catalog-test")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    this.spark.stop()
  }

  test("validate required params/param-combinations") {
    // attempt to initialize with no namespace
    an [IllegalArgumentException] should be thrownBy HerdOptions(getDefaultHerdOptions - "namespace")(spark)

    // attempt to initialize with no bDef
    an [IllegalArgumentException] should be thrownBy HerdOptions(getDefaultHerdOptions - "businessObjectName")(spark)

    // attempt to initialize with subPartitions but no primary partition
    an [IllegalArgumentException] should be thrownBy HerdOptions(getDefaultHerdOptions - "partitionValue")(spark)

    // attempt to initialize with subPartitionKeys but no subPartitionValues
    an [IllegalArgumentException] should be thrownBy HerdOptions(getDefaultHerdOptions - "subPartitionValues")(spark)

    // attempt to initialize with subPartitionValues but no subPartitionKeys
    an [IllegalArgumentException] should be thrownBy HerdOptions(getDefaultHerdOptions - "subPartitionKeys")(spark)
  }

  test("validate default params") {

    // initialize with no dataProvider and verify default value
    val herdOptionsNoDataProvider: HerdOptions = HerdOptions(getDefaultHerdOptions - "dataProvider")(spark)
    assert(!herdOptionsNoDataProvider.dataProvider.isEmpty, "dataProvider should have a default value")
    herdOptionsNoDataProvider.dataProvider shouldBe "FINRA"

    // initialize with no formatUsage and verify default value
    val herdOptionsNoUsage: HerdOptions = HerdOptions(getDefaultHerdOptions - "businessObjectFormatUsage")(spark)
    assert(!herdOptionsNoUsage.formatUsage.isEmpty, "businessObjectFormatUsage should have a default value")
    herdOptionsNoUsage.formatUsage shouldBe "PRC"

    // initialize with no delimiter and verify default value
    val herdOptionsNoDelimiter: HerdOptions = HerdOptions(getDefaultHerdOptions - "delimiter")(spark)
    assert(!herdOptionsNoDelimiter.delimiter.isEmpty, "delimiter should have a default value")
    herdOptionsNoDelimiter.delimiter shouldBe ","

    // initialize with no nullValue and verify default value
    val herdOptionsNoNullValue: HerdOptions = HerdOptions(getDefaultHerdOptions - "nullValue")(spark)
    assert(herdOptionsNoNullValue.nullValue != null, "nullValue should not be null")
    herdOptionsNoNullValue.nullValue shouldBe ""

    val herdOptionsNoStorage: HerdOptions = HerdOptions(getDefaultHerdOptions - "storage")(spark)
    assert(!herdOptionsNoStorage.storageName.isEmpty, "storage should have a default value")
    herdOptionsNoStorage.storageName shouldBe "S3_MANAGED"

    val herdOptionsNoStoragePrefix: HerdOptions = HerdOptions(getDefaultHerdOptions - "storagePathPrefix")(spark)
    assert(!herdOptionsNoStoragePrefix.storagePathPrefix.isEmpty, "storagePathPrefix should have a default value")
    herdOptionsNoStoragePrefix.storagePathPrefix shouldBe "s3a://"
  }

  test("validate that subPartitionKeys/Values are valid that are read correctly") {

    val herdOptions: HerdOptions = HerdOptions(getDefaultHerdOptions)(spark)
    herdOptions.subPartitionKeys shouldBe Array[String]("BUSINESS_LOCATION", "BUSINESS_DIVISION")

    val herdOptionsSubPartKeysTrimAndEmptyCheck = HerdOptions(getDefaultHerdOptions + ("subPartitionKeys" -> "  a| b   |"))(spark)
    herdOptionsSubPartKeysTrimAndEmptyCheck.subPartitionKeys shouldBe Array[String]("a", "b")

    val herdOptionsSubPartValuesTrimAndEmptyCheck = HerdOptions(getDefaultHerdOptions + ("subPartitionValues" -> " x| y  |"))(spark)
    herdOptionsSubPartValuesTrimAndEmptyCheck.subPartitions shouldBe Array[String]("x", "y")

    an [IllegalArgumentException] should be thrownBy HerdOptions(getDefaultHerdOptions + ("subPartitionKeys" -> "a") + ("subPartitionValues" -> "x|y"))(spark)
  }

  def getDefaultHerdOptions: Map[String, String] = {
    Map(
      "namespace" -> "someNamespace",
      "businessObjectName" -> "someBusinessObjectName",
      "dataProvider" -> "someDataProvider",
      "businessObjectFormatUsage" -> "someUsage",
      "businessObjectFormatFileType" -> "someFileType",
      "registerNewFormat" -> "false",
      "delimiter" -> ",",
      "nullValue" -> "\\N",
      "escape" -> "\\",
      "partitionKey" -> "date",
      "partitionValue" -> "2020-01-01",
      "partitionKeyGroup" -> "BUSINESS_CALENDAR",
      "partitionFilter" -> "",
      "subPartitionKeys" -> "BUSINESS_LOCATION|BUSINESS_DIVISION",
      "subPartitionValues" -> "USA|HR",
      "storage" -> "S3",
      "storagePathPrefix" -> "s3a://"
    )
  }

}
