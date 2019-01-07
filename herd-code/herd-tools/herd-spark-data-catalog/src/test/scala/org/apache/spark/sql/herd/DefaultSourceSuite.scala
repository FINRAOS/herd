package org.apache.spark.sql.herd

import java.text.SimpleDateFormat

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.google.common.io.Resources
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import scala.collection.JavaConverters._

import org.finra.herd.sdk.model._


private class BaseHerdApi(testCase: String, partitions: Map[(String, String), String]) extends HerdApi with Serializable {
  private val mapper = new ObjectMapper()

  override def getBusinessObjectByName(namespace: String, businessObjectName: String): BusinessObjectDefinition = {
    val bizObjJson = Resources.toString(
      Resources.getResource(s"herd-models/$testCase/businessObjectDefinition.json"), Charsets.UTF_8)
    val bizObj: BusinessObjectDefinition = mapper.readValue(bizObjJson, classOf[BusinessObjectDefinition])

    bizObj
  }

  override def registerBusinessObject(namespace: String, businessObjectName: String, dataProvider: String): Unit = {
    // no op
  }

  override def getBusinessObjectFormats(namespace: String, businessObjectName: String): BusinessObjectFormatKeys = {
    val formatKeysJson = Resources.toString(
      Resources.getResource(s"herd-models/$testCase/businessObjectFormats.json"), Charsets.UTF_8)
    val formatKeys: BusinessObjectFormatKeys = mapper.readValue(formatKeysJson, classOf[BusinessObjectFormatKeys])

    formatKeys
  }

  override def getBusinessObjectFormat(namespace: String, businessObjectName: String, formatUsage: String,
                                       formatFileType: String, formatVersion: Int): BusinessObjectFormat = {
    val formatJson = Resources.toString(
      Resources.getResource(s"herd-models/$testCase/businessObjectFormat.json"), Charsets.UTF_8)
    val format: BusinessObjectFormat = mapper.readValue(formatJson, classOf[BusinessObjectFormat])

    format
  }

  override def registerBusinessObjectFormat(namespace: String, businessObjectName: String, formatUsage: String,
                                            formatFileType: String, partitionKey: String,
                                            schema: Option[Schema]): Int = {
    // no op
    0
  }

  override def getBusinessObjectPartitions(namespace: String, businessObjectName: String, formatUsage: String,
                                           formatFileType: String, formatVersion: Int,
                                           partitionFilter: Option[PartitionFilter]): Seq[(Int, String, Seq[String], Int)] = {
    val keysJson = Resources.toString(
      Resources.getResource(s"herd-models/$testCase/businessObjectDataByFormat.json"), Charsets.UTF_8)
    val keys = mapper.readValue(keysJson, classOf[BusinessObjectDataKeys])

    keys.getBusinessObjectDataKeys.asScala.map { element =>
      (element.getBusinessObjectFormatVersion.toInt, element.getPartitionValue,
        element.getSubPartitionValues.asScala.toSeq, element.getBusinessObjectDataVersion.toInt)
    }
  }

  override def getBusinessObjectData(namespace: String, businessObjectName: String, formatUsage: String,
                                     formatFileType: String, formatVersion: Int, partitionKey: String,
                                     partitionValue: String, subPartitionValues: Seq[String],
                                     dataVersion: Int): BusinessObjectData = {
    val dataJson = Resources.toString(
      Resources.getResource(s"herd-models/$testCase/${partitions((partitionValue, subPartitionValues.mkString(",")))}"), Charsets.UTF_8)
    val data = mapper.readValue(dataJson, classOf[BusinessObjectData])

    data
  }

  override def getBusinessObjectDataGenerateDdl(namespace: String, businessObjectName: String,
                                       formatUsage: String, formatFileType: String,
                                       formatVersion: Int, partitionKey: String, partitionValues: Seq[String],
                                       dataVersion: Int): BusinessObjectDataDdl = {
    val dataFile = if (partitionValues.length == 1) {
      partitions((partitionValues(0), ""))
    } else {
      partitions((partitionValues(0), partitionValues(1)))
    }

    val dataJson = Resources.toString(Resources.getResource(s"herd-models/$testCase/$dataFile"), Charsets.UTF_8)
    val data = mapper.readValue(dataJson, classOf[BusinessObjectDataDdl])

    data

  }

  override def registerBusinessObjectData(namespace: String, businessObjectName: String, formatUsage: String,
                                          formatFileType: String, formatVersion: Int, partitionKey: String,
                                          partitionValue: String, subPartitionValues: Seq[String],
                                          status: ObjectStatus.Value, storageName: String,
                                          storageDirectory: Option[String] = None): (Int, Seq[StorageUnit]) = {
    val dataJson = Resources.toString(
      Resources.getResource(s"herd-models/$testCase/registerBusinessObjectData1.json"), Charsets.UTF_8)
    val data = mapper.readValue(dataJson, classOf[BusinessObjectData])

    (data.getVersion, data.getStorageUnits.asScala)
  }

  override def setStorageFiles(namespace: String, businessObjectName: String, formatUsage: String,
                               formatFileType: String, formatVersion: Int, partitionKey: String,
                               partitionValue: String, subPartitionValues: Seq[String], dataVersion: Int,
                               storageName: String, files: Seq[(String, Long)]): Unit = {

  }

  override def updateBusinessObjectData(namespace: String, businessObjectName: String, formatUsage: String,
                                        formatFileType: String, formatVersion: Int, partitionKey: String,
                                        partitionValue: String, subPartitionValues: Seq[String], dataVersion: Int,
                                        status: ObjectStatus.Value): Unit = {

  }

  override def removeBusinessObjectData(namespace: String, businessObjectName: String, formatUsage: String,
                                        formatFileType: String, formatVersion: Int, partitionKey: String,
                                        partitionValue: String, subPartitionValues: Seq[String],
                                        dataVersion: Int): Unit = {

  }

  override def getStorage(name: String): Storage = {
    val storage = new Storage()

    storage.setName(name)

    storage.setAttributes(List({
      val attr = new Attribute()
      attr.setName("bucket.name")
      attr.setValue("mybucket")
      attr
    }, {
      val attr = new Attribute()
      attr.setName("spark.base.path")
      attr.setValue("test-output/test-case-6")
      attr
    }).asJava)

    storage.setStoragePlatformName("S3")

    storage
  }
}

class DefaultSourceSuite extends FunSuite with BeforeAndAfterAll with Matchers {

  /* creates a local SparkSession with a mock S3A filesystem */
  private lazy val spark = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.hadoop.fs.s3a.impl", "org.apache.spark.sql.herd.MockS3AFileSystem")
    .getOrCreate()

  import spark.implicits._

  private val namespace = "FOO"

  private val businessObjectDefinitionName = "TEST"

  private val defaultParams = Map(
    "url" -> "http://localhost",
    "namespace" -> namespace,
    "businessObjectName" -> businessObjectDefinitionName,
    "businessObjectFormatFileType" -> "CSV, orc"
  )

  private val EXPECTED_ROWS = {
    val fmt = new SimpleDateFormat("yyyy-MM-dd")

    Seq(
      Row(new java.sql.Date(fmt.parse("2017-01-01").getTime), "A", "row1", 100),
      Row(new java.sql.Date(fmt.parse("2017-01-01").getTime), "A", "row2", 200),
      Row(new java.sql.Date(fmt.parse("2017-01-01").getTime), "B", "row3", 300),
      Row(new java.sql.Date(fmt.parse("2017-01-01").getTime), "B", "row4", 400),
      Row(new java.sql.Date(fmt.parse("2017-01-02").getTime), "A", "row5", 500),
      Row(new java.sql.Date(fmt.parse("2017-01-02").getTime), "A", "row6", 600),
      Row(new java.sql.Date(fmt.parse("2017-01-02").getTime), "B", "row7", 700),
      Row(new java.sql.Date(fmt.parse("2017-01-02").getTime), "B", "row8", 800)
    )
  }

  private val ORC_EXPECTED_ROWS = EXPECTED_ROWS.map(f => Row.fromSeq(f.toSeq :+ f(0)))

  private val EXPECTED_SCHEMA = new StructType()
    .add("TDATE", DateType)
    .add("SYMBOL", StringType)
    .add("COL1", StringType)
    .add("COL2", IntegerType)

  private def getDataFrame(api: HerdApi, parameters: Map[String, String]): DataFrame = {
    val source = new DefaultSource((_, _, _) => api)
    val relation = source.createRelation(spark.sqlContext, parameters)

    spark.sqlContext.baseRelationToDataFrame(relation)
  }

  private def writeDataFrame(api: HerdApi, parameters: Map[String, String],
                             df: DataFrame, mode: SaveMode = SaveMode.Append): DataFrame = {
    val source = new DefaultSource((_, _, _) => api)
    val relation = source.createRelation(spark.sqlContext, mode, parameters, df)

    spark.sqlContext.baseRelationToDataFrame(relation)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("load with minimal options") {
    val parts = Map(
      ("2017-01-01", "2017-01-02") -> "businessObjectDataDdl.json"
    )
    val df = getDataFrame(new BaseHerdApi("test-case-1", parts), defaultParams)

    val result = df.collect()

    result should contain theSameElementsAs(EXPECTED_ROWS)
  }

  test("load all partitions and filter") {
    val parts = Map(
      ("2017-01-01", "2017-01-02") -> "businessObjectDataDdl.json",
      ("2017-01-01", "") -> "businessObjectDataDdl1.json"
    )
    val df = getDataFrame(new BaseHerdApi("test-case-1", parts), defaultParams)

    val result = df.filter('tdate === "2017-01-01").collect()

    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val tdate = fmt.parse("2017-01-01")

    val expected = EXPECTED_ROWS.filter(_.getDate(0) == tdate)

    result should contain theSameElementsAs(expected)
  }

  test("load sub-partitioned data") {
    val parts = Map(
      ("2017-01-01", "2017-01-02") -> "businessObjectDataDdl.json"
    )
    val df = getDataFrame(new BaseHerdApi("test-case-3", parts), defaultParams)

    val result = df.collect()

    result should contain theSameElementsAs(EXPECTED_ROWS)
  }

  test("load sub-partitioned data and prune by partition columns") {
    val parts = Map(
      ("2017-01-01", "2017-01-02") -> "businessObjectDataDdl.json"
    )
    val df = getDataFrame(new BaseHerdApi("test-case-3", parts), defaultParams)

    val result = df.select("tdate", "symbol").collect()

    val expected = EXPECTED_ROWS.map(p => Row(p(0), p(1)))

    result should contain theSameElementsAs(expected)
  }

  test("load sub-partitioned data and filter") {
    val parts = Map(
      ("2017-01-01", "2017-01-02") -> "businessObjectDataDdl.json"
    )
    val df = getDataFrame(new BaseHerdApi("test-case-3", parts), defaultParams)

    val result = df.filter($"symbol" === "A").collect()

    val expected = EXPECTED_ROWS.filter(_.getString(1).equalsIgnoreCase("A"))

    result should contain theSameElementsAs(expected)
  }

  test("load non-partitioned data") {
    val parts = Map(("none", "") -> "businessObjectDataDdl.json")
    val df = getDataFrame(new BaseHerdApi("test-case-2", parts), defaultParams)

    val result = df.collect()

    result should contain theSameElementsAs(EXPECTED_ROWS)
  }

  test("load and filter non-partitioned data") {
    val parts = Map(("none", "") -> "businessObjectDataDdl.json")
    val df = getDataFrame(new BaseHerdApi("test-case-2", parts), defaultParams)

    val result = df.filter($"symbol" === "A").collect()

    val expected = EXPECTED_ROWS.filter(_.getString(1).equalsIgnoreCase("A"))

    result should contain theSameElementsAs(expected)
  }

  test("load from S3 storage platform") {
    val parts = Map(
      ("2017-01-01", "2017-01-02") -> "businessObjectDataDdl.json"
    )
    val df = getDataFrame(new BaseHerdApi("test-case-4", parts), defaultParams)

    val result = df.collect()

    result should contain theSameElementsAs(EXPECTED_ROWS)
  }

  test("load ORC files") {
    val parts = Map(
      ("2017-01-01", "2017-01-02") -> "businessObjectDataDdl.json"
    )
    val df = getDataFrame(new BaseHerdApi("test-case-5", parts), defaultParams)

    val rows = df.collect()

    rows should contain theSameElementsAs(ORC_EXPECTED_ROWS)
  }

  test("load ORC files, prune and filter") {
    val parts = Map(
      ("2017-01-01", "2017-01-02") -> "businessObjectDataDdl.json",
      ("2017-01-01", "") -> "businessObjectDataDdl1.json"
    )
    val df = getDataFrame(new BaseHerdApi("test-case-5", parts), defaultParams)

    val rows = df.select("tdate", "symbol").filter($"tdate" === "2017-01-01").collect()

    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val tdate = fmt.parse("2017-01-01")

    val expected = ORC_EXPECTED_ROWS.filter(_.getDate(4) == tdate).map(r => Row(r(4), r(1)))

    rows should contain theSameElementsAs(expected)
  }

  test("save partitioned data") {
    FileUtils.deleteDirectory(new java.io.File("./test-output"))

    val df = spark.createDataFrame(EXPECTED_ROWS.asJava, EXPECTED_SCHEMA).filter($"tdate" === "2017-01-01")

    val params = defaultParams + ("partitionValue" -> "2017-01-01")

    val parts = Map(
      ("2017-01-01", "") -> "businessObjectData1.json"
    )

    writeDataFrame(new BaseHerdApi("test-case-6", parts), params, df)
  }

  test("metadata only query") {
    val parts = Map(
      ("2017-01-01", "2017-01-02") -> "businessObjectDataDdl.json"
    )
    val df = getDataFrame(new BaseHerdApi("test-case-1", parts), defaultParams)

    val result = df.selectExpr("min(tdate)", "max(tdate)").collect()
  }
}
