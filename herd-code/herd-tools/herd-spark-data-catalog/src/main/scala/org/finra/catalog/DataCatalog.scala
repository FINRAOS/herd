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

import java.io.File
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.xml._

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.kms.AWSKMSClient
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.herd._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import org.finra.herd.sdk.api._
import org.finra.herd.sdk.invoker.ApiException
import org.finra.herd.sdk.model._

/**
  * Used to contain a partition's name and value pair
  *
  * @param n name of the partition
  * @param v value for the partition name
  */
case class Partition(n: String, v: String)

/**
  * Business Object Definition
  *
  * Defines the schema of the DataFrame for business object definitions.
  *
  * @param namespace      namespace of the definition
  * @param definitionName name of the business object
  */
case class BusinessObjectDefinition(namespace: String, definitionName: String)

/**
  * Business Object Format
  *
  * Defines schema of DataFrame for business object formats
  *
  */
case class BusinessObjectFormat(namespace: String, definitionName: String, formatUsage: String, formatFileType: String, formatVersion: Integer)

/**
  * Class to create DataFrames of data registered with Herd
  * Goal of the object is to provide a facility to create Spark DataFrames of any businessObject
  * registered with Herd
  *
  * Browse for available objects
  * get necessary parameters to query for a specific businessObject
  * get the businessObject as a Spark DataFrame
  *
  * @todo get parsing details from the format, do not hard code
  *
  * - get a data frame given object identifiers
  * - be optimal on what file format is used (select format for user if possible)
  * - if format contains schema (ORC, parquet) no need to ask for schema from Herd
  * @todo add way to browse available partitions
  * @todo handle multiple levels of partitions (today only does one)
  * @todo use parsing parameters as given by businessObjectFormats call to Herd
  * @param spark the spark session
  *
  */
// noinspection SimplifyBoolean

  class DataCatalog(val spark: SparkSession, host: String) extends Serializable {

    import spark.implicits._

    // for contacting DM service
    var username = ""
    var password = ""

    val usage = "PRC"
    val fileFormat = "UNKNOWN"
    val storageUnit = "S3_DATABRICKS"
    val ds: DefaultSource.type = DefaultSource

    // used for logging
    private val logger = LoggerFactory.getLogger(getClass)

    // for credStash
    var credStash: CredStashWrapper = getCredStash
    var herdApiWrapper: HerdApiWrapper = new HerdApiWrapper

    // XML pretty printer
    private val printer = new scala.xml.PrettyPrinter(80, 4)

    def printXMLString(s: String): String = printer.format(scala.xml.XML.loadString(s))

    def printXML(x: scala.xml.Elem): String = printer.format(x)

    //  val nullValue = "\\N"
    val dateFormat = "yyyy-MM-dd"

    private def baseRestUrl: String = {
      host + "/herd-app/rest"
    }

    /**
      * Create a credStash instance
      *
      * @return CredStashWrapper instance
      */
    private def getCredStash: CredStashWrapper = {
      logger.info("Creating credStash wrapper")

      val proxyHost = spark.conf.getOption("spark.herd.proxy.host")
      val proxyPort = spark.conf.getOption("spark.herd.proxy.port")

      val clientConf = new ClientConfiguration
      if (proxyHost.isDefined && proxyPort.isDefined) {
        clientConf.setProxyHost(proxyHost.get)
        clientConf.setProxyPort(Integer.parseInt(proxyPort.get))
      }

      val provider = new DefaultAWSCredentialsProviderChain
      val ddb: AmazonDynamoDBClient = new AmazonDynamoDBClient(provider, clientConf).withRegion(Regions.US_EAST_1)
      val kms: AWSKMSClient = new AWSKMSClient(provider, clientConf).withRegion(Regions.US_EAST_1)
      new CredStashWrapper(ddb, kms)
    }

    /**
      * Auxiliary constructor using credstash"
      *
      * @param spark         spark context
      * @param host          DM host https://host.name.com:port
      * @param credName      credential name (e.g. username for Herd)
      * @param credAGS       AGS for credential lookup
      * @param credSDLC      SDLC for credential lookup
      * @param credComponent Component for credential lookup
      *
      */
    def this(spark: SparkSession,
             host: String,
             credName: String,
             credAGS: String,
             credSDLC: String,
             credComponent: String
            ) {
      // core constructor
      this(spark, host)

      logger.info("credName=" + credName)
      logger.info("credAGS=" + credAGS)
      logger.info("credSDLC=" + credSDLC)
      logger.info("credComponent=" + credComponent)

      this.username = credName
      this.password = getPassword(spark, credName, credAGS, credSDLC, credComponent)

      herdApiWrapper = new HerdApiWrapper(ds, baseRestUrl, username, password)
    }

    /**
      * Auxiliary constructor using username/password
      *
      * @param spark    spark session
      * @param username username for DM
      * @param password password for DM
      * @param host     DM host https://host.name.com:port
      */
    def this(spark: SparkSession, host: String, username: String, password: String) {
      this(spark, host)

      this.username = username
      this.password = password

      herdApiWrapper = new HerdApiWrapper(ds, baseRestUrl, username, password)
    }

    /**
      * Union the DataFrame with a unioned schema, missing columns get null values
      *
      * @param otherDF DataFrame
      * @return DataFrame
      */
    def unionUnionSchema(dataFrame: DataFrame, otherDF: DataFrame): DataFrame = {
      val dataFrameCols = dataFrame.columns.toList
      val otherDFCols = otherDF.columns.toList
      val allCols = (dataFrameCols ++ otherDFCols).distinct

      dataFrame.select(mergeColumns(dataFrameCols, allCols): _*).union(otherDF.select(mergeColumns(otherDFCols, allCols): _*))
    }

    /**
      * Add missing columns to the data frame schema
      *
      * @param cols    list of columns for a data frame schema
      * @param allCols lif of all columns from data frame schemas
      *
      */
    private def mergeColumns(cols: List[String], allCols: List[String]) = {
      allCols.map(x => x match {
        case x if cols.contains(x) => col(x)
        case _ => lit(null).as(x)
      })
    }

    /**
      * Retrieve the password using credstash
      *
      * @param spark         spark context
      * @param credName      credential name (e.g. username for DM)
      * @param credAGS       AGS for credential lookup
      * @param credSDLC      SDLC for credential lookup
      * @param credComponent Component for credential lookup
      * @return the password
      */
    private[catalog] def getPassword(spark: SparkSession,
                            credName: String,
                            credAGS: String,
                            credSDLC: String,
                            credComponent: String = null
                           ): String = {
      val context = new util.HashMap[String, String] {
        put("AGS", credAGS)
        put("SDLC", credSDLC)
        if (credComponent != null) {
          put("Component", credComponent)
        }
      }

      var prefixedCredName: String = null
      if (context.containsKey("Component")) {
        prefixedCredName = context.get("AGS") + "." + context.get("Component") + "." + context.get("SDLC") + "." + credName
      } else {
        prefixedCredName = context.get("AGS") + "." + context.get("SDLC") + "." + credName
      }

      credStash.getSecret(prefixedCredName, context)
    }

    /**
      * It searches for business object data in a namespace and object
      *
      * @param ns  namespace
      * @param obj object name
      * @return list of (object name, partition value) tuples
      */
    private[catalog] def dmSearchRequest(ns: String, obj: String): util.List[BusinessObjectData] = {
      val businessObjectDataSearchKey = new BusinessObjectDataSearchKey()
      businessObjectDataSearchKey.setNamespace(ns)
      businessObjectDataSearchKey.setBusinessObjectDefinitionName(obj)
      val businessObjectDataSearchFilter = new BusinessObjectDataSearchFilter()
      businessObjectDataSearchFilter.addBusinessObjectDataSearchKeysItem(businessObjectDataSearchKey)
      val businessObjectDataSearchRequest = new BusinessObjectDataSearchRequest()
      businessObjectDataSearchRequest.addBusinessObjectDataSearchFiltersItem(businessObjectDataSearchFilter)

      herdApiWrapper.getHerdApi.searchBusinessObjectData(businessObjectDataSearchRequest).getBusinessObjectDataElements
    }

    /**
      * Like dmSearchRequest but returns a list of tuples
      *
      * @param ns  namespace
      * @param obj object name
      * @return list of (object name, partition value) tuples
      */
    private[catalog] def dmSearch(ns: String, obj: String): List[(String, String)] = {
      val businessObjectDataElements = dmSearchRequest(ns, obj)

      (for (businessObjectData <- businessObjectDataElements.asScala)
        yield (businessObjectData.getBusinessObjectDefinitionName, businessObjectData.getPartitionValue)).toList
    }

    /**
      * Deletes registered formats for an object in DM (private function to not allow users accidentally execute it)
      *
      * @param ns     namespace
      * @param obj    object name
      * @param schema schema version
      * @return nothing
      */
    private[catalog] def dmDeleteFormat(ns: String, obj: String, schema: Integer, usage: String = "PRC", format: String = "PARQUET"): Unit = {
      logger.debug(s"Deleting registered formats for obj $obj")
      try {
        herdApiWrapper.getHerdApi.removeBusinessObjectFormat(ns, obj, usage, format, schema)
      } catch {
        case _: Throwable => logger.debug("WARNING: Could not remove format.  Ignoring...")
      }
    }

    /**
      * Deletes object definition in DM  (private[catalog] function to not allow users accidentally execute it)
      *
      * @param ns  namespace
      * @param obj object name
      * @return nothing
      */
    private[catalog] def dmDeleteObjectDefinition(ns: String, obj: String): Unit = {
      logger.debug(s"Deleting obj definition for $obj")
      try {
        herdApiWrapper.getHerdApi.removeBusinessObjectDefinition(ns, obj)
      } catch {
        case _: Throwable => logger.debug("WARNING: Could not remove object definition.  Ignoring...")
      }
    }

    /**
      * Deletes registered partitions and files for a object in DM  (private[catalog] function to not allow users accidentally execute it)
      *
      * @param ns          namespace
      * @param obj         object name
      * @param deleteFiles set to true if want to delete files
      */
    private[catalog] def dmDeleteObjectPartitions(ns: String, obj: String, deleteFiles: Boolean = false): Unit = {
      val objList = dmSearchRequest(ns, obj)

      val partitions = (for (businessObjectData <- objList.asScala)
        yield (businessObjectData.getNamespace, businessObjectData.getBusinessObjectDefinitionName,
          businessObjectData.getBusinessObjectFormatUsage, businessObjectData.getBusinessObjectFormatFileType,
          businessObjectData.getBusinessObjectFormatVersion, businessObjectData.getPartitionKey,
          businessObjectData.getPartitionValue, businessObjectData.getVersion)).toList

      for ((ns, obj, usage, format, schema, pk, part, version) <- partitions) {
        logger.debug(s"Deleting registered partitions of $obj")
        herdApiWrapper.getHerdApi.removeBusinessObjectData(ns, obj, usage, format, schema, pk, part, Seq(), version)
      }
    }

    /**
      * Gets all objects registered in a namespace as a list of names
      *
      * @param ns namespace
      * @return list of names
      */
    def dmAllObjectsInNamespace(ns: String): List[String] = {

      val businessObjectDefinitionKeys = herdApiWrapper.getHerdApi.getBusinessObjectsByNamespace(ns).getBusinessObjectDefinitionKeys
      (for (obj <- businessObjectDefinitionKeys.asScala) yield obj.getBusinessObjectDefinitionName).toList
    }

    /**
      * This function removes objects, schemas, partitions, and files from a namespace.
      *
      * @param ns the namespace to wipe
      */

    def dmWipeNamespace(ns: String): Unit = {
      val toDelete = dmAllObjectsInNamespace(ns)
      for (name <- toDelete) {
        dmDeleteFormat(ns, name, 0)
        dmDeleteObjectDefinition(ns, name)
        dmDeleteObjectPartitions(ns, name)
      }
    }

    /**
      * Get the parent directory of the given file/path (assumed to be a file, but does not matter as per description)
      *
      * @param fileNameOrPath full filename or path
      * @return parent directory if path, containing directory if file
      */
    private[catalog] def getParent(fileNameOrPath: String): String = {
      val aFile = new File(fileNameOrPath)

      // this will return everything before the last "/"
      aFile.getParent
    }

    /**
      * Returns the Spark SQL Structure type base on the given schema string value.  default return is StringType
      *
      * @param columnType Name from DM for the column type
      * @return DataType of the string value
      */
    private[catalog] def getStructType(columnType: String, columnSize: String): DataType = {
      val dbType = columnType match {
        case "STRING" => StringType
        case "DATE" => DateType
        case "TIMESTAMP" => TimestampType
        case "VARCHAR" => StringType
        case "DECIMAL" =>
          val ss = columnSize.split(",")
          if (ss.length != 2) {
            DecimalType(18, 8)
          } else {
            DecimalType(ss(0).toInt, ss(1).toInt)
          }
        case "BIGINT" => LongType
        case "INT" => IntegerType
        case "SMALLINT" => ShortType
        case _ => StringType
      }
      dbType
    }

    /**
      * Convert the given schema into a schema of all StringTypes
      *
      * @param schema schema to convert into all StringTypes
      * @return a schema of all string types
      */
    private[catalog] def convertToAllStringTypes(schema: StructType): StructType = {
      val allStringSchema = schema.map { f =>
        StructField(f.name, StringType, f.nullable, f.metadata)
      }

      StructType(allStringSchema)
    }

    /**
      * Cast columns in the given dataframe to the targetSchema.
      * Column names that match will be cast over
      * As a safety measure, first casting to string then to the desired type
      *
      * @param df           the source data frame
      * @param targetSchema the schema to cast columns into
      * @return data frame with schema of targetSchema
      *
      */
    private[catalog] def convertToSchema(df: DataFrame, targetSchema: StructType, nullValue: String): DataFrame = {
      logger.debug("Source Schema:")
      logger.debug(df.schema.treeString)
      logger.debug("Dest Schema:")
      logger.debug(targetSchema.treeString)

      var retDF = df

      // for each column
      targetSchema.foreach { tField =>

        // column we are going to cast to target type
        var aCol: Column = retDF.col(tField.name).cast(StringType)

        // cast to string if not a string
        val i = df.schema.fieldIndex(tField.name)

        if (i > 0 && df.schema.fields.lift(i).get.dataType != StringType) {
          aCol = aCol.cast(StringType)
        }

        // if nullable, clean nulls
        if (tField.nullable) aCol = when(aCol.notEqual(nullValue), aCol).otherwise(lit(null))

        // finally: cast column to right target type
        retDF = retDF.withColumn(tField.name, aCol.cast(tField.dataType))
      }

      retDF
    }

    /**
      * convenience function for creating dataframes
      *
      * @param readFormat  format to use when reading the files
      * @param readOptions reading options key, value map; used by the reader
      * @param readSchema  schema of the file being read
      * @param path        full path to read, can be parent directory
      * @return
      */
    private[catalog] def createDataFrame(readFormat: String, readOptions: Map[String, String], readSchema: StructType, path: String): DataFrame = {

      val df = {
        // If readSchema was not null, we've already got it, so use it (for non-orc files). If not, try to get it from the file itself (for orc files).
        // If we can't get a schema from the file, try to read it without supplying the schema. This is the least efficient choice but better than nothing.
        if (readSchema != null && !readFormat.equals("orc")) {
          spark.sqlContext.read.format(readFormat).options(readOptions).schema(readSchema).load(path.replaceAll("s3n://", "s3a://"))
        }

        else {
          // This is here as a sort of worst case fallback, but this should never get executed. If the file is ORC, we will read schema from file,
          // and if it's bz, txt, or csv, use parseSchema.
          spark.sqlContext.read.format(readFormat).options(readOptions).load(path.replaceAll("s3n://", "s3a://"))
        }
      }
      df

    }

    /**
      * Queries for the path(s) of the namespace / object
      *
      * @param namespace              DM namespace
      * @param objectName             DM objectName
      * @param usage                  usage in DM
      * @param fileFormat             file format in DM
      * @param partitionValuesInOrder value of list of partition (has to be in order) ++ adding List(Partition) -> redefine Partition to Maps
      * @return XML response from DM REST call
      */
    def queryPath(namespace: String, objectName: String, usage: String, fileFormat: String, partitionKey: String, partitionValuesInOrder: Array[String],
                  schemaVersion: Integer = null, dataVersion: Integer = null): String = {
      val businessObjectData = herdApiWrapper.getHerdApi.getBusinessObjectData(namespace, objectName,
        usage, fileFormat, schemaVersion, partitionKey, partitionValuesInOrder(0),
        partitionValuesInOrder.drop(1), dataVersion)

      val xmlMapper = new XmlMapper
      xmlMapper.writeValueAsString(businessObjectData)
    }

    /**
      * Queries for the path(s) of the namespace / object
      *
      * @param namespace              DM namespace
      * @param objectName             DM objectName
      * @param usage                  usage in DM
      * @param fileFormat             file format in DM
      * @param partitionValuesInOrder value of list of partition (has to be in order) ++ adding List(Partition) -> redefine Partition to Maps
      * @return list of S3 key prefixes
      */
    private[catalog] def queryPathFromGeneratePartitions(namespace: String, objectName: String, usage: String, fileFormat: String,
       partitionKey: String, partitionValuesInOrder: Array[String], schemaVersion: Integer = null, dataVersion: Integer = null): List[String] = {

      val businessObjectDataPartitions = herdApiWrapper.getHerdApi().getBusinessObjectDataPartitions(namespace, objectName, usage, fileFormat,
        schemaVersion, partitionKey, partitionValuesInOrder, dataVersion)

      // get partitions information from generate partitions response
      val generatedPartitions = businessObjectDataPartitions.getPartitions

      // Parse the response, grab the partition values and their S3 prefixes
      val s3KeyPrefixes = new ListBuffer[String]()
      for (partition <- generatedPartitions.asScala) {
        logger.debug(s"partition location: ${partition.getPartitionLocation}")
        s3KeyPrefixes.append(partition.getPartitionLocation)
      }

      // return all s3 prefixes
      s3KeyPrefixes.map("s3n://" + _).toList
    }

    /**
      * Get available namespaces
      *
      * @param namespaceCode specific namespace, if empty searches all namespaces
      * @return list of namespaces
      */
    def getNamespaces(namespaceCode: String = ""): List[String] = {

      if (namespaceCode.isEmpty) {
        val namespaceKeys = herdApiWrapper.getHerdApi.getAllNamespaces.getNamespaceKeys

        (for (namespaceKey <- namespaceKeys.asScala) yield namespaceKey.getNamespaceCode).toList
      } else {
        val namespace = herdApiWrapper.getHerdApi.getNamespaceByNamespaceCode(namespaceCode)
        List(namespace.getNamespaceCode)
      }
    }

    /**
      * Get available business object definitions
      *
      * @param namespace specific namespace, if empty searches all namespaces
      * @return DataFrame of BusinessObjectDefinition objects
      */
    def getBusinessObjectDefinitions(namespace: String = ""): DataFrame = {

      val businessObjectDefinitionKeys = herdApiWrapper.getHerdApi.getBusinessObjectsByNamespace(namespace).getBusinessObjectDefinitionKeys

      (for (businessObjectDefinitionKey <- businessObjectDefinitionKeys.asScala)
        yield BusinessObjectDefinition(businessObjectDefinitionKey.getNamespace,
          businessObjectDefinitionKey.getBusinessObjectDefinitionName)).toDF
    }

    /**
      * calls the rest function to get the business object formats.
      *
      * @param namespace     namespace in DM
      * @param objectName    objectName in DM
      * @param usage         usage in DM
      * @param fileFormat    file format for object
      * @param schemaVersion schema version, null for latest
      * @return business object format instance
      *
      */
    private[catalog] def callBusinessObjectFormatQuery(namespace: String, objectName: String, usage: String, fileFormat: String, schemaVersion: Integer = null):
    org.finra.herd.sdk.model.BusinessObjectFormat = {
      return herdApiWrapper.getHerdApi.getBusinessObjectFormat(namespace, objectName, usage, fileFormat, schemaVersion)
    }

    /**
      * get the business object formats for the given object definition in the namespace
      *
      * @param namespace                    namespace to find the definition
      * @param businessObjectDefinitionName name of the object definition
      * @return DataFrame of business object formats
      */
    def getBusinessObjectFormats(namespace: String, businessObjectDefinitionName: String, latestVersion: Boolean = true): DataFrame = {

      val businessObjectFormatKeys = herdApiWrapper.getHerdApi.getBusinessObjectFormats(namespace, businessObjectDefinitionName, latestVersion)
        .getBusinessObjectFormatKeys

      (for (businessObjectFormatKey <- businessObjectFormatKeys.asScala)
        yield BusinessObjectFormat(businessObjectFormatKey.getNamespace,
          businessObjectFormatKey.getBusinessObjectDefinitionName,
          businessObjectFormatKey.getBusinessObjectFormatUsage,
          businessObjectFormatKey.getBusinessObjectFormatFileType,
          businessObjectFormatKey.getBusinessObjectFormatVersion)).toDF
    }

    /**
      * get the bucket name from the path
      *
      * @param path XML path response from DM
      * @return list of paths strings
      */
    private[catalog] def getS3Paths(path: Elem, fileFormat: String): List[String] = {

      // Fetch only enabled storage units
      val storageUnitPath = (path \\ "storageUnits" \\ "storageUnit")
        .filter(p => (p \\ "storageUnitStatus") // Find storage unit status
          .flatMap(_.child.map(_.text)).contains("ENABLED")) // get all status values and check for enabled

      val s3BucketNames = (storageUnitPath \\ "storageUnit" \\ "attributes" \ "attribute").map { a =>

        val n = (a \ "name") text
        val v = (a \ "value") text

        if (n == "bucket.name") {
          v
        } else {
          ""
        }

      }.filter(p => p != "")

      // if fileFormat is "bz" its actual bz2
      var fFormat = fileFormat.toLowerCase

      if (fFormat == "bz") {
        fFormat = "bz2"
      }

      val bucketName = s3BucketNames.head
      val storageName = (storageUnitPath \\ "storageUnit" \\ "storage" \ "name") text
      var storagePlatform = ((storageUnitPath \\ "storageUnit" \\ "storage" \ "storagePlatformName") text).toLowerCase

      // BUG need to use s3a for buckets, if you want to use AMI Roles for access
      if (storagePlatform.equalsIgnoreCase("S3")) {
        storagePlatform = "s3a"
      }

      // best to use the exact filenames given by DM...
      // 9/20: S3 wants the path/bucket not endpoints; get parent path and then distinct (should only be one)
      val s3Paths = storageName match {
        case "???S3_MANAGED" => // noinspection RedundantBlock
        {
          // NOT IDEAL use the actual file names
          (storageUnitPath \\ "storageUnit" \ "storageDirectory").map(p => storagePlatform + "://" + p.text + s"/*.$fFormat")
        }
        case _ =>
          var p =
            (storageUnitPath \\ "storageUnit" \\ "storageFiles" \\ "storageFile" \ "filePath").map { x =>
              storagePlatform + "://" + bucketName + "/" + getParent(x.text)
            }

          if (p.isEmpty) {
            p = (storageUnitPath \\ "storageUnit" \ "storageDirectory").map { y =>
              storagePlatform + "://" + bucketName + "/" + y.text
            }
          }

          p
      }


      // avoid duplicates
      s3Paths.toList.distinct
    }

    /**
      * Queries REST and returns the Spark SQL StructFields for the given data object
      *
      * @param namespace     namespace in DM
      * @param objectName    objectName in DM
      * @param usage         usage in DM
      * @param fileFormat    file format for object
      * @param schemaVersion schema version, <0 for latest
      * @return Spark schema (structFields)
      */
    def getSchema(namespace: String, objectName: String, usage: String, fileFormat: String, schemaVersion: Integer = null): StructType = {

      val bFormat = callBusinessObjectFormatQuery(namespace, objectName, usage, fileFormat, schemaVersion)

      parseSchema(bFormat)
    }

    /**
      * Retrieve the format schema
      *
      * @param businessObjectFormat the business object format instance
      * @return Spark schema (structFields)
      */
    private[catalog] def parseSchema(businessObjectFormat: org.finra.herd.sdk.model.BusinessObjectFormat): StructType = {
      val columns = businessObjectFormat.getSchema.getColumns.asScala

      // map the columns to a list of StructFields
      val fields = columns.map { c =>
        val n = c.getName
        val t = c.getType
        val s = c.getSize

        val required = c.getRequired
        val description = c.getDescription

        // create a comment on each field, just to show how you can (ideal would be from DM)
        val commentJSON =
          s"""{"COMMENT" : "$description"}"""

        StructField(name = n,
          dataType = getStructType(t, s),
          nullable = Try(required.booleanValue()).getOrElse(true),
          Metadata.fromJson(commentJSON))
      }

      // return the StructType
      StructType(fields)
    }

    /**
      * gets the parse options for given object and schema version
      *
      * @param namespace     namespace in DM
      * @param objectName    objectName in DM
      * @param usage         usage in DM
      * @param fileFormat    file format for object
      * @param schemaVersion schema version, <0 for latest
      * @return Map of parse options
      */
    private[catalog] def getParseOptions(namespace: String, objectName: String, usage: String, fileFormat: String, schemaVersion: Integer = null,
                                         csvBug: Boolean = false): Map[String, String] = {

      val businessObjectFormat = callBusinessObjectFormatQuery(namespace, objectName, usage, fileFormat, schemaVersion)
      parseParseOptions(businessObjectFormat, csvBug)
    }

    /**
      * Returns the parse options
      * todo: this is a quick fix, use unescape utils to elegantly handle this
      *
      * @param businessObjectFormat the business object format instance
      * @return Map of parse options
      */
    private[catalog] def parseParseOptions(businessObjectFormat: org.finra.herd.sdk.model.BusinessObjectFormat, csvBug: Boolean): Map[String, String] = {

      val actualNullValue: Option[String] = Option(businessObjectFormat.getSchema.getNullValue)
      val actualDelimiter: Option[String] = Option(businessObjectFormat.getSchema.getDelimiter)
      val actualEscapeCharacter: Option[String] = Option(businessObjectFormat.getSchema.getEscapeCharacter)

      var nullValue = None: Option[String]
      var delimiter = None: Option[String]
      var escapeCharacter = None: Option[String]

      actualNullValue match {
        case Some(x) => if (x equalsIgnoreCase "\\\\") nullValue = Option("\\") else nullValue = Option(x)
        case None => nullValue = None
      }

      actualDelimiter match {
        case Some(x) =>
          if (x equalsIgnoreCase "\\\\") delimiter = Option("\\")
          else if (x equalsIgnoreCase "\\") delimiter = Option("\\")
          else if (x contains "\\") delimiter = Option(x.replace("\\u", "").replace("\\", "").toInt.toChar.toString)
          else delimiter = Option(x)
        case None => delimiter = None
      }

      actualEscapeCharacter match {
        case Some(x) =>
          if (x equalsIgnoreCase "\\\\") escapeCharacter = Option("\\")
          else if (x equalsIgnoreCase "\\") escapeCharacter = Option("\\")
          else if (x contains "\\") escapeCharacter = Option(x.replace("\\u", "\\").replace("\\", ""))
          else escapeCharacter = Option(x)
        case None => escapeCharacter = None
      }

      var parseOptions = scala.collection.mutable.Map(
        "delimiter" -> delimiter.orNull,
        "mode" -> "PERMISSIVE",
        "escape" -> escapeCharacter.orNull,
        "nullValue" -> nullValue.orNull
      )

      if (!csvBug) {
        parseOptions += "dateFormat" -> dateFormat
      }

      parseOptions.toMap
    }

    /**
      * Queries and returns the Spark SQL StructFields of partitions of the given object
      *
      * @param namespace     namespace in DM
      * @param objectName    objectName in DM
      * @param usage         usage in DM
      * @param fileFormat    file format for object
      * @param schemaVersion schema version, <0 for current
      * @return Spark schema (structFields)
      */
    def getPartitions(namespace: String, objectName: String, usage: String, fileFormat: String, schemaVersion: Integer = null): StructType = {
      val businessObjectFormat = callBusinessObjectFormatQuery(namespace, objectName, usage, fileFormat, schemaVersion)
      parsePartitions(businessObjectFormat)
    }

    /**
      * Retrieve the schema partitions
      *
      * @param businessObjectFormat the business object format instance
      * @return Spark schema (structFields)
      */
    private[catalog] def parsePartitions(businessObjectFormat: org.finra.herd.sdk.model.BusinessObjectFormat): StructType = {
      // get the partitionKey
      val partitionKey = businessObjectFormat.getPartitionKey

      // get the list of partitions
      val partitions = businessObjectFormat.getSchema.getPartitions.asScala

      // get from the XML the partition columns, map them to a list of StructFields
      val fields = partitions.map { c =>
        val n = c.getName
        val t = c.getType
        val s = c.getSize

        val description = c.getDescription

        // create a comment on each field, just to show how you can (ideal would be from DM)
        val commentJSON =
          s"""{"COMMENT" : "$description"}"""

        StructField(name = n,
          dataType = getStructType(t, s),
          nullable = false, // partitions are non-nullable
          Metadata.fromJson(commentJSON))
      }

      // return the partions with schema
      // if no partitions registered, return the partitionKey
      if (fields.isEmpty) {
        StructType(StructField(partitionKey, StringType, nullable = false) :: Nil)
      } else {
        StructType(fields)
      }
    }

    /**
      * Checks that the defined list of partitions exists in the list of given partition values.
      *
      * @param definedPartitions partitions as defined/returned by DM
      * @param givenPartitions   partitions as given from caller
      * @return partitions are good
      */
    private[catalog] def checkPartitions(definedPartitions: StructType, givenPartitions: List[Partition]): Boolean = {
      // check: do partition keys match with what was given?
      if (definedPartitions.length != givenPartitions.length) {
        logger.error("Partitions do not match (definedPartitions.length != givenPartitions.length)")
        return false
      }


      // get the list of keys
      val keys = givenPartitions.map(v => v.n)

      if (definedPartitions.fieldNames.count(keys.toSet) != definedPartitions.length) {
        logger.error("Partitions do not match (definedPartitions.fieldNames.count(keys.toSet) != definedPartitions.length)")
        return false
      }
      true
    }

    /**
      * get the S3 locations as a collection of strings for the given object and partition
      *
      * @param namespace       namespace in DM
      * @param objectName      objectName in DM
      * @param usage           usage of data in DM
      * @param fileFormat      file format in DM
      * @param partitionValues value of Map of partition (order-independent)
      * @return list of paths for the data
      */

    def getPaths(namespace: String, objectName: String, usage: String, fileFormat: String, partitionValues: List[Partition], schemaVersion: Integer = null,
                 dataVersion: Integer = null): List[String] = {
      // make  partitions in order
      val allPartitionKeys = getPartitions(namespace, objectName, usage, fileFormat, schemaVersion).fieldNames
      val partitionsMap = partitionValues.filter(p => p.v != null).flatMap(x => List(x.n.toLowerCase -> x.v.toLowerCase)).toMap

      val partitionValuesInOrder = allPartitionKeys.filter(x => partitionsMap isDefinedAt x.toLowerCase).flatMap { x =>
        List(x -> partitionsMap(x.toLowerCase))
      }.map(_._2)

      queryPathFromGeneratePartitions(namespace, objectName, usage, fileFormat, allPartitionKeys(0), partitionValuesInOrder, schemaVersion, dataVersion)
    }

    /**
      * Returns data availability
      *
      * @param namespace    DM namespace
      * @param objectName   DM object name
      * @param usage        usage of data in DM
      * @param fileFormat   file format in DM
      * @param partitionKey partitionKey for the object
      * @return
      */
    def getDataAvailabilityRange(namespace: String, objectName: String, usage: String, fileFormat: String, partitionKey: String, firstPartValue: String,
                                 lastPartValue: String, schemaVersion: Integer = null): DataFrame = {

      val businessObjectFormat = callBusinessObjectFormatQuery(namespace, objectName, usage, fileFormat, schemaVersion)
      val parts = parsePartitions(businessObjectFormat)

      logger.debug(s"Partitions: $parts")

      // Full schema for returned object; Partitions and their types at the end
      val schema = StructType(
        StructField("Namespace", StringType, nullable = false) ::
          StructField("ObjectName", StringType, nullable = false) ::
          StructField("Usage", StringType, nullable = false) ::
          StructField("FileFormat", StringType, nullable = false) ::
          StructField("FormatVersion", IntegerType, nullable = false) ::
          StructField("DataVersion", IntegerType, nullable = false) ::
          StructField("Reason", StringType, nullable = false) :: parts.toList)

      // get data availability
      val businessObjectDataAvailability = herdApiWrapper.getHerdApi.getBusinessObjectDataAvailability(namespace, objectName, usage, fileFormat,
        partitionKey, firstPartValue, lastPartValue)
      val businessObjectDataAvailableStatuses = businessObjectDataAvailability.getAvailableStatuses

      val availData = (
        for (businessObjectDataAvailableStatus <- businessObjectDataAvailableStatuses.asScala)
          yield Row.fromSeq(Seq(businessObjectDataAvailability.getNamespace, businessObjectDataAvailability.getBusinessObjectDefinitionName,
            businessObjectDataAvailability.getBusinessObjectFormatUsage, businessObjectDataAvailability.getBusinessObjectFormatFileType,
            businessObjectDataAvailableStatus.getBusinessObjectFormatVersion.toString, businessObjectDataAvailableStatus.getBusinessObjectDataVersion.toString,
            businessObjectDataAvailableStatus.getReason, businessObjectDataAvailableStatus.getPartitionValue,
            businessObjectDataAvailableStatus.getSubPartitionValues))
        ).toList

      // make list an RDD
      val rdd = spark.sparkContext.makeRDD(availData)

      // use a string schema first
      val sschema = convertToAllStringTypes(schema)

      // dataframe with a string schema
      val df = spark.createDataFrame(rdd, sschema)

      // convert to final schema (ints, dates, etc.)
      val parseOptions = parseParseOptions(businessObjectFormat, csvBug = true)

      convertToSchema(df, schema, parseOptions("nullValue"))
    }

    /**
      * Gets data availability
      *
      * @param namespace     DM namespace
      * @param objectName    DM object name
      * @param usage         usage of data in DM
      * @param fileFormat    file format in DM
      * @param schemaVersion version of schema, <0 for latest
      * @return
      */
    def getDataAvailability(namespace: String, objectName: String, usage: String, fileFormat: String, schemaVersion: Integer = null): DataFrame = {

      // need the first partition (the partitionKey)
      val parts = getPartitions(namespace, objectName, usage, fileFormat, schemaVersion)

      logger.debug(s"Partitions: $parts")

      val partitionKey = parts.head.name

      val firstPartValue = "2000-01-01"
      val lastPartValue = "2099-12-31"

      getDataAvailabilityRange(namespace, objectName, usage, fileFormat, partitionKey, firstPartValue, lastPartValue, schemaVersion)
    }

    /**
      * Given the object location identifiers, will load the data from S3 and return the Spark DataFrame
      *
      * Works for a list of partitions
      *
      * @param namespace       DM namespace
      * @param objectName      DM object name
      * @param usage           usage of data in DM
      * @param fileFormat      file format in DM
      * @param partitionValues DM partition/value List
      * @return Spark DataFrame of data
      */
    // noinspection SimplifyBoolean
    def getDataFrame(namespace: String, objectName: String, usage: String, fileFormat: String, partitionValues: List[Partition], schemaVersion: Integer = null,
                     dataVersion: Integer = null): DataFrame = {

      // get the fileReading format. We treat ORC differently from the others..
      val sparkReader = fileFormat.toUpperCase match {
        case "ORC" => "orc"
        case "PARQUET" => "parquet"
        case _ => "csv"
      }

      // bug in Spark 2.0 when reading csv files, account for it here
      val csvBug = spark.version.startsWith("2.0") && sparkReader == "csv"
      var useSchemaFromREST = true

      logger.debug("csvBug? " + csvBug)

      // call REST once
      val businessObjectFormat = callBusinessObjectFormatQuery(namespace, objectName, usage, fileFormat, schemaVersion)

      var readOptions = parseParseOptions(businessObjectFormat, csvBug)
      val finalSchema = parseSchema(businessObjectFormat)
      val parts = parsePartitions(businessObjectFormat)

      // read options only for CSV files; ORC files won't work with the correct schema. We have to read with the embedded schema and then rename columns.
      if (sparkReader == "orc" || sparkReader == "parquet") {
        readOptions = Map[String, String]()
        useSchemaFromREST = false
      }

      // check: do partition keys match with what was given?
      // noinspection SimplifyBoolean
      if (checkPartitions(parts, partitionValues) == false) {
        logger.error("Partitions do not match (checkPartitions returned false)")

        // return null (error) dataframe
        return null
      }

      val s3Paths = getPaths(namespace, objectName, usage, fileFormat, partitionValues, schemaVersion, dataVersion)
      logger.debug("s3Paths=" + s3Paths)

      var readSchema = if (useSchemaFromREST) finalSchema else null

      // read everything as a string first if csv bug is in effect
      if (csvBug) {
        readSchema = convertToAllStringTypes(finalSchema)
      }

      var retDF: DataFrame = null

      // Multiple directories MAY mean we have multiple partitions
      for (aPath <- s3Paths) {
        var aDF = createDataFrame(sparkReader, readOptions, readSchema, aPath)

        // Convert dataframe to right schema
        if (csvBug) {
          aDF = convertToSchema(aDF, finalSchema, readOptions("nullValue").toString)
        }

        // make sure the final names match (in case of ORC, it will not; this covers any issues)
        val finalNames = finalSchema.map { n => n.name }
        aDF = aDF.toDF(finalNames: _*)

        // add the partition columns
        for (aPart <- parts) {

          var partValue = partitionValues.filter(p => p.n == aPart.name).head.v

          // IF VALUE WAS NULL: Discover the partition from the path
          if (partValue == null) {
            val bits = aPath.split("=").map(_.trim)
            partValue = bits.last
          }

          // retDF.withColumn(tField.name, aCol.cast(tField.dataType))
          aDF = aDF.withColumn(aPart.name, lit(partValue).cast(aPart.dataType))
        }

        // build up the dataframe
        if (retDF == null) {
          retDF = aDF
        } else {
          retDF = aDF.union(retDF)
        }
      }

      retDF
    }

    /**
      * Get dataframes using the given dataframe for schema/data version and partitions
      *
      * input DataFrame is expected to be returned from the getDataAvailability call
      *
      * returned dataframe created will be a unioned schema of all the DataFrames created
      *
      * Expected Columns:
      * FormatVersion: schema version
      * DataVersion: version of the data
      * [partitionName]: other columns are the partitions for the object
      *
      * @param availableData Dataframe from getDataAvailability call, which data to use
      * @return Spark DataFrame of data
      *
      */
    def getDataFrame(availableData: DataFrame): DataFrame = {

      val formatVersionCol = "FormatVersion"
      val dataVersionCol = "DataVersion"
      val reasonCol = "Reason"
      val namespaceCol = "Namespace"
      val objectNameCol = "ObjectName"
      val usageCol = "Usage"
      val fileFormatCol = "FileFormat"

      val baseCols = formatVersionCol ::
        dataVersionCol ::
        reasonCol ::
        namespaceCol ::
        objectNameCol ::
        usageCol ::
        fileFormatCol ::
        Nil

      // check that schema is as expected
      if (baseCols.intersect(availableData.schema.fieldNames).length != baseCols.length) {
        logger.error(s"Unexpected schema, does not contain $baseCols")
        return null
      }

      // other cols are the partition names
      val partitionCols = availableData.schema.fieldNames.diff(baseCols)

      // the dataframe to create and return
      var retDF: DataFrame = null

      for (aRow <- availableData.filter(col(reasonCol) === "VALID").collect()) {
        // construct the partitions collection
        val parts = partitionCols.map { aPart =>
          Partition(aPart, aRow.get(aRow.fieldIndex(aPart)).toString)
        }.toList

        // get the schema and data versions
        val schemaVersion = aRow.getAs[Int](formatVersionCol)
        val dataVersion = aRow.getAs[Int](dataVersionCol)
        val namespace = aRow.getAs[String](namespaceCol)
        val objectName = aRow.getAs[String](objectNameCol)
        val usage = aRow.getAs[String](usageCol)
        val fileFormat = aRow.getAs[String](fileFormatCol)

        // create the dataframe
        val aDF = getDataFrame(namespace, objectName, usage, fileFormat, parts, schemaVersion, dataVersion)
        if (retDF == null) {
          retDF = aDF
        } else {
          // union the dataframes, using the union of their schemas
          retDF = unionUnionSchema(retDF, aDF)
        }
      }

      retDF
    }

    /**
      * Creates a dataframe for the given list of key partition values.
      * This function only works for objects where there is only the key partition, does not work for cases of sub-partitions
      *
      * @param namespace     DM namespace
      * @param objectName    DM object name
      * @param usage         usage of data in DM
      * @param fileFormat    file format in DM
      * @param keyPartValues list of key partition values
      * @return
      */
    def getDataFrame(namespace: String, objectName: String, usage: String, fileFormat: String, keyPartValues: List[String],
                     formatVersion: Integer): DataFrame = {
      val pp = getPartitions(namespace, objectName, usage, fileFormat, formatVersion)

      if (pp.length > 1) {
        logger.error("object has multiple partitions, this works only for objects with one (key) partition")
        return null
      }

      val reasonCol = "Reason"

      val da = getDataAvailability(namespace, objectName, usage, fileFormat, formatVersion)
        .filter(col(pp.head.name).isin(keyPartValues: _*))
        .filter(col(reasonCol) === "VALID")

      // @todo ensure there is only one row for each key partition value given
      if (da.count() != keyPartValues.length) {
        logger.error("could not find all partitions for list given, or too many returned")
        return null
      }

      getDataFrame(da)
    }

    /**
      * Searches for the given table the list of given namespaces
      *
      * @param objectName the business object name
      * @param namespaces the namespace
      * @return namespace
      */
    private[catalog] def findNamespace(objectName: String, namespaces: List[String]): String = {

      var namespace: String = null

      // search for the table in the right namespace
      for (aNamespace <- namespaces; if namespace == null) {
        val hubObjectDefs = getBusinessObjectDefinitions(aNamespace)

        // was a row returned?
        val cnt = hubObjectDefs.filter('definitionName === objectName).count

        // got our namespace!
        if (cnt == 1) {
          namespace = aNamespace
        }
      }

      namespace
    }

    /**
      * Will search for the given objectName (table) preferring the given namespaces (in order) and the preferred file types
      *
      * The returned data frame will be created for the given key partition values.
      * if any sub-partitions exist, will create the DataFrame for all
      *
      * @param objectName    table to search for in DM
      * @param keyPartValues the key partition values to create DataFrame for
      * @param namespaces    ordered list of namespaces to search
      * @param fileFormats   ordered list of preferred file formats
      */
    private[catalog] def findDataFrame(
                               objectName: String,
                               keyPartValues: List[String],
                               namespaces: List[String],
                               fileFormats: List[String]): DataFrame = {

      val namespace: String = findNamespace(objectName, namespaces)
      var fileFormat: String = null
      var usage: String = null
      var formatVersion: Integer = null

      // check that a namespace was found for the table
      require(namespace != null, "Cannot find table in namespaces: " + namespaces.mkString(", "))

      // get the business object formats
      val formats = getBusinessObjectFormats(namespace, objectName)

      // search for the preferred file format
      for (aFileFormat <- fileFormats; if fileFormat == null && formatVersion < 0) {
        val maxVersions = formats
          .filter('formatFileType === aFileFormat)
          .select(max('formatVersion))

        if (maxVersions.count == 1) {
          formatVersion = maxVersions.first.getInt(0)
          val aRow = formats
            .filter('formatFileType === aFileFormat && 'formatVersion === formatVersion)
            .first

          usage = aRow.getAs[String]("formatUsage")
          fileFormat = aRow.getAs[String]("formatFileType")
        }
      }

      // check
      require(fileFormat != null, "Cannot find format given: " + fileFormats.mkString(", "))

      // get the partitions
      val parts = getPartitions(namespace, objectName, usage, fileFormat, formatVersion)
      val keyPart = parts.head.name

      // get available data, only valid data!
      val availData = getDataAvailability(namespace, objectName, usage, fileFormat, formatVersion).filter('Reason === "VALID")

      // filter for most recent versions for the key partition values given
      val dfData = availData
        .filter(availData(keyPart).isin(keyPartValues: _*))
        .groupBy('Namespace, 'ObjectName, 'Usage, 'FileFormat, 'Reason, availData(keyPart))
        .agg(
          max('FormatVersion).alias("FormatVersion"),
          max('DataVersion).alias("DataVersion")
        )

      getDataFrame(dfData)
    }

    /** A helper function which is used here to get storageDirectory
      *
      * @todo it is a copy of a private[catalog] function from Herd data source
      * @todo make it public there to use here
      * @param storageUnit DM storage unit
      * @return storageDirectory or file paths
      */
    private[catalog] def getFilePaths(storageUnit: StorageUnit): Seq[String] = {
      val paths = Option(storageUnit.getStorageFiles).map(i => i.asScala.map(_.getFilePath))
        .orElse(Some(Seq(storageUnit.getStorageDirectory.getDirectoryPath)))
        .getOrElse(sys.error("No storage paths could be found!"))

      paths
    }

    // some defaults for non-DataFrame object registration


    /** Pre-Registers a non-DataFrame object.  The method only pre-registers the object and retunrs storageDirectory.
      * It is user's responsibility and choice whether to write anything to this directory.
      * If written, it can be in any format, no validation is to be performed by the DM.
      * If the specified object is not registered, the function will register it and its format.
      * If the object is already registered a new data version will be created.
      *
      * @param nameSpace      Namespace
      * @param objectName     Business object name
      * @param formatVersion  An existing format version.  Specify null to get the latest.
      * @param partitionKey   A partition key
      * @param partitionValue A partition value
      * @return a tuple (formatVersion, dataVersion, path), where the path is location of data to be stored
      */
    def preRegisterBusinessObjectPath(nameSpace: String,
                                      objectName: String,
                                      formatVersion: Integer = null,
                                      partitionKey: String = "partition",
                                      partitionValue: String = "none"): (Integer, Integer, String) = {

      Try(herdApiWrapper.getHerdApi.getBusinessObjectByName(nameSpace, objectName)) match {
        case Success(_) => Unit
        case Failure(ex: ApiException) if ex.getCode == 404 =>
          logger.info(s"Business object not found, registering it")
          herdApiWrapper.getHerdApi.registerBusinessObject(nameSpace, objectName, "FINRA") // @todo: Should the Data Provider be a parameter?
          herdApiWrapper.getHerdApi.registerBusinessObjectFormat(nameSpace, objectName, usage, fileFormat, partitionKey, None)
        case Failure(ex) => throw ex
      }

      val formatVersionToUse: Integer = formatVersion match {
        case v if v >= 0 =>
          // check if exists and then use
          Try(herdApiWrapper.getHerdApi.getBusinessObjectFormat(nameSpace, objectName, usage, fileFormat, v)) match {
            case Success(_) => v
            case Failure(ex) => throw ex
          }
        case _ =>
          // use the latest version
          herdApiWrapper.getHerdApi.getBusinessObjectFormats(nameSpace, objectName)
            .getBusinessObjectFormatKeys
            .asScala.head
            .getBusinessObjectFormatVersion
            .toInt
      }

      // Pre-register and get the path
      val (dataVersion, storageUnits) = herdApiWrapper.getHerdApi.registerBusinessObjectData(
        nameSpace, objectName, usage,
        fileFormat, formatVersionToUse,
        partitionKey, partitionValue, Nil,
        ObjectStatus.UPLOADING, storageUnit, None)

      val path = storageUnits.flatMap(getFilePaths).head

      (formatVersionToUse, dataVersion, path)
    }

    /**
      * Completes registration of previously preregistered object.
      *
      * The pre-registered object has "UPLOADING" status which must be changed to either
      * VALID or INVALID.  This function does just this.
      *
      * @param nameSpace      Namespace
      * @param objectName     Business object name
      * @param formatVersion  Format version
      * @param partitionKey   Partition key
      * @param partitionValue Partition value
      * @param dataVersion    Data version
      * @param status         Status
      */
    def completeRegisterBusinessObjectPath(nameSpace: String,
                                           objectName: String,
                                           formatVersion: Integer = null,
                                           partitionKey: String = "partition",
                                           partitionValue: String = "none",
                                           dataVersion: Integer = null,
                                           status: ObjectStatus.Value = ObjectStatus.VALID): Unit = {

      herdApiWrapper.getHerdApi.updateBusinessObjectData(nameSpace, objectName, usage, fileFormat, formatVersion,
        partitionKey, partitionValue, Nil, dataVersion, status)
    }

    /** Retrieve a non-DataFrame object registered with DM.
      * In fact, the function only retrieves a path to the stored object.
      * It is user's responsibility and choice whether and how to read the object.
      * The object can be represented by any directories and files structure
      *
      * @param nameSpace      Namespace
      * @param objectName     Business object name
      * @param formatVersion  Format version.  Use null to get the latest format
      * @param partitionKey   Partition key
      * @param partitionValue Partition value
      * @param dataVersion    Data version.  Use null to get the latest data version
      * @return Path to the object
      */
    def getBusinessObjectPath(nameSpace: String,
                              objectName: String,
                              formatVersion: Integer = null,
                              partitionKey: String = "partition",
                              partitionValue: String = "none",
                              dataVersion: Integer = null): String = {

      val dataVersionToUse: Integer = dataVersion

      val formatVersionToUse: Integer = formatVersion match {
        case v if v >= 0 =>
          // check if exists and then use
          Try(herdApiWrapper.getHerdApi.getBusinessObjectFormat(nameSpace, objectName, usage, fileFormat, v)) match {
            case Success(_) => v
            case Failure(ex) => throw ex
          }
        case _ =>
          // use the latest version
          herdApiWrapper.getHerdApi.getBusinessObjectFormats(nameSpace, objectName)
            .getBusinessObjectFormatKeys
            .asScala.head
            .getBusinessObjectFormatVersion
            .toInt
      }

      val api = new BusinessObjectDataApi(herdApiWrapper.getApiClient)

      // @todo Change the corresponding Herd data source function to pass null: Integer
      // @todo Then use Herd data source's function instead of Herd SDK here
      val retrivedObject = api.businessObjectDataGetBusinessObjectData(
        nameSpace,
        objectName,
        usage,
        fileFormat,
        partitionKey,
        partitionValue,
        null,
        formatVersionToUse,
        dataVersionToUse,
        ObjectStatus.VALID.toString,
        false,
        false)

      retrivedObject.getStorageUnits.asScala.flatMap(getFilePaths).head
    }

    /** Register a new formatVersion
      *
      * @param nameSpace      Namespace
      * @param objectName     Business object name
      * @param partitionKey   Partition key
      * @param partitionValue Partition value
      * @return A new format version
      */
    def registerNewFormat(nameSpace: String,
                          objectName: String,
                          partitionKey: String = "partition",
                          partitionValue: String = "none"): Int = {

      Try(herdApiWrapper.getHerdApi.getBusinessObjectByName(nameSpace, objectName)) match {
        case Success(_) => Unit
        case Failure(ex) => throw ex
      }

      herdApiWrapper.getHerdApi.registerBusinessObjectFormat(nameSpace, objectName, usage, fileFormat, partitionKey, None)
    }

    /** retrieves a previously saved DataFrame
      *
      * @param namespace  logical name of the namespace
      * @param objName    businessObjectName
      * @param usage      DM supported format usage (e.g., "PRC")
      * @param fileFormat DM supported file format (e.g., "BZ", "PARQUET", etc)
      * @return a DataFrame for the object (lazy)
      */
    def loadDataFrame(namespace: String,
                      objName: String,
                      usage: String = "PRC",
                      fileFormat: String = "PARQUET",
                      storagePathPrefix: String = "s3a"): DataFrame = {

      spark.read.format("herd")
        .option("url", baseRestUrl)
        .option("username", username)
        .option("password", password)
        .option("namespace", namespace)
        .option("businessObjectName", objName)
        .option("businessObjectFormatUsage", usage)
        .option("businessObjectFormatFileType", fileFormat)
        .option("storagePathPrefix", storagePathPrefix)
        .load()

    }

    /** saves a DataFrame and register with DM
      *
      * @param df                a DataFrame
      * @param namespace         a logical name of the namespace
      * @param objName           businessObjectName
      * @param partitionKey      name of the partition key
      * @param partitionValue    value of the partition key
      * @param partitionKeyGroup partition key group
      * @param usage             DM supported format usage (e.g., "PRC")
      * @param fileFormat        DM supported file format (e.g., "BZ", "PARQUET", etc)
      */
    def saveDataFrame(df: DataFrame,
                      namespace: String,
                      objName: String,
                      partitionKey: String = "partition",
                      partitionValue: String = "none",
                      partitionKeyGroup: String = "TRADE_DT",
                      usage: String = "PRC",
                      fileFormat: String = "PARQUET",
                      delimiter: String = null,
                      escapeChar: String = null,
                      nullValue: String = null): Unit = {

      var baseWriteOptions = df.write.format("herd")
        .option("url", baseRestUrl)
        .option("username", username)
        .option("password", password)
        .option("namespace", namespace)
        .option("businessObjectName", objName)
        .option("partitionKey", partitionKey)
        .option("partitionKeyGroup", partitionKeyGroup)
        .option("partitionValue", partitionValue)
        .option("businessObjectFormatUsage", usage)
        .option("businessObjectFormatFileType", fileFormat)
        .option("registerNewFormat", "true")


      val options = Map("delimiter" -> Option(delimiter),
        "escape" -> Option(escapeChar),
        "nullValue" -> Option(nullValue)
      ).filter(opt => opt._2.nonEmpty)

      // add optional parameters if specified by user
      baseWriteOptions = options.foldLeft(baseWriteOptions)((df, opts) => df.option(opts._1, opts._2.get))

      // write out the data
      baseWriteOptions.save()
    }
  }




