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
package org.finra.herd.service.helper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.HivePartitionDto;
import org.finra.herd.model.dto.StorageUnitAvailabilityDto;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;

/**
 * The DDL generator for Hive 13.
 */
@Component
@SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "We will use the standard carriage return character.")
public class Hive13DdlGenerator extends DdlGenerator
{
    /**
     * The partition key value for business object data without partitioning.
     */
    public static final String NO_PARTITIONING_PARTITION_KEY = "partition";

    /**
     * The partition value for business object data without partitioning.
     */
    public static final String NO_PARTITIONING_PARTITION_VALUE = "none";

    /**
     * Hive file format for ORC files.
     */
    public static final String ORC_HIVE_FILE_FORMAT = "ORC";

    /**
     * Hive file format for PARQUET files.
     */
    public static final String PARQUET_HIVE_FILE_FORMAT = "PARQUET";

    /**
     * Hive file format for text files.
     */
    public static final String TEXT_HIVE_FILE_FORMAT = "TEXTFILE";

    /**
     * Hive file format for JSON files.
     */
    public static final String JSON_HIVE_FILE_FORMAT = "JSONFILE";

    /**
     * The regular expression that represents an empty partition in S3, this is because hadoop file system implements directory support in S3 by creating empty
     * files with the "directoryname_$folder$" suffix.
     */
    public static final String REGEX_S3_EMPTY_PARTITION = "_\\$folder\\$";

    /**
     * Hive complex data types list.
     */
    private static final List<String> HIVE_COMPLEX_DATA_TYPES =
        Arrays.asList(Category.LIST.toString(), Category.MAP.toString(), Category.UNION.toString(), Category.STRUCT.toString());

    @Autowired
    private BusinessObjectDataDdlPartitionsHelper businessObjectDataDdlPartitionsHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    /**
     * Escapes single quote characters, if not already escaped, with an extra backslash.
     *
     * @param string the input text
     *
     * @return the output text with all single quote characters escaped by an extra backslash
     */
    public String escapeSingleQuotes(String string)
    {
        Pattern pattern = Pattern.compile("(?<!\\\\)(')");
        Matcher matcher = pattern.matcher(string);
        StringBuffer stringBuffer = new StringBuffer();

        while (matcher.find())
        {
            matcher.appendReplacement(stringBuffer, matcher.group(1).replace("'", "\\\\'"));
        }
        matcher.appendTail(stringBuffer);

        return stringBuffer.toString();
    }

    /**
     * Generates the create table Hive 13 DDL as per specified business object data DDL request.
     *
     * @param request the business object data DDL request
     * @param businessObjectFormatEntity the business object format entity
     * @param customDdlEntity the optional custom DDL entity
     * @param storageNames the list of storage names
     * @param requestedStorageEntities the list of storage entities per storage names specified in the request
     * @param cachedStorageEntities the map of storage names in upper case to the relative storage entities
     * @param cachedS3BucketNames the map of storage names in upper case to the relative S3 bucket names
     *
     * @return the create table Hive DDL
     */
    @Override
    public String generateCreateTableDdl(BusinessObjectDataDdlRequest request, BusinessObjectFormatEntity businessObjectFormatEntity,
        CustomDdlEntity customDdlEntity, List<String> storageNames, List<StorageEntity> requestedStorageEntities,
        Map<String, StorageEntity> cachedStorageEntities, Map<String, String> cachedS3BucketNames)
    {
        BusinessObjectDataDdlPartitionsHelper.GenerateDdlRequestWrapper generateDdlRequestWrapper = businessObjectDataDdlPartitionsHelper
            .buildGenerateDdlPartitionsWrapper(request, businessObjectFormatEntity, customDdlEntity, storageNames, requestedStorageEntities,
                cachedStorageEntities, cachedS3BucketNames);
        return generateCreateTableDdlHelper(generateDdlRequestWrapper);
    }

    /**
     * Generates the create table Hive 13 DDL as per specified business object format DDL request.
     *
     * @param request the business object format DDL request
     * @param businessObjectFormatEntity the business object format entity
     * @param customDdlEntity the optional custom DDL entity
     *
     * @return the create table Hive DDL
     */
    @Override
    public String generateCreateTableDdl(BusinessObjectFormatDdlRequest request, BusinessObjectFormatEntity businessObjectFormatEntity,
        CustomDdlEntity customDdlEntity)
    {
        // If the partitionKey="partition", then DDL should return a DDL which treats business object data as a table, not a partition.
        Boolean isPartitioned = !businessObjectFormatEntity.getPartitionKey().equalsIgnoreCase(NO_PARTITIONING_PARTITION_KEY);

        // Generate the create table Hive 13 DDL.
        BusinessObjectDataDdlPartitionsHelper.GenerateDdlRequestWrapper generateDdlRequestWrapper =
            businessObjectDataDdlPartitionsHelper.getGenerateDdlRequestWrapperInstance();
        generateDdlRequestWrapper.setBusinessObjectFormatEntity(businessObjectFormatEntity);
        generateDdlRequestWrapper.setCustomDdlEntity(customDdlEntity);
        generateDdlRequestWrapper.setPartitioned(isPartitioned);
        generateDdlRequestWrapper.setTableName(request.getTableName());
        generateDdlRequestWrapper.setIncludeDropTableStatement(request.isIncludeDropTableStatement());
        generateDdlRequestWrapper.setIncludeIfNotExistsOption(request.isIncludeIfNotExistsOption());
        generateDdlRequestWrapper.setGeneratePartitionsRequest(false);
        return generateCreateTableDdlHelper(generateDdlRequestWrapper);
    }

    @Override
    public String generateReplaceColumnsStatement(BusinessObjectFormatDdlRequest request, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        BusinessObjectFormat businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

        assertSchemaColumnsNotEmpty(businessObjectFormat, businessObjectFormatEntity);

        StringBuilder builder = new StringBuilder(34);
        builder.append("ALTER TABLE `");
        builder.append(request.getTableName());
        builder.append("` REPLACE COLUMNS (\n");
        builder.append(generateDdlColumns(businessObjectFormatEntity, businessObjectFormat));
        return builder.toString().trim() + ';';
    }

    /**
     * Gets the DDL character value based on the specified configured character value. This method supports UTF-8 encoded strings and will "Hive" escape any
     * non-ASCII printable characters using '\(value)'.
     *
     * @param string the configured character value.
     *
     * @return the DDL character value.
     */
    public String getDdlCharacterValue(String string)
    {
        return getDdlCharacterValue(string, false);
    }

    /**
     * Gets the DDL character value based on the specified configured character value. This method supports UTF-8 encoded strings and will "Hive" escape any
     * non-ASCII printable characters using '\(value)'.
     *
     * @param string the configured character value.
     * @param escapeSingleBackslash specifies if we need to escape a single backslash character with an extra backslash
     *
     * @return the DDL character value.
     */
    public String getDdlCharacterValue(String string, boolean escapeSingleBackslash)
    {
        // Assume the empty string for the return value.
        StringBuilder returnValueStringBuilder = new StringBuilder();

        // If we have an actual character, set the return value based on our rules.
        if (StringUtils.isNotEmpty(string))
        {
            // Convert the string to UTF-8 so we can the proper characters that were sent via XML.
            String utf8String = new String(string.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);

            // Loop through each character and add each one to the return value.
            for (int i = 0; i < utf8String.length(); i++)
            {
                // Default to the character itself.
                Character character = string.charAt(i);
                String nextValue = character.toString();

                // If the character isn't ASCII printable, then "Hive" escape it.
                if (!CharUtils.isAsciiPrintable(character))
                {
                    // If the character is unprintable, then display it as the ASCII octal value in \000 format.
                    nextValue = String.format("\\%03o", (int) character);
                }

                // Add this character to the return value.
                returnValueStringBuilder.append(nextValue);
            }

            // Check if we need to escape a single backslash character with an extra backslash.
            if (escapeSingleBackslash && returnValueStringBuilder.toString().equals("\\"))
            {
                returnValueStringBuilder.append('\\');
            }
        }

        // Return the value.
        return returnValueStringBuilder.toString();
    }

    @Override
    public BusinessObjectDataDdlOutputFormatEnum getDdlOutputFormat()
    {
        return BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL;
    }

    /**
     * Gets a list of Hive partitions. For single level partitioning, no auto-discovery of sub-partitions (sub-directories) is needed - the business object data
     * will be represented by a single Hive partition instance. For multiple level partitioning, this method performs an auto-discovery of all sub-partitions
     * (sub-directories) and creates a Hive partition object instance for each partition.
     *
     * @param businessObjectDataKey the business object data key
     * @param autoDiscoverableSubPartitionColumns the auto-discoverable sub-partition columns
     * @param s3KeyPrefix the S3 key prefix
     * @param storageFiles the storage files
     * @param storageName the storage name
     *
     * @return the list of Hive partitions
     */
    public List<HivePartitionDto> getHivePartitions(BusinessObjectDataKey businessObjectDataKey, List<SchemaColumn> autoDiscoverableSubPartitionColumns,
        String s3KeyPrefix, Collection<String> storageFiles, String storageName)
    {
        // We are using linked hash map to preserve the order of the discovered partitions.
        LinkedHashMap<List<String>, HivePartitionDto> linkedHashMap = new LinkedHashMap<>();

        Pattern pattern = getHivePathPattern(autoDiscoverableSubPartitionColumns);
        for (String storageFile : storageFiles)
        {
            // Remove S3 key prefix from the file path. Please note that the storage files are already validated to start with S3 key prefix.
            String relativeFilePath = storageFile.substring(s3KeyPrefix.length());

            // Try to match the relative file path to the expected subpartition folders.
            Matcher matcher = pattern.matcher(relativeFilePath);
            Assert.isTrue(matcher.matches(), String.format("Registered storage file or directory does not match the expected Hive sub-directory pattern. " +
                    "Storage: {%s}, file/directory: {%s}, business object data: {%s}, S3 key prefix: {%s}, pattern: {%s}", storageName, storageFile,
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), s3KeyPrefix, pattern.pattern()));

            // Create a list of partition values.
            List<String> partitionValues = new ArrayList<>();

            // Add partition values per business object data key.
            partitionValues.add(businessObjectDataKey.getPartitionValue());
            partitionValues.addAll(businessObjectDataKey.getSubPartitionValues());

            // Extract relative partition values.
            for (int i = 1; i <= matcher.groupCount() - 1; i++)
            {
                partitionValues.add(matcher.group(i));
            }

            // Get the matched trailing folder markers and an optional file name.
            String partitionPathTrailingPart = matcher.group(matcher.groupCount());

            // If we did not match all expected partition values along with the trailing folder markers and an optional file name, then this storage file
            // path is not part of a fully qualified partition (this is an intermediate folder marker) and we ignore it.
            if (!partitionValues.contains(null))
            {
                // Get path for this partition by removing trailing "/" followed by an optional file name or "_$folder$" which represents an empty folder in S3.
                String partitionPath = relativeFilePath.substring(0, relativeFilePath.length() - StringUtils.length(partitionPathTrailingPart));

                // Check if we already have that partition discovered - that would happen if partition contains multiple data files.
                HivePartitionDto hivePartition = linkedHashMap.get(partitionValues);

                if (hivePartition != null)
                {
                    // Partition is already discovered, so just validate that the relative paths match.
                    Assert.isTrue(hivePartition.getPath().equals(partitionPath), String.format(
                        "Found two different locations for the same Hive partition. Storage: {%s}, business object data: {%s}, " +
                            "S3 key prefix: {%s}, path[1]: {%s}, path[2]: {%s}", storageName,
                        businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), s3KeyPrefix, hivePartition.getPath(), partitionPath));
                }
                else
                {
                    // Add this partition to the hash map of discovered partitions.
                    linkedHashMap.put(partitionValues, new HivePartitionDto(partitionPath, partitionValues));
                }
            }
        }

        List<HivePartitionDto> hivePartitions = new ArrayList<>();
        hivePartitions.addAll(linkedHashMap.values());

        return hivePartitions;
    }

    /**
     * Gets a pattern to match Hive partition sub-directories.
     *
     * @param partitionColumns the list of partition columns
     *
     * @return the newly created pattern to match Hive partition sub-directories.
     */
    public Pattern getHivePathPattern(List<SchemaColumn> partitionColumns)
    {
        return Pattern.compile(getHivePathRegex(partitionColumns));
    }

    /**
     * Gets a regex to match Hive partition sub-directories.
     *
     * @param partitionColumns the list of partition columns
     *
     * @return the newly created regex to match Hive partition sub-directories.
     */
    public String getHivePathRegex(List<SchemaColumn> partitionColumns)
    {
        StringBuilder sb = new StringBuilder(26);

        sb.append("^(?:");      // Start a non-capturing group for the entire regex.

        // For each partition column, add a regular expression to match "<COLUMN_NAME|COLUMN-NAME>=<VALUE>" sub-directory.
        for (SchemaColumn partitionColumn : partitionColumns)
        {
            sb.append("(?:");   // Start a non-capturing group for the remainder of the regex.
            sb.append("(?:");   // Start a non-capturing group for folder markers.
            sb.append("\\/");   // Add a trailing "/".
            sb.append('|');     // Ann an OR.
            sb.append(REGEX_S3_EMPTY_PARTITION);    // Add a trailing "_$folder$", which represents an empty partition in S3.
            sb.append(')');     // Close the non-capturing group for folder markers.
            sb.append('|');     // Add an OR.
            sb.append("(?:");   // Start a non-capturing group for "/<column name>=<column value>".
            sb.append("\\/");   // Add a "/".
            // We are using a non-capturing group for the partition column names here - this is done by adding "?:" to the beginning of a capture group.
            sb.append("(?:");   // Start a non-capturing group for column name.
            sb.append("(?i)");  // Match partition column names case insensitive.
            sb.append(Matcher.quoteReplacement(partitionColumn.getName()));
            sb.append('|');     // Add an OR.
            // For sub-partition folder, we do support partition column names having all underscores replaced with hyphens.
            sb.append(Matcher.quoteReplacement(partitionColumn.getName().replace("_", "-")));
            sb.append(')');     // Close the non-capturing group for column name.
            sb.append("=([^/]+)"); // Add a capturing group for a column value.
        }

        // Add additional regular expression for the trailing empty folder marker and/or "/" followed by an optional file name.
        sb.append("(");     // Start a capturing group for folder markers and an optional file name.
        sb.append("\\/");   // Add a trailing "/".
        sb.append("[^/]*"); // Add an optional file name.
        sb.append('|');     // Add an OR.
        sb.append(REGEX_S3_EMPTY_PARTITION);    // Add a trailing "_$folder$", which represents an empty partition in S3.
        sb.append(")");     // Close the capturing group for folder markers and an optional file name.

        // Close all non-capturing groups that are still open.
        for (int i = 0; i < 2 * partitionColumns.size(); i++)
        {
            sb.append(')');
        }

        sb.append(')');     // Close the non-capturing group for the entire regex.
        sb.append('$');

        return sb.toString();
    }

    /**
     * Asserts that there exists at least one column specified in the business object format schema.
     *
     * @param businessObjectFormat The {@link BusinessObjectFormat} containing schema columns.
     * @param businessObjectFormatEntity The entity used to generate the error message.
     */
    private void assertSchemaColumnsNotEmpty(BusinessObjectFormat businessObjectFormat, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        Assert.notEmpty(businessObjectFormat.getSchema().getColumns(), String.format("No schema columns specified for business object format {%s}.",
            businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
    }

    /**
     * Generates and append to the string builder the create table Hive 13 DDL as per specified parameters.
     */
    private String generateCreateTableDdlHelper(BusinessObjectDataDdlPartitionsHelper.GenerateDdlRequestWrapper generateDdlRequest)
    {
        // TODO: We might want to consider using a template engine such as Velocity to generate this DDL so we don't wind up just doing string manipulation.

        StringBuilder sb = new StringBuilder();

        // For custom DDL, we would need to substitute the custom DDL tokens with their relative values.
        HashMap<String, String> replacements = new HashMap<>();

        BusinessObjectFormat businessObjectFormat = businessObjectDataDdlPartitionsHelper.validatePartitionFiltersAndFormat(generateDdlRequest);

        // Add drop table if requested.
        if (BooleanUtils.isTrue(generateDdlRequest.getIncludeDropTableStatement()))
        {
            sb.append(String.format("DROP TABLE IF EXISTS `%s`;\n\n", generateDdlRequest.getTableName()));
        }

        // Depending on the flag, prepare "if not exists" option text or leave it an empty string.
        String ifNotExistsOption = BooleanUtils.isTrue(generateDdlRequest.getIncludeIfNotExistsOption()) ? "IF NOT EXISTS " : "";

        // Only generate the create table DDL statement, if custom DDL was not specified.
        if (generateDdlRequest.getCustomDdlEntity() == null)
        {
            generateStandardBaseDdl(generateDdlRequest, sb, businessObjectFormat, ifNotExistsOption);
        }
        else
        {
            // Use the custom DDL in place of the create table statement.
            sb.append(String.format("%s\n\n", generateDdlRequest.getCustomDdlEntity().getDdl()));

            // We need to substitute the relative custom DDL token with an actual table name.
            replacements.put(TABLE_NAME_CUSTOM_DDL_TOKEN, generateDdlRequest.getTableName());
        }

        // Add alter table statements only if the list of partition filters is not empty - this is applicable to generating DDL for business object data only.
        if (!CollectionUtils.isEmpty(generateDdlRequest.getPartitionFilters()))
        {
            processPartitionFiltersForGenerateDdl(generateDdlRequest, sb, replacements, businessObjectFormat, ifNotExistsOption);
        }
        // Add a location statement with a token if this is format dll that does not use custom ddl.
        else if (!generateDdlRequest.getPartitioned() && generateDdlRequest.getCustomDdlEntity() == null)
        {
            // Since custom DDL is not specified, there are no partition values, and this table is not partitioned, add a LOCATION clause with a token.
            sb.append(String.format("LOCATION '%s';", NON_PARTITIONED_TABLE_LOCATION_CUSTOM_DDL_TOKEN));
        }

        // Trim to remove unnecessary end-of-line characters, if any, from the end of the generated DDL.
        String resultDdl = sb.toString().trim();

        // For custom DDL, substitute the relative custom DDL tokens with their values.
        if (generateDdlRequest.getCustomDdlEntity() != null)
        {
            for (Map.Entry<String, String> entry : replacements.entrySet())
            {
                String token = entry.getKey();
                String value = entry.getValue();
                resultDdl = resultDdl.replaceAll(Pattern.quote(token), value);
            }
        }

        return resultDdl;
    }

    /**
     * Generates the DDL column definitions based on the given business object format. The generated column definitions look like:
     * <p/>
     * <pre>
     *     `COL_NAME1` VARCHAR(2) COMMENT 'some comment',
     *     `COL_NAME2` VARCHAR(2),
     *     `ORIG_COL_NAME3` DATE
     * )
     * </pre>
     * <p/>
     * Each column definition is indented using 4 spaces. If a column is also a partition, the text 'ORIG_' will be prefixed in the column name. Note the
     * closing parenthesis at the end of the statement.
     *
     * @param businessObjectFormatEntity The persistent entity of business object format
     * @param businessObjectFormat The {@link BusinessObjectFormat}
     *
     * @return String containing the generated column definitions.
     */
    private String generateDdlColumns(BusinessObjectFormatEntity businessObjectFormatEntity, BusinessObjectFormat businessObjectFormat)
    {
        StringBuilder sb = new StringBuilder();
        // Add schema columns.
        Boolean firstRow = true;
        for (SchemaColumn schemaColumn : businessObjectFormat.getSchema().getColumns())
        {
            if (!firstRow)
            {
                sb.append(",\n");
            }
            else
            {
                firstRow = false;
            }

            // Add a schema column declaration. Check if a schema column is also a partition column and prepend "ORGNL_" prefix if this is the case.
            sb.append(String.format("    `%s%s` %s%s", (!CollectionUtils.isEmpty(businessObjectFormat.getSchema().getPartitions()) &&
                    businessObjectFormat.getSchema().getPartitions().contains(schemaColumn) ? "ORGNL_" : ""), schemaColumn.getName(),
                getHiveDataType(schemaColumn, businessObjectFormatEntity),
                StringUtils.isNotBlank(schemaColumn.getDescription()) ? String.format(" COMMENT '%s'", escapeSingleQuotes(schemaColumn.getDescription())) :
                    ""));
        }
        sb.append(")\n");
        return sb.toString();
    }

    private void generateStandardBaseDdl(BusinessObjectDataDdlPartitionsHelper.GenerateDdlRequestWrapper generateDdlRequest, StringBuilder sb,
        BusinessObjectFormat businessObjectFormat, String ifNotExistsOption)
    {
        // Please note that we escape table name and all column names to avoid Hive reserved words in DDL statement generation.
        sb.append(String.format("CREATE EXTERNAL TABLE %s`%s` (\n", ifNotExistsOption, generateDdlRequest.getTableName()));

        // Add schema columns.
        sb.append(generateDdlColumns(generateDdlRequest.getBusinessObjectFormatEntity(), businessObjectFormat));

        if (generateDdlRequest.getPartitioned())
        {
            // Add a partitioned by clause.
            sb.append("PARTITIONED BY (");
            // List all partition columns.
            List<String> partitionColumnDeclarations = new ArrayList<>();
            for (SchemaColumn partitionColumn : businessObjectFormat.getSchema().getPartitions())
            {
                partitionColumnDeclarations.add(
                    String.format("`%s` %s", partitionColumn.getName(), getHiveDataType(partitionColumn, generateDdlRequest.getBusinessObjectFormatEntity())));
            }
            sb.append(StringUtils.join(partitionColumnDeclarations, ", "));
            sb.append(")\n");
        }

        if (StringUtils.isNotBlank(generateDdlRequest.getBusinessObjectFormatEntity().getCustomClusteredBy()))
        {
            // Add custom ClusteredBy defined in business object format schema.
            sb.append(String.format("CLUSTERED BY %s\n", generateDdlRequest.getBusinessObjectFormatEntity().getCustomClusteredBy()));
        }

        if (!StringUtils.isEmpty(generateDdlRequest.getBusinessObjectFormatEntity().getCustomRowFormat()))
        {
            // Add custom row format defined in business object format.
            // This will override everything after "ROW FORMAT" including delimiter, escape value, null value statements defined in the business object format
            // schema.
            sb.append(String.format("ROW FORMAT %s\n", generateDdlRequest.getBusinessObjectFormatEntity().getCustomRowFormat()));
        }
        else
        {
            // We output delimiter character, collection items delimiter, map keys delimiter, escape character, and null value only when they are defined
            // in the business object format schema.
            sb.append("ROW FORMAT DELIMITED");
            if (!StringUtils.isEmpty(generateDdlRequest.getBusinessObjectFormatEntity().getDelimiter()))
            {
                // Note that the escape character is only output when the delimiter is present.
                sb.append(String.format(" FIELDS TERMINATED BY '%s'%s",
                    escapeSingleQuotes(getDdlCharacterValue(generateDdlRequest.getBusinessObjectFormatEntity().getDelimiter(), true)),
                    StringUtils.isEmpty(generateDdlRequest.getBusinessObjectFormatEntity().getEscapeCharacter()) ? "" : String.format(" ESCAPED BY '%s'",
                        escapeSingleQuotes(getDdlCharacterValue(generateDdlRequest.getBusinessObjectFormatEntity().getEscapeCharacter(), true)))));
            }
            if (!StringUtils.isEmpty(generateDdlRequest.getBusinessObjectFormatEntity().getCollectionItemsDelimiter()))
            {
                sb.append(String.format(" COLLECTION ITEMS TERMINATED BY '%s'",
                    escapeSingleQuotes(getDdlCharacterValue(generateDdlRequest.getBusinessObjectFormatEntity().getCollectionItemsDelimiter(), true))));
            }
            if (!StringUtils.isEmpty(generateDdlRequest.getBusinessObjectFormatEntity().getMapKeysDelimiter()))
            {
                sb.append(String.format(" MAP KEYS TERMINATED BY '%s'",
                    escapeSingleQuotes(getDdlCharacterValue(generateDdlRequest.getBusinessObjectFormatEntity().getMapKeysDelimiter(), true))));
            }
            sb.append(String.format(" NULL DEFINED AS '%s'\n",
                escapeSingleQuotes(getDdlCharacterValue(generateDdlRequest.getBusinessObjectFormatEntity().getNullValue()))));
        }

        // If this table is not partitioned, then STORED AS clause will be followed by LOCATION. Otherwise, the CREATE TABLE is complete.
        sb.append(String.format("STORED AS %s%s\n", getHiveFileFormat(generateDdlRequest.getBusinessObjectFormatEntity()),
            generateDdlRequest.getPartitioned() ? ";\n" : ""));
    }

    /**
     * Returns the corresponding Hive data type per specified schema column entity.
     *
     * @param schemaColumn the schema column that we want to get the corresponding Hive data type for
     * @param businessObjectFormatEntity the business object format entity that schema column belongs to
     *
     * @return the Hive data type
     * @throws IllegalArgumentException if schema column data type is not supported
     */
    private String getHiveDataType(SchemaColumn schemaColumn, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        String hiveDataType;

        // Remove all the whitespaces and convert the schema column data type to lower case.
        // This sanitizedSchemaColumnDataType is only used for hive complex data types.
        String sanitizedSchemaColumnDataType = schemaColumn.getType().replaceAll("\\s", "").toLowerCase();
        try
        {
            if (schemaColumn.getType().equalsIgnoreCase("TINYINT") || schemaColumn.getType().equalsIgnoreCase("SMALLINT") ||
                schemaColumn.getType().equalsIgnoreCase("INT") || schemaColumn.getType().equalsIgnoreCase("BIGINT") ||
                schemaColumn.getType().equalsIgnoreCase("FLOAT") || schemaColumn.getType().equalsIgnoreCase("DOUBLE") ||
                schemaColumn.getType().equalsIgnoreCase("TIMESTAMP") || schemaColumn.getType().equalsIgnoreCase("DATE") ||
                schemaColumn.getType().equalsIgnoreCase("STRING") || schemaColumn.getType().equalsIgnoreCase("BOOLEAN") ||
                schemaColumn.getType().equalsIgnoreCase("BINARY"))
            {
                hiveDataType = schemaColumn.getType().toUpperCase();
            }
            else if (schemaColumn.getType().equalsIgnoreCase("DECIMAL") || schemaColumn.getType().equalsIgnoreCase("NUMBER"))
            {
                hiveDataType = StringUtils.isNotBlank(schemaColumn.getSize()) ? String.format("DECIMAL(%s)", schemaColumn.getSize()) : "DECIMAL";
            }
            else if (schemaColumn.getType().equalsIgnoreCase("VARCHAR") || schemaColumn.getType().equalsIgnoreCase("CHAR"))
            {
                hiveDataType = String.format("%s(%s)", schemaColumn.getType().toUpperCase(), schemaColumn.getSize());
            }
            else if (schemaColumn.getType().equalsIgnoreCase("VARCHAR2"))
            {
                hiveDataType = String.format("VARCHAR(%s)", schemaColumn.getSize());
            }
            else if (isHiveComplexDataType(sanitizedSchemaColumnDataType))
            {
                hiveDataType = String.format("%s", sanitizedSchemaColumnDataType);
            }
            else
            {
                // this exception is thrown when the isHiveComplexDataType() method returns false (e.g : INT(5))
                throw new IllegalArgumentException();
            }
        }
        catch (RuntimeException e)
        {
            throw new IllegalArgumentException(String
                .format("Column \"%s\" has an unsupported data type \"%s\" in the schema for business object format {%s}. Exception : \"%s\"",
                    schemaColumn.getName(), schemaColumn.getType(),
                    businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity), e.getMessage()));
        }
        return hiveDataType;
    }

    /**
     * Determines if the given input string is a hive complex data type or not.
     *
     * @param inputString the input string
     *
     * @return true if the inputString is a hive complex data type, false otherwise
     */
    private boolean isHiveComplexDataType(String inputString)
    {
        return HIVE_COMPLEX_DATA_TYPES.contains(TypeInfoUtils.getTypeInfoFromTypeString(inputString).getCategory().toString());
    }

    /**
     * Returns the corresponding Hive file format.
     *
     * @param businessObjectFormatEntity the business object format entity that schema column belongs to
     *
     * @return the Hive file format
     * @throws IllegalArgumentException if business object format file type is not supported
     */
    private String getHiveFileFormat(BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        String fileFormat = businessObjectFormatEntity.getFileType().getCode();
        String hiveFileFormat;

        if (fileFormat.equalsIgnoreCase(FileTypeEntity.BZ_FILE_TYPE) || fileFormat.equalsIgnoreCase(FileTypeEntity.GZ_FILE_TYPE) ||
            fileFormat.equalsIgnoreCase(FileTypeEntity.TXT_FILE_TYPE))
        {
            hiveFileFormat = TEXT_HIVE_FILE_FORMAT;
        }
        else if (fileFormat.equalsIgnoreCase(FileTypeEntity.PARQUET_FILE_TYPE))
        {
            hiveFileFormat = PARQUET_HIVE_FILE_FORMAT;
        }
        else if (fileFormat.equalsIgnoreCase(FileTypeEntity.ORC_FILE_TYPE))
        {
            hiveFileFormat = ORC_HIVE_FILE_FORMAT;
        }
        else if (fileFormat.equalsIgnoreCase(FileTypeEntity.JSON_FILE_TYPE))
        {
            hiveFileFormat = JSON_HIVE_FILE_FORMAT;
        }
        else
        {
            throw new IllegalArgumentException(String.format("Unsupported format file type for business object format {%s}.",
                businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
        }

        return hiveFileFormat;
    }

    /**
     * Processes partition filters for DDL generation as per generate DDL request.
     *
     * @param generateDdlRequest the generate DDL request
     * @param sb the string builder to be updated with the "alter table add partition" statements
     * @param replacements the hash map of string values to be used to substitute the custom DDL tokens with their actual values
     * @param businessObjectFormat the business object format
     * @param ifNotExistsOption specifies if generated DDL contains "if not exists" option
     */
    private void processPartitionFiltersForGenerateDdl(BusinessObjectDataDdlPartitionsHelper.GenerateDdlRequestWrapper generateDdlRequest, StringBuilder sb,
        HashMap<String, String> replacements, BusinessObjectFormat businessObjectFormat, String ifNotExistsOption)
    {
        List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos =
            businessObjectDataDdlPartitionsHelper.processPartitionFiltersForGenerateDdlPartitions(generateDdlRequest);

        // We still need to close/complete the create table statement when there is no custom DDL,
        // the table is non-partitioned, and there is no business object data found.
        if (generateDdlRequest.getCustomDdlEntity() == null && !generateDdlRequest.getPartitioned() && CollectionUtils.isEmpty(storageUnitAvailabilityDtos))
        {
            // Add a LOCATION clause with a token.
            sb.append(String.format("LOCATION '%s';", NON_PARTITIONED_TABLE_LOCATION_CUSTOM_DDL_TOKEN));
        }
        // The table is partitioned, custom DDL is specified, or there is at least one business object data instance found.
        else
        {
            // If drop partitions flag is set and the table is partitioned, drop partitions specified by the partition filters.
            if (generateDdlRequest.getPartitioned() && BooleanUtils.isTrue(generateDdlRequest.getIncludeDropPartitions()))
            {
                // Generate the beginning of the alter table statement.
                String alterTableFirstToken = String.format("ALTER TABLE `%s` DROP IF EXISTS", generateDdlRequest.getTableName());

                // Create a drop partition statement for each partition filter entry.
                List<String> dropPartitionStatements = new ArrayList<>();
                for (List<String> partitionFilter : generateDdlRequest.getPartitionFilters())
                {
                    // Start building a drop partition statement for this partition filter.
                    StringBuilder dropPartitionStatement = new StringBuilder();
                    dropPartitionStatement.append(String.format("%s PARTITION (",
                        BooleanUtils.isTrue(generateDdlRequest.getCombineMultiplePartitionsInSingleAlterTable()) ? "   " : alterTableFirstToken));

                    // Specify all partition column values as per this partition filter.
                    List<String> partitionKeyValuePairs = new ArrayList<>();
                    for (int i = 0; i < partitionFilter.size(); i++)
                    {
                        if (StringUtils.isNotBlank(partitionFilter.get(i)))
                        {
                            // We cannot hit ArrayIndexOutOfBoundsException on getPartitions() since partitionFilter would
                            // not have a value set at an index that is greater or equal than the number of partitions in the schema.
                            String partitionColumnName = businessObjectFormat.getSchema().getPartitions().get(i).getName();
                            partitionKeyValuePairs.add(String.format("`%s`='%s'", partitionColumnName, partitionFilter.get(i)));
                        }
                    }

                    // Complete the drop partition statement.
                    dropPartitionStatement.append(StringUtils.join(partitionKeyValuePairs, ", ")).append(')');

                    // Add this drop partition statement to the list.
                    dropPartitionStatements.add(dropPartitionStatement.toString());
                }

                // Add all drop partition statements to the main string builder.
                if (CollectionUtils.isNotEmpty(dropPartitionStatements))
                {
                    // If specified, combine dropping multiple partitions in a single ALTER TABLE statement.
                    if (BooleanUtils.isTrue(generateDdlRequest.getCombineMultiplePartitionsInSingleAlterTable()))
                    {
                        sb.append(alterTableFirstToken).append('\n');
                    }

                    sb.append(StringUtils.join(dropPartitionStatements,
                        BooleanUtils.isTrue(generateDdlRequest.getCombineMultiplePartitionsInSingleAlterTable()) ? ",\n" : ";\n")).append(";\n\n");
                }
            }

            // Process storage unit entities.
            if (!CollectionUtils.isEmpty(storageUnitAvailabilityDtos))
            {
                businessObjectDataDdlPartitionsHelper
                    .processStorageUnitsForGenerateDdlPartitions(generateDdlRequest, sb, new ArrayList(), replacements, businessObjectFormat, ifNotExistsOption,
                        storageUnitAvailabilityDtos);
            }
        }
    }
}
