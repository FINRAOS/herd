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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.BusinessObjectDefinitionDaoTestHelper;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.BusinessObjectFormatDaoTestHelper;
import org.finra.herd.dao.CustomDdlDaoTestHelper;
import org.finra.herd.dao.DataProviderDaoTestHelper;
import org.finra.herd.dao.FileTypeDaoTestHelper;
import org.finra.herd.dao.NamespaceDaoTestHelper;
import org.finra.herd.dao.PartitionKeyGroupDaoTestHelper;
import org.finra.herd.dao.SchemaColumnDaoTestHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;

@Component
public class BusinessObjectFormatServiceTestHelper
{
    @Autowired
    private BusinessObjectDefinitionDaoTestHelper businessObjectDefinitionDaoTestHelper;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatDaoTestHelper businessObjectFormatDaoTestHelper;

    @Autowired
    private BusinessObjectFormatService businessObjectFormatService;

    @Autowired
    private CustomDdlDaoTestHelper customDdlDaoTestHelper;

    @Autowired
    private CustomDdlServiceTestHelper customDdlServiceTestHelper;

    @Autowired
    private DataProviderDaoTestHelper dataProviderDaoTestHelper;

    @Autowired
    private FileTypeDaoTestHelper fileTypeDaoTestHelper;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    @Autowired
    private PartitionKeyGroupDaoTestHelper partitionKeyGroupDaoTestHelper;

    @Autowired
    private SchemaColumnDaoTestHelper schemaColumnDaoTestHelper;

    /**
     * Creates and persists {@link org.finra.herd.model.jpa.BusinessObjectFormatEntity} from the given request. Also creates and persists namespace, data
     * provider, bdef, and file type required for the format. If the request has sub-partitions, schema columns will be persisted. Otherwise, no schema will be
     * set for this format.
     *
     * @param request {@link org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest} format alt key
     *
     * @return created {@link org.finra.herd.model.jpa.BusinessObjectFormatEntity}
     */
    public BusinessObjectFormatEntity createBusinessObjectFormat(BusinessObjectDataInvalidateUnregisteredRequest request)
    {
        // Create namespace
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(request.getNamespace());

        // Create data provider with a name which is irrelevant for the test cases
        DataProviderEntity dataProviderEntity = dataProviderDaoTestHelper.createDataProviderEntity(AbstractServiceTest.DATA_PROVIDER_NAME);

        // Create business object definition
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity, request.getBusinessObjectDefinitionName(), dataProviderEntity,
                AbstractServiceTest.NO_BDEF_DESCRIPTION, AbstractServiceTest.NO_BDEF_DISPLAY_NAME, AbstractServiceTest.NO_ATTRIBUTES,
                AbstractServiceTest.NO_SAMPLE_DATA_FILES);

        // Create file type
        FileTypeEntity fileTypeEntity = fileTypeDaoTestHelper.createFileTypeEntity(request.getBusinessObjectFormatFileType());

        // Manually creating format since it is easier than providing large amounts of params to existing method
        // Create format
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setUsage(request.getBusinessObjectFormatUsage());
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());
        // If sub-partition values exist in the request
        if (!CollectionUtils.isEmpty(request.getSubPartitionValues()))
        {
            // Create schema columns
            List<SchemaColumnEntity> schemaColumnEntities = new ArrayList<>();
            for (int partitionLevel = 0; partitionLevel < request.getSubPartitionValues().size() + 1; partitionLevel++)
            {
                SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
                schemaColumnEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                schemaColumnEntity.setName(AbstractServiceTest.PARTITION_KEY + partitionLevel);
                schemaColumnEntity.setType("STRING");
                schemaColumnEntity.setPartitionLevel(partitionLevel);
                schemaColumnEntity.setPosition(partitionLevel);
                schemaColumnEntities.add(schemaColumnEntity);
            }
            businessObjectFormatEntity.setSchemaColumns(schemaColumnEntities);
            businessObjectFormatEntity.setPartitionKey(AbstractServiceTest.PARTITION_KEY + "0");
        }
        // If sub-partition values do not exist in the request
        else
        {
            businessObjectFormatEntity.setPartitionKey(AbstractServiceTest.PARTITION_KEY);
        }
        businessObjectFormatEntity.setLatestVersion(true);
        businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        return businessObjectFormatEntity;
    }

    /**
     * Creates a business object format create request.
     *
     * @param businessObjectDefinitionName the business object format definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param partitionKey the business object format partition key
     * @param description the business object format description
     * @param documentSchema the business object format document schema
     * @param attributes the list of attributes
     * @param attributeDefinitions the list of attribute definitions
     * @param schema the business object format schema
     *
     * @return the created business object format create request
     */
    public BusinessObjectFormatCreateRequest createBusinessObjectFormatCreateRequest(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, String partitionKey, String description, String documentSchema,
        List<Attribute> attributes, List<AttributeDefinition> attributeDefinitions, Schema schema)
    {
        BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest = new BusinessObjectFormatCreateRequest();

        businessObjectFormatCreateRequest.setNamespace(namespaceCode);
        businessObjectFormatCreateRequest.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectFormatCreateRequest.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectFormatCreateRequest.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectFormatCreateRequest.setPartitionKey(partitionKey);
        businessObjectFormatCreateRequest.setDescription(description);
        businessObjectFormatCreateRequest.setDocumentSchema(documentSchema);
        businessObjectFormatCreateRequest.setAttributes(attributes);
        businessObjectFormatCreateRequest.setAttributeDefinitions(attributeDefinitions);
        businessObjectFormatCreateRequest.setSchema(schema);

        return businessObjectFormatCreateRequest;
    }

    /**
     * Creates a business object format update request.
     *
     * @param description the business object format description
     * @param attributes the list of attributes
     * @param schema the business object format schema
     *
     * @return the created business object format create request
     */
    public BusinessObjectFormatUpdateRequest createBusinessObjectFormatUpdateRequest(String description, String documentSchema, List<Attribute> attributes,
        Schema schema)
    {
        BusinessObjectFormatUpdateRequest businessObjectFormatUpdateRequest = new BusinessObjectFormatUpdateRequest();

        businessObjectFormatUpdateRequest.setDescription(description);
        businessObjectFormatUpdateRequest.setDocumentSchema(documentSchema);
        businessObjectFormatUpdateRequest.setAttributes(attributes);
        businessObjectFormatUpdateRequest.setSchema(schema);

        return businessObjectFormatUpdateRequest;
    }

    /**
     * Creates and persists database entities required for generate business object format ddl collection testing.
     */
    public void createDatabaseEntitiesForBusinessObjectFormatDdlCollectionTesting()
    {
        // Build a list of schema columns.
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        schemaColumns.add(
            new SchemaColumn(AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, "DATE", AbstractServiceTest.NO_COLUMN_SIZE, AbstractServiceTest.COLUMN_REQUIRED,
                AbstractServiceTest.NO_COLUMN_DEFAULT_VALUE, AbstractServiceTest.NO_COLUMN_DESCRIPTION));
        schemaColumns.add(new SchemaColumn(AbstractServiceTest.COLUMN_NAME, "NUMBER", AbstractServiceTest.COLUMN_SIZE, AbstractServiceTest.NO_COLUMN_REQUIRED,
            AbstractServiceTest.COLUMN_DEFAULT_VALUE, AbstractServiceTest.COLUMN_DESCRIPTION));

        // Use the first column as a partition column.
        List<SchemaColumn> partitionColumns = schemaColumns.subList(0, 1);

        // Create a business object format entity with the schema.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.FORMAT_DESCRIPTION,
                AbstractServiceTest.FORMAT_DOCUMENT_SCHEMA, AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
                AbstractServiceTest.NO_PARTITION_KEY_GROUP, AbstractServiceTest.NO_ATTRIBUTES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    public void createDatabaseEntitiesForBusinessObjectFormatDdlTesting()
    {
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting(FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
            AbstractServiceTest.SCHEMA_DELIMITER_PIPE, AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N,
            schemaColumnDaoTestHelper.getTestSchemaColumns(), schemaColumnDaoTestHelper.getTestPartitionColumns(), AbstractServiceTest.CUSTOM_DDL_NAME);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    public void createDatabaseEntitiesForBusinessObjectFormatDdlTesting(String businessObjectFormatFileType, String partitionKey,
        String schemaDelimiterCharacter, String schemaEscapeCharacter, String schemaNullValue, List<SchemaColumn> schemaColumns,
        List<SchemaColumn> partitionColumns, String customDdlName)
    {
        // Create a business object format entity if it does not exist.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                businessObjectFormatFileType, AbstractServiceTest.FORMAT_VERSION));
        if (businessObjectFormatEntity == null)
        {
            businessObjectFormatEntity = businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                    businessObjectFormatFileType, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.FORMAT_DESCRIPTION,
                    AbstractServiceTest.FORMAT_DOCUMENT_SCHEMA, AbstractServiceTest.LATEST_VERSION_FLAG_SET, partitionKey,
                    AbstractServiceTest.NO_PARTITION_KEY_GROUP, AbstractServiceTest.NO_ATTRIBUTES, schemaDelimiterCharacter, schemaEscapeCharacter,
                    schemaNullValue, schemaColumns, partitionColumns);
        }

        if (StringUtils.isNotBlank(customDdlName))
        {
            boolean partitioned = (partitionColumns != null);
            customDdlDaoTestHelper.createCustomDdlEntity(businessObjectFormatEntity, customDdlName, customDdlServiceTestHelper.getTestCustomDdl(partitioned));
        }
    }

    /**
     * Creates business object format by calling the relative service method and using hard coded test values.
     *
     * @return the newly created business object format
     */
    public BusinessObjectFormat createTestBusinessObjectFormat()
    {
        return createTestBusinessObjectFormat(AbstractServiceTest.NO_ATTRIBUTES);
    }

    /**
     * Creates business object format by calling the relative service method and using hard coded test values.
     *
     * @param attributes the attributes
     *
     * @return the newly created business object format
     */
    public BusinessObjectFormat createTestBusinessObjectFormat(List<Attribute> attributes)
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.FORMAT_DESCRIPTION,
                AbstractServiceTest.FORMAT_DOCUMENT_SCHEMA, attributes, getTestAttributeDefinitions(), getTestSchema());

        return businessObjectFormatService.createBusinessObjectFormat(request);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    public void createTestDatabaseEntitiesForBusinessObjectFormatTesting()
    {
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(AbstractServiceTest.NAMESPACE, AbstractServiceTest.DATA_PROVIDER_NAME,
            AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.PARTITION_KEY_GROUP);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    public void createTestDatabaseEntitiesForBusinessObjectFormatTestingWithParent(List<BusinessObjectFormatKey> businessObjectParents)
    {
        createTestDatabaseEntitiesForBusinessObjectFormatTesting(AbstractServiceTest.NAMESPACE, AbstractServiceTest.DATA_PROVIDER_NAME,
            AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.PARTITION_KEY_GROUP);
    }

    /**
     * Creates relative database entities required for the unit tests.
     *
     * @param namespaceCode the namespace Code
     * @param dataProviderName the data provider name
     * @param businessObjectDefinitionName the business object format definition name
     * @param businessObjectFormatFileType the business object format file type
     * @param partitionKeyGroupName the partition key group name
     */
    public void createTestDatabaseEntitiesForBusinessObjectFormatTesting(String namespaceCode, String dataProviderName, String businessObjectDefinitionName,
        String businessObjectFormatFileType, String partitionKeyGroupName)
    {
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceCode, businessObjectDefinitionName, dataProviderName, AbstractServiceTest.BDEF_DESCRIPTION);
        fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType, AbstractServiceTest.FORMAT_FILE_TYPE_DESCRIPTION);
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(partitionKeyGroupName);
    }

    /**
     * Returns the actual HIVE DDL expected to be produced by a unit test.
     *
     * @return the actual HIVE DDL expected to be produced by a unit test
     */
    public String getExpectedBusinessObjectFormatDdl()
    {
        StringBuilder ddlBuilder = new StringBuilder();

        ddlBuilder.append("DROP TABLE IF EXISTS `" + AbstractServiceTest.TABLE_NAME + "`;\n");
        ddlBuilder.append("\n");
        ddlBuilder.append("CREATE EXTERNAL TABLE IF NOT EXISTS `" + AbstractServiceTest.TABLE_NAME + "` (\n");
        ddlBuilder.append("    `ORGNL_" + AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME + "` DATE,\n");
        ddlBuilder.append("    `" + AbstractServiceTest.COLUMN_NAME + "` DECIMAL(" + AbstractServiceTest.COLUMN_SIZE + ") COMMENT '" +
            AbstractServiceTest.COLUMN_DESCRIPTION + "')\n");
        ddlBuilder.append("PARTITIONED BY (`" + AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME + "` DATE)\n");
        ddlBuilder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS '\\N'\n");
        ddlBuilder.append("STORED AS TEXTFILE;");

        return ddlBuilder.toString();
    }

    /**
     * Returns Hive DDL that is expected to be produced by a unit test based on specified parameters and hard-coded test values.
     *
     * @param partitionLevels the number of partition levels
     * @param firstColumnName the name of the first schema column
     * @param firstColumnDataType the data type of the first schema column
     * @param hiveRowFormat the Hive row format
     * @param hiveFileFormat the Hive file format
     * @param businessObjectFormatFileType the business object format file type
     * @param isDropStatementIncluded specifies if expected DDL should include a drop table statement
     *
     * @return the Hive DDL
     */
    public String getExpectedBusinessObjectFormatDdl(int partitionLevels, String firstColumnName, String firstColumnDataType, String hiveRowFormat,
        String hiveFileFormat, String businessObjectFormatFileType, boolean isDropStatementIncluded, boolean isIfNotExistsOptionIncluded)
    {
        StringBuilder sb = new StringBuilder();

        if (isDropStatementIncluded)
        {
            sb.append("DROP TABLE IF EXISTS `[Table Name]`;\n\n");
        }
        sb.append("CREATE EXTERNAL TABLE [If Not Exists]`[Table Name]` (\n");
        sb.append(String.format("    `%s` %s,\n", firstColumnName, firstColumnDataType));
        sb.append("    `COLUMN002` SMALLINT COMMENT 'This is \\'COLUMN002\\' column. ");
        sb.append("Here are \\'single\\' and \"double\" quotes along with a backslash \\.',\n");
        sb.append("    `COLUMN003` INT,\n");
        sb.append("    `COLUMN004` BIGINT,\n");
        sb.append("    `COLUMN005` FLOAT,\n");
        sb.append("    `COLUMN006` DOUBLE,\n");
        sb.append("    `COLUMN007` DECIMAL,\n");
        sb.append("    `COLUMN008` DECIMAL(p,s),\n");
        sb.append("    `COLUMN009` DECIMAL,\n");
        sb.append("    `COLUMN010` DECIMAL(p),\n");
        sb.append("    `COLUMN011` DECIMAL(p,s),\n");
        sb.append("    `COLUMN012` TIMESTAMP,\n");
        sb.append("    `COLUMN013` DATE,\n");
        sb.append("    `COLUMN014` STRING,\n");
        sb.append("    `COLUMN015` VARCHAR(n),\n");
        sb.append("    `COLUMN016` VARCHAR(n),\n");
        sb.append("    `COLUMN017` CHAR(n),\n");
        sb.append("    `COLUMN018` BOOLEAN,\n");
        sb.append("    `COLUMN019` BINARY)\n");

        if (partitionLevels > 0)
        {
            if (partitionLevels > 1)
            {
                // Multiple level partitioning.
                sb.append("PARTITIONED BY (`PRTN_CLMN001` DATE, `PRTN_CLMN002` STRING, `PRTN_CLMN003` INT, `PRTN_CLMN004` DECIMAL, " +
                    "`PRTN_CLMN005` BOOLEAN, `PRTN_CLMN006` DECIMAL, `PRTN_CLMN007` DECIMAL)\n");
            }
            else
            {
                // Single level partitioning.
                sb.append("PARTITIONED BY (`PRTN_CLMN001` DATE)\n");
            }
        }

        sb.append("[Row Format]\n");
        sb.append(String.format("STORED AS [Hive File Format]%s\n", partitionLevels > 0 ? ";" : ""));

        if (partitionLevels == 0)
        {
            // Add a location statement for a non-partitioned table for the business object format dll unit tests.
            sb.append("LOCATION '${non-partitioned.table.location}';");
        }

        String ddlTemplate = sb.toString().trim();
        Pattern pattern = Pattern.compile("\\[(.+?)\\]");
        Matcher matcher = pattern.matcher(ddlTemplate);
        HashMap<String, String> replacements = new HashMap<>();

        // Populate the replacements map.
        replacements.put("Table Name", AbstractServiceTest.TABLE_NAME);
        replacements.put("Random Suffix", AbstractServiceTest.RANDOM_SUFFIX);
        replacements.put("Format Version", String.valueOf(AbstractServiceTest.FORMAT_VERSION));
        replacements.put("Data Version", String.valueOf(AbstractServiceTest.DATA_VERSION));
        replacements.put("Row Format", hiveRowFormat);
        replacements.put("Hive File Format", hiveFileFormat);
        replacements.put("Format File Type", businessObjectFormatFileType.toLowerCase());
        replacements.put("If Not Exists", isIfNotExistsOptionIncluded ? "IF NOT EXISTS " : "");

        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find())
        {
            String replacement = replacements.get(matcher.group(1));
            builder.append(ddlTemplate.substring(i, matcher.start()));
            if (replacement == null)
            {
                builder.append(matcher.group(0));
            }
            else
            {
                builder.append(replacement);
            }
            i = matcher.end();
        }

        builder.append(ddlTemplate.substring(i, ddlTemplate.length()));

        return builder.toString();
    }

    /**
     * Creates an expected generate business object format ddl collection response using hard coded test values.
     *
     * @return the business object format ddl collection response
     */
    public BusinessObjectFormatDdlCollectionResponse getExpectedBusinessObjectFormatDdlCollectionResponse()
    {
        // Prepare a generate business object data collection response using hard coded test values.
        BusinessObjectFormatDdlCollectionResponse businessObjectFormatDdlCollectionResponse = new BusinessObjectFormatDdlCollectionResponse();

        // Create a list of business object data ddl responses.
        List<BusinessObjectFormatDdl> businessObjectFormatDdlResponses = new ArrayList<>();
        businessObjectFormatDdlCollectionResponse.setBusinessObjectFormatDdlResponses(businessObjectFormatDdlResponses);

        // Get the actual HIVE DDL expected to be generated.
        String expectedDdl = getExpectedBusinessObjectFormatDdl();

        // Create a business object data ddl response.
        BusinessObjectFormatDdl expectedBusinessObjectFormatDdl =
            new BusinessObjectFormatDdl(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL,
                AbstractServiceTest.TABLE_NAME, AbstractServiceTest.NO_CUSTOM_DDL_NAME, expectedDdl);

        // Add two business object ddl responses to the collection response.
        businessObjectFormatDdlResponses.add(expectedBusinessObjectFormatDdl);
        businessObjectFormatDdlResponses.add(expectedBusinessObjectFormatDdl);

        // Set the expected DDL collection value.
        businessObjectFormatDdlCollectionResponse.setDdlCollection(String.format("%s\n\n%s", expectedDdl, expectedDdl));

        return businessObjectFormatDdlCollectionResponse;
    }

    /**
     * Returns an expected string representation of the specified business object format key.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     *
     * @return the string representation of the specified business object format key
     */
    public String getExpectedBusinessObjectFormatKeyAsString(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion)
    {
        return String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d", namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage,
            businessObjectFormatFileType, businessObjectFormatVersion);
    }

    /**
     * Returns the business object format not found error message per specified parameters.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     *
     * @return the business object format not found error message
     */
    public String getExpectedBusinessObjectFormatNotFoundErrorMessage(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion)
    {
        return String.format("Business object format with namespace \"%s\", business object definition name \"%s\"," +
                " format usage \"%s\", format file type \"%s\", and format version \"%d\" doesn't exist.", namespaceCode, businessObjectDefinitionName,
            businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion);
    }

    /**
     * Returns a list of attribute definitions that use hard coded test values.
     *
     * @return the list of test attribute definitions
     */
    public List<AttributeDefinition> getTestAttributeDefinitions()
    {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();

        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.NO_PUBLISH_ATTRIBUTE));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.NO_PUBLISH_ATTRIBUTE));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_3_MIXED_CASE, AbstractServiceTest.PUBLISH_ATTRIBUTE));

        return attributeDefinitions;
    }

    /**
     * Returns a list of attribute definitions that use hard coded test values.
     *
     * @return the list of test attribute definitions
     */
    public List<AttributeDefinition> getTestAttributeDefinitions2()
    {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();

        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.NO_PUBLISH_ATTRIBUTE));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_2, AbstractServiceTest.NO_PUBLISH_ATTRIBUTE));
        attributeDefinitions.add(new AttributeDefinition(AbstractServiceTest.ATTRIBUTE_NAME_3, AbstractServiceTest.PUBLISH_ATTRIBUTE));

        return attributeDefinitions;
    }

    /**
     * Creates a generate business object format ddl collection request using hard coded test values.
     *
     * @return the business object format ddl collection request
     */
    public BusinessObjectFormatDdlCollectionRequest getTestBusinessObjectFormatDdlCollectionRequest()
    {
        // Create a generate business object format ddl collection request.
        BusinessObjectFormatDdlCollectionRequest businessObjectFormatDdlCollectionRequest = new BusinessObjectFormatDdlCollectionRequest();

        // Create a list of generate business object format ddl requests.
        List<BusinessObjectFormatDdlRequest> businessObjectFormatDdlRequests = new ArrayList<>();
        businessObjectFormatDdlCollectionRequest.setBusinessObjectFormatDdlRequests(businessObjectFormatDdlRequests);

        // Create a generate business object format ddl request.
        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest =
            new BusinessObjectFormatDdlRequest(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL,
                AbstractServiceTest.TABLE_NAME, AbstractServiceTest.NO_CUSTOM_DDL_NAME, AbstractServiceTest.INCLUDE_DROP_TABLE_STATEMENT,
                AbstractServiceTest.INCLUDE_IF_NOT_EXISTS_OPTION, null);

        // Add two business object ddl requests to the collection request.
        businessObjectFormatDdlRequests.add(businessObjectFormatDdlRequest);
        businessObjectFormatDdlRequests.add(businessObjectFormatDdlRequest);

        return businessObjectFormatDdlCollectionRequest;
    }

    /**
     * Creates and returns a business object format ddl request using passed parameters along with some hard-coded test values.
     *
     * @param customDdlName the custom DDL name
     *
     * @return the newly created business object format ddl request
     */
    public BusinessObjectFormatDdlRequest getTestBusinessObjectFormatDdlRequest(String customDdlName)
    {
        BusinessObjectFormatDdlRequest request = new BusinessObjectFormatDdlRequest();

        request.setNamespace(AbstractServiceTest.NAMESPACE);
        request.setBusinessObjectDefinitionName(AbstractServiceTest.BDEF_NAME);
        request.setBusinessObjectFormatUsage(AbstractServiceTest.FORMAT_USAGE_CODE);
        request.setBusinessObjectFormatFileType(FileTypeEntity.TXT_FILE_TYPE);
        request.setBusinessObjectFormatVersion(AbstractServiceTest.FORMAT_VERSION);
        request.setOutputFormat(BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL);
        request.setTableName(AbstractServiceTest.TABLE_NAME);
        request.setCustomDdlName(customDdlName);
        request.setIncludeDropTableStatement(true);
        request.setIncludeIfNotExistsOption(true);

        return request;
    }

    /**
     * Returns a business object format schema that uses hard coded test values.
     *
     * @return the test business object format schema
     */
    public Schema getTestSchema()
    {
        Schema schema = new Schema();

        schema.setNullValue(AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N);
        schema.setDelimiter(AbstractServiceTest.SCHEMA_DELIMITER_PIPE);
        schema.setEscapeCharacter(AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH);
        schema.setPartitionKeyGroup(AbstractServiceTest.PARTITION_KEY_GROUP);
        schema.setColumns(schemaColumnDaoTestHelper.getTestSchemaColumns(AbstractServiceTest.RANDOM_SUFFIX));
        schema.setPartitions(schemaColumnDaoTestHelper.getTestPartitionColumns(AbstractServiceTest.RANDOM_SUFFIX));

        return schema;
    }

    /**
     * Returns a business object format schema that uses hard coded test values.
     *
     * @return the test business object format schema
     */
    public Schema getTestSchema2()
    {
        Schema schema = new Schema();

        schema.setNullValue(AbstractServiceTest.SCHEMA_NULL_VALUE_NULL_WORD);
        schema.setDelimiter(AbstractServiceTest.SCHEMA_DELIMITER_COMMA);
        schema.setEscapeCharacter(AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_TILDE);
        schema.setPartitionKeyGroup(AbstractServiceTest.PARTITION_KEY_GROUP_2);
        schema.setColumns(schemaColumnDaoTestHelper.getTestSchemaColumns(AbstractServiceTest.RANDOM_SUFFIX_2));
        schema.setPartitions(schemaColumnDaoTestHelper.getTestPartitionColumns(AbstractServiceTest.RANDOM_SUFFIX_2));

        return schema;
    }

    /**
     * Validates business object format contents against specified arguments and expected (hard coded) test values.
     *
     * @param expectedBusinessObjectFormatId the expected business object format ID
     * @param expectedNamespaceCode the expected namespace code
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedIsLatestVersion the expected business object format version
     * @param expectedPartitionKey the expected business object format partition key
     * @param expectedDescription the expected business object format description
     * @param expectedDocumentSchema the expected business object format document schema
     * @param expectedAttributes the expected attributes
     * @param expectedAttributeDefinitions the list of expected attribute definitions
     * @param expectedSchema the expected business object format schema
     * @param actualBusinessObjectFormat the BusinessObjectFormat object instance to be validated
     */
    public void validateBusinessObjectFormat(Integer expectedBusinessObjectFormatId, String expectedNamespaceCode, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        Boolean expectedIsLatestVersion, String expectedPartitionKey, String expectedDescription, String expectedDocumentSchema,
        List<Attribute> expectedAttributes, List<AttributeDefinition> expectedAttributeDefinitions, Schema expectedSchema,
        BusinessObjectFormat actualBusinessObjectFormat)
    {
        assertNotNull(actualBusinessObjectFormat);

        if (expectedBusinessObjectFormatId != null)
        {
            assertEquals(expectedBusinessObjectFormatId, Integer.valueOf(actualBusinessObjectFormat.getId()));
        }

        assertEquals(expectedNamespaceCode, actualBusinessObjectFormat.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectFormat.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualBusinessObjectFormat.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualBusinessObjectFormat.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, Integer.valueOf(actualBusinessObjectFormat.getBusinessObjectFormatVersion()));
        assertEquals(expectedIsLatestVersion, actualBusinessObjectFormat.isLatestVersion());
        assertEquals(expectedPartitionKey, actualBusinessObjectFormat.getPartitionKey());
        assertEquals(expectedDocumentSchema, actualBusinessObjectFormat.getDocumentSchema());
        AbstractServiceTest.assertEqualsIgnoreNullOrEmpty("description", expectedDescription, actualBusinessObjectFormat.getDescription());

        // Ignoring the order, check if the actual list of attributes matches the expected list.
        if (!CollectionUtils.isEmpty(expectedAttributes))
        {
            assertEquals(expectedAttributes, actualBusinessObjectFormat.getAttributes());
        }
        else
        {
            assertEquals(0, actualBusinessObjectFormat.getAttributes().size());
        }

        // Ignoring the order, check if the actual list of attribute definitions matches the expected list.
        if (!CollectionUtils.isEmpty(expectedAttributeDefinitions))
        {
            assertEquals(expectedAttributeDefinitions, actualBusinessObjectFormat.getAttributeDefinitions());
        }
        else
        {
            assertEquals(0, actualBusinessObjectFormat.getAttributeDefinitions().size());
        }

        // Validate the schema.
        if (expectedSchema != null)
        {
            assertNotNull(actualBusinessObjectFormat.getSchema());
            AbstractServiceTest
                .assertEqualsIgnoreNullOrEmpty("null value", expectedSchema.getNullValue(), actualBusinessObjectFormat.getSchema().getNullValue());
            AbstractServiceTest
                .assertEqualsIgnoreNullOrEmpty("delimiter", expectedSchema.getDelimiter(), actualBusinessObjectFormat.getSchema().getDelimiter());
            AbstractServiceTest.assertEqualsIgnoreNullOrEmpty("escape character", expectedSchema.getEscapeCharacter(),
                actualBusinessObjectFormat.getSchema().getEscapeCharacter());
            assertEquals(expectedSchema.getPartitionKeyGroup(), actualBusinessObjectFormat.getSchema().getPartitionKeyGroup());
            assertEquals(expectedSchema.getColumns().size(), actualBusinessObjectFormat.getSchema().getColumns().size());

            for (int i = 0; i < expectedSchema.getColumns().size(); i++)
            {
                SchemaColumn expectedSchemaColumn = expectedSchema.getColumns().get(i);
                SchemaColumn actualSchemaColumn = actualBusinessObjectFormat.getSchema().getColumns().get(i);
                assertEquals(expectedSchemaColumn.getName(), actualSchemaColumn.getName());
                assertEquals(expectedSchemaColumn.getType(), actualSchemaColumn.getType());
                assertEquals(expectedSchemaColumn.getSize(), actualSchemaColumn.getSize());
                assertEquals(expectedSchemaColumn.isRequired(), actualSchemaColumn.isRequired());
                assertEquals(expectedSchemaColumn.getDefaultValue(), actualSchemaColumn.getDefaultValue());
                assertEquals(expectedSchemaColumn.getDescription(), actualSchemaColumn.getDescription());
            }

            if (CollectionUtils.isEmpty(expectedSchema.getPartitions()))
            {
                assertTrue(CollectionUtils.isEmpty(actualBusinessObjectFormat.getSchema().getPartitions()));
            }
            else
            {
                for (int i = 0; i < expectedSchema.getPartitions().size(); i++)
                {
                    SchemaColumn expectedPartitionColumn = expectedSchema.getPartitions().get(i);
                    SchemaColumn actualPartitionColumn = actualBusinessObjectFormat.getSchema().getPartitions().get(i);
                    assertEquals(expectedPartitionColumn.getName(), actualPartitionColumn.getName());
                    assertEquals(expectedPartitionColumn.getType(), actualPartitionColumn.getType());
                    assertEquals(expectedPartitionColumn.getSize(), actualPartitionColumn.getSize());
                    assertEquals(expectedPartitionColumn.isRequired(), actualPartitionColumn.isRequired());
                    assertEquals(expectedPartitionColumn.getDefaultValue(), actualPartitionColumn.getDefaultValue());
                    assertEquals(expectedPartitionColumn.getDescription(), actualPartitionColumn.getDescription());
                }
            }
        }
        else
        {
            assertNull(actualBusinessObjectFormat.getSchema());
        }
    }

    /**
     * Validate retention information
     *
     * @param expectedRecordFlag the expected record flag
     * @param expectedRetentionDays the expected retention in days
     * @param expectedRetentionType the expected retention type
     * @param actualBusinessObjectFormat the actual business object format
     */
    public void validateRetentionInformation(boolean expectedRecordFlag, Integer expectedRetentionDays, String expectedRetentionType,
        BusinessObjectFormat actualBusinessObjectFormat)
    {
        assertEquals(actualBusinessObjectFormat.getRetentionType(), expectedRetentionType);
        assertEquals(actualBusinessObjectFormat.isRecordFlag(), expectedRecordFlag);
        assertEquals(actualBusinessObjectFormat.getRetentionPeriodInDays(), expectedRetentionDays);
    }

    /**
     * Validates business object format ddl object instance against specified parameters.
     *
     * @param expectedNamespaceCode the expected namespace code
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedOutputFormat the expected output format
     * @param expectedTableName the expected table name
     * @param expectedCustomDdlName the expected custom ddl name
     * @param expectedDdl the expected DDL
     * @param actualBusinessObjectFormatDdl the business object format ddl object instance to be validated
     */
    public void validateBusinessObjectFormatDdl(String expectedNamespaceCode, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        BusinessObjectDataDdlOutputFormatEnum expectedOutputFormat, String expectedTableName, String expectedCustomDdlName, String expectedDdl,
        BusinessObjectFormatDdl actualBusinessObjectFormatDdl)
    {
        assertNotNull(actualBusinessObjectFormatDdl);
        assertEquals(expectedNamespaceCode, actualBusinessObjectFormatDdl.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectFormatDdl.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualBusinessObjectFormatDdl.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualBusinessObjectFormatDdl.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, actualBusinessObjectFormatDdl.getBusinessObjectFormatVersion());
        assertEquals(expectedOutputFormat, actualBusinessObjectFormatDdl.getOutputFormat());
        assertEquals(expectedTableName, actualBusinessObjectFormatDdl.getTableName());
        assertEquals(expectedCustomDdlName, actualBusinessObjectFormatDdl.getCustomDdlName());
        assertEquals(expectedDdl, actualBusinessObjectFormatDdl.getDdl());
    }

    /**
     * Validates business object format ddl object instance against hard coded test values.
     *
     * @param expectedCustomDdlName the expected custom ddl name
     * @param expectedDdl the expected DDL
     * @param actualBusinessObjectFormatDdl the business object format ddl object instance to be validated
     */
    public void validateBusinessObjectFormatDdl(String expectedCustomDdlName, String expectedDdl, BusinessObjectFormatDdl actualBusinessObjectFormatDdl)
    {
        validateBusinessObjectFormatDdl(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
            FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION, BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, AbstractServiceTest.TABLE_NAME,
            expectedCustomDdlName, expectedDdl, actualBusinessObjectFormatDdl);
    }

    /**
     * Validates a business object data status change notification message.
     *
     * @param expectedMessageType the expected message type
     * @param expectedMessageDestination the expected message destination
     * @param expectedBusinessObjectFormatKey the expected business object data key
     * @param expectedUsername the expected username
     * @param expectedNewBusinessObjectFormatVersion the expected new business object data status
     * @param expectedOldBusinessObjectFormatVersion the expected old business object data status
     * @param expectedMessageHeaders the list of expected message headers
     * @param notificationMessage the notification message to be validated
     */
    public void validateBusinessObjectFormatVersionChangeMessageWithXmlPayload(String expectedMessageType, String expectedMessageDestination,
        BusinessObjectFormatKey expectedBusinessObjectFormatKey, String expectedUsername, String expectedNewBusinessObjectFormatVersion,
        String expectedOldBusinessObjectFormatVersion, List<MessageHeader> expectedMessageHeaders, NotificationMessage notificationMessage)
    {
        assertNotNull(notificationMessage);

        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        String messageText = notificationMessage.getMessageText();

        validateXmlFieldPresent(messageText, "triggered-by-username", expectedUsername);
        validateXmlFieldPresent(messageText, "context-message-type", "testDomain/testApplication/BusinessObjectFormatVersionChanged");
        validateXmlFieldPresent(messageText, "newBusinessObjectFormatVersion", expectedNewBusinessObjectFormatVersion);

        if (expectedOldBusinessObjectFormatVersion == null)
        {
            validateXmlFieldNotPresent(messageText, "oldBusinessObjectFormatVersion");
        }
        else
        {
            validateXmlFieldPresent(messageText, "oldBusinessObjectFormatVersion", expectedOldBusinessObjectFormatVersion);
        }

        validateXmlFieldPresent(messageText, "namespace", expectedBusinessObjectFormatKey.getNamespace());
        validateXmlFieldPresent(messageText, "businessObjectDefinitionName", expectedBusinessObjectFormatKey.getBusinessObjectDefinitionName());
        validateXmlFieldPresent(messageText, "businessObjectFormatUsage", expectedBusinessObjectFormatKey.getBusinessObjectFormatUsage());
        validateXmlFieldPresent(messageText, "businessObjectFormatFileType", expectedBusinessObjectFormatKey.getBusinessObjectFormatFileType());
        validateXmlFieldPresent(messageText, "businessObjectFormatVersion", expectedBusinessObjectFormatKey.getBusinessObjectFormatVersion());

        assertEquals(expectedMessageHeaders, notificationMessage.getMessageHeaders());
    }

    /**
     * Validates that a specified XML opening and closing set of tags are not present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     */
    private void validateXmlFieldNotPresent(String message, String xmlTagName)
    {
        for (String xmlTag : Arrays.asList(String.format("<%s>", xmlTagName), String.format("</%s>", xmlTagName)))
        {
            assertTrue(String.format("%s tag not expected, but found.", xmlTag), !message.contains(xmlTag));
        }
    }

    /**
     * Validates that a specified XML tag and value are present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     * @param value the value of the data for the tag.
     */
    private void validateXmlFieldPresent(String message, String xmlTagName, Object value)
    {
        assertTrue(xmlTagName + " \"" + value + "\" expected, but not found.",
            message.contains("<" + xmlTagName + ">" + (value == null ? null : value.toString()) + "</" + xmlTagName + ">"));
    }
}
