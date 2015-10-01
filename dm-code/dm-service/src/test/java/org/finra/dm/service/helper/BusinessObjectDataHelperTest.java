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
package org.finra.dm.service.helper;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.SchemaColumn;
import org.finra.dm.service.AbstractServiceTest;

public class BusinessObjectDataHelperTest extends AbstractServiceTest
{
    private static final String S3_KEY_PREFIX_TEMPLATE_HIGH_ENV =
        "~namespace~/~dataProviderName~/~businessObjectFormatUsage~/~businessObjectFormatFileType~/~businessObjectDefinitionName~" +
            "/schm-v~businessObjectFormatVersion~/data-v~businessObjectDataVersion~" + "/~businessObjectFormatPartitionKey~=~businessObjectDataPartitionValue~";
    private static final String S3_KEY_PREFIX_TEMPLATE = "s3.key.prefix.template";
    private static final Logger logger = Logger.getLogger(BusinessObjectDataHelperTest.class);

    @After
    public void cleanupEnv()
    {
        try
        {
            restorePropertySourceInEnvironment();
        }
        catch (Exception e)
        {
            // This method throws an exception when no override happens. Ignore the error.
        }
    }

    @Test
    public void testSubPartitions()
    {
        logger.info("testSubPartitions()");

        String namespace = NAMESPACE_CD;
        String dataProvider = DATA_PROVIDER_NAME;
        String businessObjectDefinitionName = BOD_NAME;
        String businessObjectFormatUsage = FORMAT_USAGE_CODE;
        String fileType = FORMAT_FILE_TYPE_CODE;
        Integer businessObjectFormatVersion = FORMAT_VERSION;
        String businessObjectFormatPartitionKey = PARTITION_KEY;
        List<SchemaColumn> schemaColumns = getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        String partitionValue = PARTITION_VALUE;
        List<String> subPartitionValues = SUBPARTITION_VALUES;
        Integer businessObjectDataVersion = DATA_VERSION;

        String actualS3KeyPrefix = buildS3KeyPrefix(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatPartitionKey, schemaColumns, partitionColumns, partitionValue, subPartitionValues, businessObjectDataVersion);

        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(namespace, dataProvider, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
                businessObjectFormatPartitionKey, partitionColumns, partitionValue, subPartitionValues, businessObjectDataVersion, "frmt-v");

        logger.info("actualS3KeyPrefix = " + actualS3KeyPrefix);
        logger.info("expectedS3KeyPrefix = " + expectedS3KeyPrefix);
        assertEquals(expectedS3KeyPrefix, actualS3KeyPrefix);
    }

    @Test
    public void testSubPartitionsTemplateIsSpecified() throws Exception
    {
        logger.info("testSubPartitionsTemplateIsNotSpecified()");

        String s3KeyPrefixTemplateOverride = S3_KEY_PREFIX_TEMPLATE_HIGH_ENV;
        logger.info("Overriding property '" + S3_KEY_PREFIX_TEMPLATE + "' to '" + s3KeyPrefixTemplateOverride + "'");

        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(S3_KEY_PREFIX_TEMPLATE, s3KeyPrefixTemplateOverride);
        modifyPropertySourceInEnvironment(overrideMap);

        String namespace = NAMESPACE_CD;
        String dataProvider = DATA_PROVIDER_NAME;
        String businessObjectDefinitionName = BOD_NAME;
        String businessObjectFormatUsage = FORMAT_USAGE_CODE;
        String fileType = FORMAT_FILE_TYPE_CODE;
        Integer businessObjectFormatVersion = FORMAT_VERSION;
        String businessObjectFormatPartitionKey = PARTITION_KEY;
        List<SchemaColumn> schemaColumns = getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = getTestPartitionColumns();
        String partitionValue = PARTITION_VALUE;
        List<String> subPartitionValues = SUBPARTITION_VALUES;
        Integer businessObjectDataVersion = DATA_VERSION;

        String actualS3KeyPrefix = buildS3KeyPrefix(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatPartitionKey, schemaColumns, partitionColumns, partitionValue, subPartitionValues, businessObjectDataVersion);

        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(namespace, dataProvider, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
                businessObjectFormatPartitionKey, partitionColumns, partitionValue, subPartitionValues, businessObjectDataVersion, "schm-v");

        logger.info("actualS3KeyPrefix = " + actualS3KeyPrefix);
        logger.info("expectedS3KeyPrefix = " + expectedS3KeyPrefix);
        assertEquals(expectedS3KeyPrefix, actualS3KeyPrefix);
    }

    private String buildS3KeyPrefix(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage, String fileType,
        Integer businessObjectFormatVersion, String businessObjectFormatPartitionKey, List<SchemaColumn> schemaColumns, List<SchemaColumn> partitionColumns,
        String partitionValue, List<String> subPartitionValues, Integer businessObjectDataVersion)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity =
            createBusinessObjectFormatEntity(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion, null,
                true, businessObjectFormatPartitionKey, null, null, null, null, schemaColumns, partitionColumns);
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion, partitionValue,
                subPartitionValues, businessObjectDataVersion);

        return businessObjectDataHelper.buildS3KeyPrefix(businessObjectFormatEntity, businessObjectDataKey);
    }

    private String getExpectedS3KeyPrefix(String namespace, String dataProvider, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String fileType, Integer businessObjectFormatVersion, String businessObjectFormatPartitionKey, List<SchemaColumn> partitionColumns,
        String partitionValue, List<String> subPartitionValues, Integer businessObjectDataVersion, String formatVersionPrefix)
    {
        partitionColumns = partitionColumns.subList(0, Math.min(partitionColumns.size(), 5));
        SchemaColumn[] subPartitionKeys = new SchemaColumn[partitionColumns.size() - 1];
        subPartitionKeys = partitionColumns.subList(1, partitionColumns.size()).toArray(subPartitionKeys);

        String[] subPartitionValueArray = new String[subPartitionValues.size()];
        subPartitionValueArray = subPartitionValues.toArray(subPartitionValueArray);

        return getExpectedS3KeyPrefix(namespace, dataProvider, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatPartitionKey, partitionValue, subPartitionKeys, subPartitionValueArray, businessObjectDataVersion, formatVersionPrefix);
    }
}
