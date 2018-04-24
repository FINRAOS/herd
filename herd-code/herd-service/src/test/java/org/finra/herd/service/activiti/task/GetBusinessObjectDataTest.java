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
package org.finra.herd.service.activiti.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.activiti.bpmn.model.FieldExtension;
import org.activiti.engine.history.HistoricProcessInstance;
import org.apache.commons.collections4.IterableUtils;
import org.junit.Test;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusChangeEvent;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateRequest;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitStatusChangeEvent;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * <p> Test suite for Get Business Object Data Activiti wrapper. </p>
 * <p/>
 * Test plan: <ol> <li>Insert test business object data into database, where the inserted data is configurable per test case.</li> <li>Execute Activiti job
 * using configurable input parameters</li> <li>Validate response by converting JSON into response object upon success, or verifying ERROR status on
 * exceptions.</li> </ol>
 */
public class GetBusinessObjectDataTest extends HerdActivitiServiceTaskTest
{
    /**
     * The delimiter character to use to construct sub-partition values.
     */
    private static final char DELIMITER = '|';

    /**
     * The escape pattern for sub-partition values delimiter. This should be used as replacement when using .replaceAll()
     */
    private static final String DELIMITER_ESCAPE = Matcher.quoteReplacement("\\" + DELIMITER);

    /**
     * The search regex for sub-partition value delimiter. This should be used as search pattern when using .replaceAll()
     */
    private static final String PATTERN_DELIMITER = Pattern.quote(String.valueOf(DELIMITER));

    /**
     * The Activiti variable name suffix for JSON response.
     */
    private static final String VARIABLE_JSON_RESPONSE = "jsonResponse";

    /**
     * The implementation of the Activiti wrapper we are testing.
     */
    private static final String IMPLEMENTATION = GetBusinessObjectData.class.getCanonicalName();

    @Test
    public void test_WithSubPartitions_NoFormatVersion_NoDataVersion() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), randomString(), randomString(), randomString(), randomString()};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        String subPartitionValues = buildDelimitedSubPartitionValues(partitionValues);
        String partitionValue = partitionValues[0];

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValue, subPartitionValues, null,
                null);

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_SUCCESS, serviceTaskStatus);

        // Validate JSON response
        String jsonResponse = (String) variables.get(getServiceTaskVariableName(VARIABLE_JSON_RESPONSE));

        ObjectMapper om = new ObjectMapper();
        BusinessObjectData businessObjectData = om.readValue(jsonResponse.getBytes(), BusinessObjectData.class);

        assertEquals(namespace, businessObjectData.getNamespace());
        assertEquals(businessObjectDefinitionName, businessObjectData.getBusinessObjectDefinitionName());
        assertEquals(businessObjectFormatUsage, businessObjectData.getBusinessObjectFormatUsage());
        assertEquals(fileTypeCode, businessObjectData.getBusinessObjectFormatFileType());
        assertEquals(businessObjectFormatVersion.intValue(), businessObjectData.getBusinessObjectFormatVersion());
        assertEquals(partitionKey, businessObjectData.getPartitionKey());
        assertEquals(partitionValue, businessObjectData.getPartitionValue());
        assertEquals(4, businessObjectData.getSubPartitionValues().size());
        assertEquals(partitionValues[1], businessObjectData.getSubPartitionValues().get(0));
        assertEquals(partitionValues[2], businessObjectData.getSubPartitionValues().get(1));
        assertEquals(partitionValues[3], businessObjectData.getSubPartitionValues().get(2));
        assertEquals(partitionValues[4], businessObjectData.getSubPartitionValues().get(3));
        assertEquals(businessObjectDataVersion.intValue(), businessObjectData.getVersion());
        assertEquals(true, businessObjectData.isLatestVersion());
        assertEquals(1, businessObjectData.getStorageUnits().size());
        assertEquals(0, businessObjectData.getAttributes().size());
        assertEquals(0, businessObjectData.getBusinessObjectDataParents().size());
        assertEquals(0, businessObjectData.getBusinessObjectDataChildren().size());
    }

    @Test
    public void test_WithSubPartitions_NoFormatVersion_NoDataVersion_WithSubPartitionDelimiterEscapeValue() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues =
            {randomString() + DELIMITER, randomString() + DELIMITER, randomString() + DELIMITER, randomString() + DELIMITER, randomString() + DELIMITER};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        String subPartitionValues = buildDelimitedSubPartitionValues(partitionValues);
        String partitionValue = partitionValues[0];

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValue, subPartitionValues, null,
                null);

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_SUCCESS, serviceTaskStatus);

        // Validate JSON response
        String jsonResponse = (String) variables.get(getServiceTaskVariableName(VARIABLE_JSON_RESPONSE));

        ObjectMapper om = new ObjectMapper();
        BusinessObjectData businessObjectData = om.readValue(jsonResponse.getBytes(), BusinessObjectData.class);

        assertEquals(namespace, businessObjectData.getNamespace());
        assertEquals(businessObjectDefinitionName, businessObjectData.getBusinessObjectDefinitionName());
        assertEquals(businessObjectFormatUsage, businessObjectData.getBusinessObjectFormatUsage());
        assertEquals(fileTypeCode, businessObjectData.getBusinessObjectFormatFileType());
        assertEquals(businessObjectFormatVersion.intValue(), businessObjectData.getBusinessObjectFormatVersion());
        assertEquals(partitionKey, businessObjectData.getPartitionKey());
        assertEquals(partitionValue, businessObjectData.getPartitionValue());
        assertNotNull(businessObjectData.getSubPartitionValues());
        assertEquals(4, businessObjectData.getSubPartitionValues().size());
        assertEquals(partitionValues[1], businessObjectData.getSubPartitionValues().get(0));
        assertEquals(partitionValues[2], businessObjectData.getSubPartitionValues().get(1));
        assertEquals(partitionValues[3], businessObjectData.getSubPartitionValues().get(2));
        assertEquals(partitionValues[4], businessObjectData.getSubPartitionValues().get(3));
        assertEquals(businessObjectDataVersion.intValue(), businessObjectData.getVersion());
        assertEquals(true, businessObjectData.isLatestVersion());
        assertEquals(1, businessObjectData.getStorageUnits().size());
        assertEquals(0, businessObjectData.getAttributes().size());
        assertEquals(0, businessObjectData.getBusinessObjectDataParents().size());
        assertEquals(0, businessObjectData.getBusinessObjectDataChildren().size());
    }

    @Test
    public void test_NoSubPartitions_WithFormatVersion_WithDataVersion() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), null, null, null, null};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValues[0], null,
                businessObjectFormatVersion.toString(), businessObjectDataVersion.toString());

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_SUCCESS, serviceTaskStatus);

        // Validate JSON response
        String jsonResponse = (String) variables.get(getServiceTaskVariableName(VARIABLE_JSON_RESPONSE));

        ObjectMapper om = new ObjectMapper();
        BusinessObjectData businessObjectData = om.readValue(jsonResponse.getBytes(), BusinessObjectData.class);

        assertEquals(namespace, businessObjectData.getNamespace());
        assertEquals(businessObjectDefinitionName, businessObjectData.getBusinessObjectDefinitionName());
        assertEquals(businessObjectFormatUsage, businessObjectData.getBusinessObjectFormatUsage());
        assertEquals(fileTypeCode, businessObjectData.getBusinessObjectFormatFileType());
        assertEquals(businessObjectFormatVersion.intValue(), businessObjectData.getBusinessObjectFormatVersion());
        assertEquals(partitionKey, businessObjectData.getPartitionKey());
        assertEquals(partitionValues[0], businessObjectData.getPartitionValue());
        assertEquals(0, businessObjectData.getSubPartitionValues().size());
        assertEquals(businessObjectDataVersion.intValue(), businessObjectData.getVersion());
        assertEquals(true, businessObjectData.isLatestVersion());
        assertEquals(1, businessObjectData.getStorageUnits().size());
        assertEquals(0, businessObjectData.getAttributes().size());
        assertEquals(0, businessObjectData.getBusinessObjectDataParents().size());
        assertEquals(0, businessObjectData.getBusinessObjectDataChildren().size());
    }

    @Test
    public void test_NoSubPartitions_WithFormatVersion_WithDataVersion_InvalidDataVersion() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), null, null, null, null};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            Map<String, Object> variables =
                executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValues[0], null,
                    businessObjectFormatVersion.toString(), "INVALID");

            // Validate status
            String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
            assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
        });
    }

    @Test
    public void test_NoSubPartitions_WithFormatVersion_WithDataVersion_InvalidFormatVersion() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), null, null, null, null};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            Map<String, Object> variables =
                executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValues[0], null, "INVALID",
                    businessObjectDataVersion.toString());

            // Validate status
            String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
            assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
        });
    }

    @Test
    public void test_NoSubPartitions_NoFormatVersion_NoDataVersion() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), null, null, null, null};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValues[0], null, null, null);

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_SUCCESS, serviceTaskStatus);

        // Validate JSON response
        String jsonResponse = (String) variables.get(getServiceTaskVariableName(VARIABLE_JSON_RESPONSE));

        ObjectMapper om = new ObjectMapper();
        BusinessObjectData businessObjectData = om.readValue(jsonResponse.getBytes(), BusinessObjectData.class);

        assertEquals(namespace, businessObjectData.getNamespace());
        assertEquals(businessObjectDefinitionName, businessObjectData.getBusinessObjectDefinitionName());
        assertEquals(businessObjectFormatUsage, businessObjectData.getBusinessObjectFormatUsage());
        assertEquals(fileTypeCode, businessObjectData.getBusinessObjectFormatFileType());
        assertEquals(businessObjectFormatVersion.intValue(), businessObjectData.getBusinessObjectFormatVersion());
        assertEquals(partitionKey, businessObjectData.getPartitionKey());
        assertEquals(partitionValues[0], businessObjectData.getPartitionValue());
        assertEquals(0, businessObjectData.getSubPartitionValues().size());
        assertEquals(businessObjectDataVersion.intValue(), businessObjectData.getVersion());
        assertEquals(true, businessObjectData.isLatestVersion());
        assertEquals(1, businessObjectData.getStorageUnits().size());
        assertEquals(0, businessObjectData.getAttributes().size());
        assertEquals(0, businessObjectData.getBusinessObjectDataParents().size());
        assertEquals(0, businessObjectData.getBusinessObjectDataChildren().size());
    }

    @Test
    public void test_NoSubPartitions_NoFormatVersion_NoDataVersion_InvalidPartitionKey() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), null, null, null, null};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, "INVALID", partitionValues);

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            Map<String, Object> variables =
                executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValues[0], null, null,
                    null);

            // Validate status
            String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
            assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
        });
    }

    @Test
    public void test_NoSubPartitions_NoFormatVersion_NoDataVersion_NoPartitionKey() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), null, null, null, null};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, null, partitionValues[0], null, null, null);

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_SUCCESS, serviceTaskStatus);
    }

    @Test
    public void test_NoSubPartitions_NoFormatVersion_NoDataVersion_NoFileType() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), null, null, null, null};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            Map<String, Object> variables =
                executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, null, partitionKey, partitionValues[0], null, null, null);

            // Validate status
            String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
            assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
        });
    }

    @Test
    public void test_NoSubPartitions_NoFormatVersion_NoDataVersion_NoFormatUsage() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), null, null, null, null};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            Map<String, Object> variables =
                executeJob(namespace, businessObjectDefinitionName, null, fileTypeCode, partitionKey, partitionValues[0], null, null, null);

            // Validate status
            String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
            assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
        });
    }

    @Test
    public void test_NoSubPartitions_NoFormatVersion_NoDataVersion_NoDefinitionName() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), null, null, null, null};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            Map<String, Object> variables =
                executeJob(namespace, null, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValues[0], null, null, null);

            // Validate status
            String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
            assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
        });
    }

    @Test
    public void test_NoSubPartitions_NoFormatVersion_NoDataVersion_InvalidPartitionValue() throws Exception
    {
        // Information that will be used for both set up data and input data
        String dataProviderName = randomString();
        String namespace = randomString();
        String businessObjectDefinitionName = randomString();
        String fileTypeCode = randomString();
        Integer businessObjectFormatVersion = 0;
        String partitionKey = randomString();
        String businessObjectFormatUsage = randomString();
        String[] partitionValues = {randomString(), null, null, null, null};
        Integer businessObjectDataVersion = 0;

        // Set up database
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues);

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            Map<String, Object> variables =
                executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, "INVALID", null, null, null);

            // Validate status
            String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
            assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
        });
    }

    @Test
    public void test_NoSubPartitions_NoFormatVersion_NoDataVersion_SubPartitionValuesAsEmptyString() throws Exception
    {
        // Create a business object data key without sub-partition values.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("partitionValue", "${partitionValue}"));
        fieldExtensionList.add(buildFieldExtension("subPartitionValues", "${subPartitionValues}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("namespace", BDEF_NAMESPACE));
        parameters.add(buildParameter("businessObjectDefinitionName", BDEF_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("partitionValue", PARTITION_VALUE));
        parameters.add(buildParameter("subPartitionValues", EMPTY_STRING));

        // Build the expected response object.
        BusinessObjectData expectedBusinessObjectData =
            new BusinessObjectData(businessObjectDataEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                NO_STORAGE_UNITS, NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_BUSINESS_OBJECT_DATA_CHILDREN, NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY, NO_RETENTION_EXPIRATION_DATE);

        // Run the activiti task and validate the returned response object.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetBusinessObjectDataStatus.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedBusinessObjectData));
        testActivitiServiceTaskSuccess(GetBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testGetBusinessObjectDataIncludeBusinessObjectDataStatusHistory() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("partitionValue", "${partitionValue}"));
        fieldExtensionList.add(buildFieldExtension("subPartitionValues", "${subPartitionValues}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStatus", "${businessObjectDataStatus}"));
        fieldExtensionList.add(buildFieldExtension("includeBusinessObjectDataStatusHistory", "${includeBusinessObjectDataStatusHistory}"));
        fieldExtensionList.add(buildFieldExtension("includeStorageUnitStatusHistory", "${includeStorageUnitStatusHistory}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("namespace", BDEF_NAMESPACE));
        parameters.add(buildParameter("businessObjectDefinitionName", BDEF_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("partitionValue", PARTITION_VALUE));
        parameters.add(buildParameter("subPartitionValues", herdStringHelper.buildStringWithDefaultDelimiter(SUBPARTITION_VALUES)));
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataStatus", BusinessObjectDataStatusEntity.VALID));
        parameters.add(buildParameter("includeBusinessObjectDataStatusHistory", INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY.toString()));
        parameters.add(buildParameter("includeStorageUnitStatusHistory", NO_INCLUDE_STORAGE_UNIT_STATUS_HISTORY.toString()));

        // Build the expected response object. The business object data history record is expected to have system username for the createdBy auditable field.
        BusinessObjectData expectedBusinessObjectData =
            new BusinessObjectData(businessObjectDataEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                NO_STORAGE_UNITS, NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS, NO_BUSINESS_OBJECT_DATA_CHILDREN, Arrays.asList(
                new BusinessObjectDataStatusChangeEvent(BusinessObjectDataStatusEntity.VALID,
                    HerdDateUtils.getXMLGregorianCalendarValue(IterableUtils.get(businessObjectDataEntity.getHistoricalStatuses(), 0).getCreatedOn()),
                    HerdDaoSecurityHelper.SYSTEM_USER)), NO_RETENTION_EXPIRATION_DATE);

        // Run the activiti task and validate the returned response object.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetBusinessObjectDataStatus.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedBusinessObjectData));
        testActivitiServiceTaskSuccess(GetBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testGetBusinessObjectDataIncludeStorageUnitStatusHistory() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity =
            storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, StorageUnitStatusEntity.DISABLED);

        // Execute a storage unit status change.
        businessObjectDataStorageUnitStatusService.updateBusinessObjectDataStorageUnitStatus(businessObjectDataStorageUnitKey,
            new BusinessObjectDataStorageUnitStatusUpdateRequest(StorageUnitStatusEntity.ENABLED));

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("partitionValue", "${partitionValue}"));
        fieldExtensionList.add(buildFieldExtension("subPartitionValues", "${subPartitionValues}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStatus", "${businessObjectDataStatus}"));
        fieldExtensionList.add(buildFieldExtension("includeBusinessObjectDataStatusHistory", "${includeBusinessObjectDataStatusHistory}"));
        fieldExtensionList.add(buildFieldExtension("includeStorageUnitStatusHistory", "${includeStorageUnitStatusHistory}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("namespace", BDEF_NAMESPACE));
        parameters.add(buildParameter("businessObjectDefinitionName", BDEF_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("partitionValue", PARTITION_VALUE));
        parameters.add(buildParameter("subPartitionValues", herdStringHelper.buildStringWithDefaultDelimiter(SUBPARTITION_VALUES)));
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataStatus", BusinessObjectDataStatusEntity.VALID));
        parameters.add(buildParameter("includeBusinessObjectDataStatusHistory", NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY.toString()));
        parameters.add(buildParameter("includeStorageUnitStatusHistory", INCLUDE_STORAGE_UNIT_STATUS_HISTORY.toString()));

        // Build the expected response object. The storage unit history record is expected to have system username for the createdBy auditable field.
        BusinessObjectData expectedBusinessObjectData =
            new BusinessObjectData(storageUnitEntity.getBusinessObjectData().getId(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, Arrays.asList(
                new StorageUnit(new Storage(STORAGE_NAME, StoragePlatformEntity.S3, null), NO_STORAGE_DIRECTORY, null, StorageUnitStatusEntity.ENABLED, Arrays
                    .asList(new StorageUnitStatusChangeEvent(StorageUnitStatusEntity.ENABLED,
                        HerdDateUtils.getXMLGregorianCalendarValue(IterableUtils.get(storageUnitEntity.getHistoricalStatuses(), 0).getCreatedOn()),
                        HerdDaoSecurityHelper.SYSTEM_USER)), NO_STORAGE_POLICY_TRANSITION_FAILED_ATTEMPTS, NO_RESTORE_EXPIRATION_ON)), NO_ATTRIBUTES, NO_BUSINESS_OBJECT_DATA_PARENTS,
                NO_BUSINESS_OBJECT_DATA_CHILDREN, NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY, NO_RETENTION_EXPIRATION_DATE);

        // Run the activiti task and validate the returned response object.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetBusinessObjectDataStatus.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedBusinessObjectData));
        testActivitiServiceTaskSuccess(GetBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testGetBusinessObjectDataInvalidIncludeBusinessObjectDataStatusHistory() throws Exception
    {
        // Validate that activiti task fails when we pass non-boolean value for the include business object data status history flag.
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        fieldExtensionList.add(buildFieldExtension("includeBusinessObjectDataStatusHistory", "${includeBusinessObjectDataStatusHistory}"));
        fieldExtensionList.add(buildFieldExtension("includeStorageUnitStatusHistory", "${includeStorageUnitStatusHistory}"));
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));
        parameters.add(buildParameter("includeBusinessObjectDataStatusHistory", "NOT_A_BOOLEAN"));
        parameters.add(buildParameter("includeStorageUnitStatusHistory", NO_INCLUDE_STORAGE_UNIT_STATUS_HISTORY.toString()));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE,
            "\"includeBusinessObjectDataStatusHistory\" must be a valid boolean value of \"true\" or \"false\".");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(GetBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    @Test
    public void testGetBusinessObjectDataInvalidIncludeStorageUnitStatusHistory() throws Exception
    {
        // Validate that activiti task fails when we pass non-boolean value for the include business object data status history flag.
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        fieldExtensionList.add(buildFieldExtension("includeBusinessObjectDataStatusHistory", "${includeBusinessObjectDataStatusHistory}"));
        fieldExtensionList.add(buildFieldExtension("includeStorageUnitStatusHistory", "${includeStorageUnitStatusHistory}"));
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));
        parameters.add(buildParameter("includeBusinessObjectDataStatusHistory", NO_INCLUDE_BUSINESS_OBJECT_DATA_STATUS_HISTORY.toString()));
        parameters.add(buildParameter("includeStorageUnitStatusHistory", "NOT_A_BOOLEAN"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate
            .put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"includeStorageUnitStatusHistory\" must be a valid boolean value of \"true\" or \"false\".");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(GetBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    /**
     * Constructs a random string of length 10.
     *
     * @return a random string
     */
    private String randomString()
    {
        return ("test" + Math.random()).substring(0, 10);
    }

    /**
     * Inserts a business object data and their FK relationships into the database.
     *
     * @param dataProviderName the data provider name.
     * @param businessObjectDefinitionName the business object definition name.
     * @param fileTypeCode the file type code.
     * @param businessObjectFormatUsage the business object format usage.
     * @param businessObjectFormatVersion the business object format version.
     * @param businessObjectDataVersion the business object data version.
     * @param partitionKey the partition key.
     * @param partitionValues the partition values.
     */
    private void setupDatabase(String namespace, String dataProviderName, String businessObjectDefinitionName, String fileTypeCode,
        String businessObjectFormatUsage, Integer businessObjectFormatVersion, Integer businessObjectDataVersion, String partitionKey, String[] partitionValues)
    {
        DataProviderEntity dataProviderEntity = new DataProviderEntity();
        dataProviderEntity.setName(dataProviderName);
        herdDao.saveAndRefresh(dataProviderEntity);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        herdDao.saveAndRefresh(namespaceEntity);

        BusinessObjectDefinitionEntity businessObjectDefinition = new BusinessObjectDefinitionEntity();
        businessObjectDefinition.setDataProvider(dataProviderEntity);
        businessObjectDefinition.setName(businessObjectDefinitionName);
        businessObjectDefinition.setNamespace(namespaceEntity);
        herdDao.saveAndRefresh(businessObjectDefinition);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(fileTypeCode);
        herdDao.saveAndRefresh(fileTypeEntity);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinition);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setLatestVersion(true);
        businessObjectFormatEntity.setNullValue("#");
        businessObjectFormatEntity.setPartitionKey(partitionKey);
        businessObjectFormatEntity.setUsage(businessObjectFormatUsage);
        herdDao.saveAndRefresh(businessObjectFormatEntity);

        StoragePlatformEntity storagePlatformEntity = new StoragePlatformEntity();
        storagePlatformEntity.setName(randomString());
        herdDao.saveAndRefresh(storagePlatformEntity);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(randomString());
        storageEntity.setStoragePlatform(storagePlatformEntity);
        herdDao.saveAndRefresh(storageEntity);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setLatestVersion(true);
        businessObjectDataEntity.setPartitionValue(partitionValues[0]);
        businessObjectDataEntity.setPartitionValue2(partitionValues[1]);
        businessObjectDataEntity.setPartitionValue3(partitionValues[2]);
        businessObjectDataEntity.setPartitionValue4(partitionValues[3]);
        businessObjectDataEntity.setPartitionValue5(partitionValues[4]);
        businessObjectDataEntity.setStatus(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.VALID));
        Collection<StorageUnitEntity> storageUnits = new ArrayList<>();
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        Collection<StorageFileEntity> storageFiles = new ArrayList<>();
        StorageFileEntity storageFileEntity = new StorageFileEntity();
        storageFileEntity.setPath(randomString());
        storageFileEntity.setFileSizeBytes(1000l);
        storageFileEntity.setStorageUnit(storageUnitEntity);
        storageFiles.add(storageFileEntity);
        storageUnitEntity.setStorageFiles(storageFiles);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStatus(storageUnitStatusDao.getStorageUnitStatusByCode(StorageUnitStatusEntity.ENABLED));
        storageUnits.add(storageUnitEntity);
        businessObjectDataEntity.setStorageUnits(storageUnits);
        businessObjectDataEntity.setVersion(businessObjectDataVersion);
        herdDao.saveAndRefresh(businessObjectDataEntity);
    }

    /**
     * Executes the Activiti job with the given parameters and returns variables. The parameters are as defined in the documentation.
     *
     * @param businessObjectDefinitionName the business object definition name.
     * @param businessObjectFormatUsage the business object format usage.
     * @param fileTypeCode the file type code.
     * @param partitionKey the partition key.
     * @param partitionValue the partition value.
     * @param subPartitionValues the sub-partition values.
     * @param businessObjectFormatVersion the business object format version (optional).
     * @param businessObjectDataVersion the business object data version (optional).
     *
     * @return map of variable name to variable value
     * @throws Exception
     */
    private Map<String, Object> executeJob(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage, String fileTypeCode,
        String partitionKey, String partitionValue, String subPartitionValues, String businessObjectFormatVersion, String businessObjectDataVersion)
        throws Exception
    {
        // Prepare input data
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        if (namespace != null)
        {
            fieldExtensionList.add(buildFieldExtension("namespace", "${businessObjectDefinitionNamespace}"));
        }
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("partitionKey", "${partitionKey}"));
        fieldExtensionList.add(buildFieldExtension("partitionValue", "${partitionValue}"));
        fieldExtensionList.add(buildFieldExtension("subPartitionValues", "${subPartitionValues}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));

        List<Parameter> parameters = new ArrayList<>();
        if (namespace != null)
        {
            parameters.add(buildParameter("businessObjectDefinitionNamespace", namespace));
        }
        parameters.add(buildParameter("businessObjectDefinitionName", businessObjectDefinitionName));
        parameters.add(buildParameter("businessObjectFormatUsage", businessObjectFormatUsage));
        parameters.add(buildParameter("businessObjectFormatFileType", fileTypeCode));
        parameters.add(buildParameter("partitionKey", partitionKey));
        parameters.add(buildParameter("partitionValue", partitionValue));
        parameters.add(buildParameter("subPartitionValues", subPartitionValues));
        parameters.add(buildParameter("businessObjectFormatVersion", businessObjectFormatVersion));
        parameters.add(buildParameter("businessObjectDataVersion", businessObjectDataVersion));

        String activitiXml = buildActivitiXml(IMPLEMENTATION, fieldExtensionList);

        // Execute job
        Job job = jobServiceTestHelper.createJobForCreateClusterForActivitiXml(activitiXml, parameters);
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        return hisInstance.getProcessVariables();
    }

    /**
     * Constructs a delimited sub-partition values. If a delimiter character is present in the value, the delimiter is escaped. The given partition values
     * includes both the primary partition value and the sub-partition values, that is, the given array should look like {@code [primary, subpv1, subpv2,
     * subpv3, subpv4]}.
     *
     * @param partitionValues the partition values.
     *
     * @return delimited sub-partition values
     * @throws IllegalArgumentException when given partition values' size is 1 or less.
     */
    private String buildDelimitedSubPartitionValues(String[] partitionValues)
    {
        if (partitionValues.length <= 1)
        {
            throw new IllegalArgumentException("partitionValues.length must be greater than 1");
        }

        StringBuilder subPartitionValuesBuilder = new StringBuilder();
        for (int i = 1; i < partitionValues.length; i++)
        {
            if (i > 1)
            {
                subPartitionValuesBuilder.append(DELIMITER);
            }
            String partitionValue = partitionValues[i];
            partitionValue = partitionValue.replaceAll(PATTERN_DELIMITER, DELIMITER_ESCAPE);
            subPartitionValuesBuilder.append(partitionValue);
        }
        return subPartitionValuesBuilder.toString();
    }
}
