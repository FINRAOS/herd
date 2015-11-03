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
package org.finra.dm.service.activiti.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.activiti.bpmn.model.FieldExtension;
import org.activiti.engine.history.HistoricProcessInstance;
import org.junit.Test;

import org.finra.dm.model.api.xml.BusinessObjectData;
import org.finra.dm.model.api.xml.Job;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.DataProviderEntity;
import org.finra.dm.model.jpa.FileTypeEntity;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StorageFileEntity;
import org.finra.dm.model.jpa.StoragePlatformEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;
import org.finra.dm.service.activiti.ActivitiRuntimeHelper;

/**
 * <p> Test suite for Get Business Object Data Activiti wrapper. </p>
 * <p/>
 * Test plan: <ol> <li>Insert test business object data into database, where the inserted data is configurable per test case.</li> <li>Execute Activiti job
 * using configurable input parameters</li> <li>Validate response by converting JSON into response object upon success, or verifying ERROR status on
 * exceptions.</li> </ol>
 */
public class GetBusinessObjectDataTest extends DmActivitiServiceTaskTest
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
    public void test_WithSubPartitions_NoFormatVersion_NoDataVersionLegacy() throws Exception
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
            businessObjectDataVersion, partitionKey, partitionValues, true);

        String subPartitionValues = buildDelimitedSubPartitionValues(partitionValues);
        String partitionValue = partitionValues[0];

        Map<String, Object> variables =
            executeJob(null, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValue, subPartitionValues, null,
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

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValues[0], null,
                businessObjectFormatVersion.toString(), "INVALID");

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
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

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValues[0], null, "INVALID",
                businessObjectDataVersion.toString());

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
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

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValues[0], null, null, null);

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
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

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, null, partitionKey, partitionValues[0], null, null, null);

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
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

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, null, fileTypeCode, partitionKey, partitionValues[0], null, null, null);

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
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

        Map<String, Object> variables =
            executeJob(namespace, null, businessObjectFormatUsage, fileTypeCode, partitionKey, partitionValues[0], null, null, null);

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
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

        Map<String, Object> variables =
            executeJob(namespace, businessObjectDefinitionName, businessObjectFormatUsage, fileTypeCode, partitionKey, "INVALID", null, null, null);

        // Validate status
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);
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
     * @param isLegacyBDefinition Flag for legacy business object definitions.
     */
    private void setupDatabase(String namespace, String dataProviderName, String businessObjectDefinitionName, String fileTypeCode,
        String businessObjectFormatUsage, Integer businessObjectFormatVersion, Integer businessObjectDataVersion, String partitionKey, String[] partitionValues,
        boolean isLegacyBDefinition)
    {
        DataProviderEntity dataProviderEntity = new DataProviderEntity();
        dataProviderEntity.setName(dataProviderName);
        dmDao.saveAndRefresh(dataProviderEntity);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(namespace);
        dmDao.saveAndRefresh(namespaceEntity);

        BusinessObjectDefinitionEntity businessObjectDefinition = new BusinessObjectDefinitionEntity();
        businessObjectDefinition.setDataProvider(dataProviderEntity);
        businessObjectDefinition.setName(businessObjectDefinitionName);
        businessObjectDefinition.setNamespace(namespaceEntity);
        if (isLegacyBDefinition)
        {
            businessObjectDefinition.setLegacy(true);
        }
        dmDao.saveAndRefresh(businessObjectDefinition);

        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(fileTypeCode);
        dmDao.saveAndRefresh(fileTypeEntity);

        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinition);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setLatestVersion(true);
        businessObjectFormatEntity.setNullValue("#");
        businessObjectFormatEntity.setPartitionKey(partitionKey);
        businessObjectFormatEntity.setUsage(businessObjectFormatUsage);
        dmDao.saveAndRefresh(businessObjectFormatEntity);

        StoragePlatformEntity storagePlatformEntity = new StoragePlatformEntity();
        storagePlatformEntity.setName(randomString());
        dmDao.saveAndRefresh(storagePlatformEntity);

        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(randomString());
        storageEntity.setStoragePlatform(storagePlatformEntity);
        dmDao.saveAndRefresh(storageEntity);

        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);

        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setLatestVersion(true);
        businessObjectDataEntity.setPartitionValue(partitionValues[0]);
        businessObjectDataEntity.setPartitionValue2(partitionValues[1]);
        businessObjectDataEntity.setPartitionValue3(partitionValues[2]);
        businessObjectDataEntity.setPartitionValue4(partitionValues[3]);
        businessObjectDataEntity.setPartitionValue5(partitionValues[4]);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
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
        storageUnits.add(storageUnitEntity);
        businessObjectDataEntity.setStorageUnits(storageUnits);
        businessObjectDataEntity.setVersion(businessObjectDataVersion);
        dmDao.saveAndRefresh(businessObjectDataEntity);
    }

    private void setupDatabase(String namespace, String dataProviderName, String businessObjectDefinitionName, String fileTypeCode,
        String businessObjectFormatUsage, Integer businessObjectFormatVersion, Integer businessObjectDataVersion, String partitionKey, String[] partitionValues)
    {
        setupDatabase(namespace, dataProviderName, businessObjectDefinitionName, fileTypeCode, businessObjectFormatUsage, businessObjectFormatVersion,
            businessObjectDataVersion, partitionKey, partitionValues, false);
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
        Job job = createJobForCreateClusterForActivitiXml(activitiXml, parameters);
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
