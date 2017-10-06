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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.FieldExtension;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateResponse;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Test suite for Update Business Object Data Storage Unit Status Activiti wrapper.
 */
public class UpdateBusinessObjectDataStorageUnitStatusTest extends HerdActivitiServiceTaskTest
{
    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatus() throws Exception
    {
        // Create a business object data storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create a storage unit entity.
        storageUnitDaoTestHelper.createStorageUnitEntity(businessObjectDataStorageUnitKey, STORAGE_UNIT_STATUS);

        // Create a storage unit status entity.
        storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("partitionValue", "${partitionValue}"));
        fieldExtensionList.add(buildFieldExtension("subPartitionValues", "${subPartitionValues}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        fieldExtensionList.add(buildFieldExtension("storageName", "${storageName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageUnitStatus", "${businessObjectDataStorageUnitStatus}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("namespace", BDEF_NAMESPACE));
        parameters.add(buildParameter("businessObjectDefinitionName", BDEF_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("partitionValue", PARTITION_VALUE));
        parameters.add(buildParameter("subPartitionValues", herdStringHelper.buildStringWithDefaultDelimiter(SUBPARTITION_VALUES)));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));
        parameters.add(buildParameter("storageName", STORAGE_NAME));
        parameters.add(buildParameter("businessObjectDataStorageUnitStatus", STORAGE_UNIT_STATUS_2));

        // Build the expected response object.
        BusinessObjectDataStorageUnitStatusUpdateResponse expectedResponse =
            new BusinessObjectDataStorageUnitStatusUpdateResponse(businessObjectDataStorageUnitKey, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS);

        // Run the activiti task and validate the returned response object.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(UpdateBusinessObjectDataStorageUnitStatus.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedResponse));
        testActivitiServiceTaskSuccess(UpdateBusinessObjectDataStorageUnitStatus.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatusInvalidBusinessObjectDataVersion() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataVersion", INVALID_INTEGER_VALUE));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"businessObjectDataVersion\" must be a valid integer value.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(UpdateBusinessObjectDataStorageUnitStatus.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatusInvalidBusinessObjectFormatVersion() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", INVALID_INTEGER_VALUE));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"businessObjectFormatVersion\" must be a valid integer value.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(UpdateBusinessObjectDataStorageUnitStatus.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatusMissingBusinessObjectDataVersion() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataVersion", BLANK_TEXT));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"businessObjectDataVersion\" must be specified.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(UpdateBusinessObjectDataStorageUnitStatus.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatusMissingBusinessObjectFormatVersion() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", BLANK_TEXT));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"businessObjectFormatVersion\" must be specified.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(UpdateBusinessObjectDataStorageUnitStatus.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatusMissingOptionalParameters() throws Exception
    {
        // Create a business object data storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create a storage unit entity.
        storageUnitDaoTestHelper.createStorageUnitEntity(businessObjectDataStorageUnitKey, STORAGE_UNIT_STATUS);

        // Create a storage unit status entity.
        storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("partitionValue", "${partitionValue}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        fieldExtensionList.add(buildFieldExtension("storageName", "${storageName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageUnitStatus", "${businessObjectDataStorageUnitStatus}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("namespace", BDEF_NAMESPACE));
        parameters.add(buildParameter("businessObjectDefinitionName", BDEF_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("partitionValue", PARTITION_VALUE));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));
        parameters.add(buildParameter("storageName", STORAGE_NAME));
        parameters.add(buildParameter("businessObjectDataStorageUnitStatus", STORAGE_UNIT_STATUS_2));

        // Build the expected response object.
        BusinessObjectDataStorageUnitStatusUpdateResponse expectedResponse =
            new BusinessObjectDataStorageUnitStatusUpdateResponse(businessObjectDataStorageUnitKey, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS);

        // Run the activiti task and validate the returned response object.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(UpdateBusinessObjectDataStorageUnitStatus.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedResponse));
        testActivitiServiceTaskSuccess(UpdateBusinessObjectDataStorageUnitStatus.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatusMissingOptionalParametersSubPartitionValuesAsEmptyString() throws Exception
    {
        // Create a business object data storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create a storage unit entity.
        storageUnitDaoTestHelper.createStorageUnitEntity(businessObjectDataStorageUnitKey, STORAGE_UNIT_STATUS);

        // Create a storage unit status entity.
        storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("partitionValue", "${partitionValue}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        fieldExtensionList.add(buildFieldExtension("storageName", "${storageName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageUnitStatus", "${businessObjectDataStorageUnitStatus}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("namespace", BDEF_NAMESPACE));
        parameters.add(buildParameter("businessObjectDefinitionName", BDEF_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("partitionValue", PARTITION_VALUE));
        parameters.add(buildParameter("subPartitionValues", EMPTY_STRING));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));
        parameters.add(buildParameter("storageName", STORAGE_NAME));
        parameters.add(buildParameter("businessObjectDataStorageUnitStatus", STORAGE_UNIT_STATUS_2));

        // Build the expected response object.
        BusinessObjectDataStorageUnitStatusUpdateResponse expectedResponse =
            new BusinessObjectDataStorageUnitStatusUpdateResponse(businessObjectDataStorageUnitKey, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS);

        // Run the activiti task and validate the returned response object.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(UpdateBusinessObjectDataStorageUnitStatus.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedResponse));
        testActivitiServiceTaskSuccess(UpdateBusinessObjectDataStorageUnitStatus.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testUpdateBusinessObjectDataStorageUnitStatusMissingRequiredParameter() throws Exception
    {
        // Validate that business object data storage unit status service fails when we do not pass a namespace value.
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "A namespace must be specified.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(UpdateBusinessObjectDataStorageUnitStatus.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }
}
