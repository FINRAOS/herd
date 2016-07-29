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
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.activiti.bpmn.model.FieldExtension;
import org.activiti.engine.history.HistoricProcessInstance;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataAttribute;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Test suite for Create Business Object Data Attribute Activiti wrapper.
 */
public class CreateBusinessObjectDataAttributeTest extends HerdActivitiServiceTaskTest
{
    @Test
    public void testCreateBusinessObjectDataAttribute() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("partitionValue", "${partitionValue}"));
        fieldExtensionList.add(buildFieldExtension("subPartitionValues", "${subPartitionValues}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAttributeName", "${businessObjectDataAttributeName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAttributeValue", "${businessObjectDataAttributeValue}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("namespace", BDEF_NAMESPACE));
        parameters.add(buildParameter("businessObjectDefinitionName", BDEF_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("partitionValue", PARTITION_VALUE));
        parameters.add(buildParameter("subPartitionValues", herdStringHelper.buildStringWithDefaultDelimiter(SUBPARTITION_VALUES)));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataAttributeName", ATTRIBUTE_NAME_1_MIXED_CASE));
        parameters.add(buildParameter("businessObjectDataAttributeValue", ATTRIBUTE_VALUE_1));

        // Run the activiti task.
        String activitiXml = buildActivitiXml(CreateBusinessObjectDataAttribute.class.getCanonicalName(), fieldExtensionList);
        Job job = createJobFromActivitiXml(activitiXml, parameters);
        assertNotNull(job);
        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        // Validate status.
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_SUCCESS, serviceTaskStatus);

        // Retrieve JSON response.
        String jsonResponse = (String) variables.get(getServiceTaskVariableName(BaseJavaDelegate.VARIABLE_JSON_RESPONSE));
        ObjectMapper objectMapper = new ObjectMapper();
        BusinessObjectDataAttribute businessObjectDataAttribute = objectMapper.readValue(jsonResponse.getBytes(), BusinessObjectDataAttribute.class);

        // Validate JSON response.
        assertEquals(BDEF_NAMESPACE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getNamespace());
        assertEquals(BDEF_NAME, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectDefinitionName());
        assertEquals(FORMAT_USAGE_CODE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectFormatUsage());
        assertEquals(FORMAT_FILE_TYPE_CODE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectFormatFileType());
        assertEquals(FORMAT_VERSION, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectFormatVersion());
        assertEquals(PARTITION_VALUE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getPartitionValue());
        assertEquals(SUBPARTITION_VALUES, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getSubPartitionValues());
        assertEquals(DATA_VERSION, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectDataVersion());
        assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectDataAttributeName());
        assertEquals(ATTRIBUTE_VALUE_1, businessObjectDataAttribute.getBusinessObjectDataAttributeValue());
    }

    /**
     * This unit tests covers scenario when business object data attribute service fails due to a missing required parameter.
     */
    @Test
    public void testCreateBusinessObjectDataAttributeMissingRequiredParameter() throws Exception
    {
        // Validate that activiti task fails when we do not pass a namespace value.
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        List<Parameter> parameters = new ArrayList<>();

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "A namespace must be specified.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(CreateBusinessObjectDataAttribute.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testCreateBusinessObjectDataAttributeMissingOptionalParameters() throws Exception
    {
        // Create a business object data key without sub-partition values.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a business object data entity.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("partitionValue", "${partitionValue}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAttributeName", "${businessObjectDataAttributeName}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("namespace", BDEF_NAMESPACE));
        parameters.add(buildParameter("businessObjectDefinitionName", BDEF_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("partitionValue", PARTITION_VALUE));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataAttributeName", ATTRIBUTE_NAME_1_MIXED_CASE));

        // Run the activiti task.
        String activitiXml = buildActivitiXml(CreateBusinessObjectDataAttribute.class.getCanonicalName(), fieldExtensionList);
        Job job = createJobFromActivitiXml(activitiXml, parameters);
        assertNotNull(job);
        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        // Validate status.
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_SUCCESS, serviceTaskStatus);

        // Retrieve JSON response.
        String jsonResponse = (String) variables.get(getServiceTaskVariableName(BaseJavaDelegate.VARIABLE_JSON_RESPONSE));
        ObjectMapper objectMapper = new ObjectMapper();
        BusinessObjectDataAttribute businessObjectDataAttribute = objectMapper.readValue(jsonResponse.getBytes(), BusinessObjectDataAttribute.class);

        // Validate JSON response.
        assertEquals(BDEF_NAMESPACE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getNamespace());
        assertEquals(BDEF_NAME, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectDefinitionName());
        assertEquals(FORMAT_USAGE_CODE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectFormatUsage());
        assertEquals(FORMAT_FILE_TYPE_CODE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectFormatFileType());
        assertEquals(FORMAT_VERSION, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectFormatVersion());
        assertEquals(PARTITION_VALUE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getPartitionValue());
        assertEquals(NO_SUBPARTITION_VALUES, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getSubPartitionValues());
        assertEquals(DATA_VERSION, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectDataVersion());
        assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectDataAttributeName());
        assertNull(businessObjectDataAttribute.getBusinessObjectDataAttributeValue());
    }

    @Test
    public void testCreateBusinessObjectDataAttributeMissingOptionalParametersSubPartitionValuesAsEmptyString() throws Exception
    {
        // Create a business object data key without sub-partition values.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a business object data entity.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("partitionValue", "${partitionValue}"));
        fieldExtensionList.add(buildFieldExtension("subPartitionValues", "${subPartitionValues}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAttributeName", "${businessObjectDataAttributeName}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("namespace", BDEF_NAMESPACE));
        parameters.add(buildParameter("businessObjectDefinitionName", BDEF_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("partitionValue", PARTITION_VALUE));
        parameters.add(buildParameter("subPartitionValues", EMPTY_STRING));
        parameters.add(buildParameter("businessObjectDataVersion", DATA_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataAttributeName", ATTRIBUTE_NAME_1_MIXED_CASE));

        // Run the activiti task.
        String activitiXml = buildActivitiXml(CreateBusinessObjectDataAttribute.class.getCanonicalName(), fieldExtensionList);
        Job job = createJobFromActivitiXml(activitiXml, parameters);
        assertNotNull(job);
        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        // Validate status.
        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_SUCCESS, serviceTaskStatus);

        // Retrieve JSON response.
        String jsonResponse = (String) variables.get(getServiceTaskVariableName(BaseJavaDelegate.VARIABLE_JSON_RESPONSE));
        ObjectMapper objectMapper = new ObjectMapper();
        BusinessObjectDataAttribute businessObjectDataAttribute = objectMapper.readValue(jsonResponse.getBytes(), BusinessObjectDataAttribute.class);

        // Validate JSON response.
        assertEquals(BDEF_NAMESPACE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getNamespace());
        assertEquals(BDEF_NAME, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectDefinitionName());
        assertEquals(FORMAT_USAGE_CODE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectFormatUsage());
        assertEquals(FORMAT_FILE_TYPE_CODE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectFormatFileType());
        assertEquals(FORMAT_VERSION, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectFormatVersion());
        assertEquals(PARTITION_VALUE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getPartitionValue());
        assertEquals(NO_SUBPARTITION_VALUES, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getSubPartitionValues());
        assertEquals(DATA_VERSION, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectDataVersion());
        assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, businessObjectDataAttribute.getBusinessObjectDataAttributeKey().getBusinessObjectDataAttributeName());
        assertNull(businessObjectDataAttribute.getBusinessObjectDataAttributeValue());
    }

    /**
     * This unit tests validates that activiti service layer fails when we pass non-integer business object format version value.
     */
    @Test
    public void testCreateBusinessObjectDataAttributeInvalidBusinessObjectFormatVersion() throws Exception
    {
        // Validate that activiti task fails when we pass non-integer value for a business object format version.
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", "NOT_AN_INTEGER"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"businessObjectFormatVersion\" must be a valid integer value.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(CreateBusinessObjectDataAttribute.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    /**
     * This unit tests validates that activiti service layer fails when we pass non-integer business object data version value.
     */
    @Test
    public void testCreateBusinessObjectDataAttributeInvalidBusinessObjectDataVersion() throws Exception
    {
        // Validate that activiti task fails when we pass non-integer value for a business object data version.
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataVersion", "${businessObjectDataVersion}"));
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("businessObjectDataVersion", "NOT_AN_INTEGER"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"businessObjectDataVersion\" must be a valid integer value.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(CreateBusinessObjectDataAttribute.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }
}
