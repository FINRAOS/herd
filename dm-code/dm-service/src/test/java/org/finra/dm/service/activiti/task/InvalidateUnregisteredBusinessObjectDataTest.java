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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.FieldExtension;
import org.junit.Test;

import org.finra.dm.model.api.xml.BusinessObjectData;
import org.finra.dm.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.activiti.ActivitiHelper;

public class InvalidateUnregisteredBusinessObjectDataTest extends DmActivitiServiceTaskTest
{
    /**
     * Tests a standard request that is valid XML.
     * 
     * @throws Exception
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataSuccessXml() throws Exception
    {
        // The test request
        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // The expected response
        BusinessObjectDataInvalidateUnregisteredResponse expectedResponse = getExpectedBusinessObjectDataInvalidateUnregisteredResponse(request);

        // Setup format
        createBusinessObjectFormat(request);

        // Construct Activiti parameters
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataInvalidateUnregisteredRequest", "${businessObjectDataInvalidateUnregisteredRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataInvalidateUnregisteredRequest", xmlHelper.objectToXml(request)));

        /*
         * Assert that:
         * Status is SUCCESS
         * Error message is not set
         * Response is expected JSON
         */
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_STATUS, ActivitiHelper.TASK_STATUS_SUCCESS);
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, VARIABLE_VALUE_IS_NULL);
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedResponse));

        testActivitiServiceTaskSuccess(InvalidateUnregisteredBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    /**
     * Tests a standard request that is valid JSON.
     * 
     * @throws Exception
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataSuccessJson() throws Exception
    {
        // The test request
        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // The expected response
        BusinessObjectDataInvalidateUnregisteredResponse expectedResponse = getExpectedBusinessObjectDataInvalidateUnregisteredResponse(request);

        // Setup format
        createBusinessObjectFormat(request);

        // Construct Activiti parameters
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataInvalidateUnregisteredRequest", "${businessObjectDataInvalidateUnregisteredRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataInvalidateUnregisteredRequest", jsonHelper.objectToJson(request)));

        /*
         * Assert that:
         * Status is SUCCESS
         * Error message is not set
         * Response is expected JSON
         */
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_STATUS, ActivitiHelper.TASK_STATUS_SUCCESS);
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, VARIABLE_VALUE_IS_NULL);
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(expectedResponse));

        testActivitiServiceTaskSuccess(InvalidateUnregisteredBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    /**
     * Test request which results in ERROR.
     * 
     * @throws Exception
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataError() throws Exception
    {
        // The test request
        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // Construct Activiti parameters
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataInvalidateUnregisteredRequest", "${businessObjectDataInvalidateUnregisteredRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataInvalidateUnregisteredRequest", xmlHelper.objectToXml(request)));

        /*
         * Assert that:
         * Status is ERROR
         * Error message is appropriate
         * JSON response is not set
         */
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_STATUS, ActivitiHelper.TASK_STATUS_ERROR);
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "Business object format with namespace \"" + request.getNamespace()
            + "\", business object definition name \"" + request.getBusinessObjectDefinitionName() + "\", format usage \""
            + request.getBusinessObjectFormatUsage() + "\", format file type \"" + request.getBusinessObjectFormatFileType() + "\", and format version \""
            + request.getBusinessObjectFormatVersion() + "\" doesn't exist.");
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_IS_NULL);

        testActivitiServiceTaskFailure(InvalidateUnregisteredBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    private BusinessObjectDataInvalidateUnregisteredResponse getExpectedBusinessObjectDataInvalidateUnregisteredResponse(
        BusinessObjectDataInvalidateUnregisteredRequest request)
    {
        BusinessObjectDataInvalidateUnregisteredResponse expectedResponse = new BusinessObjectDataInvalidateUnregisteredResponse();
        expectedResponse.setNamespace(request.getNamespace());
        expectedResponse.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        expectedResponse.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        expectedResponse.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        expectedResponse.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());
        expectedResponse.setPartitionValue(request.getPartitionValue());
        expectedResponse.setSubPartitionValues(request.getSubPartitionValues());
        expectedResponse.setStorageName(request.getStorageName());
        expectedResponse.setRegisteredBusinessObjectDataList(new ArrayList<BusinessObjectData>());
        return expectedResponse;
    }
}
