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

import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the RegisterBusinessObjectData Activiti task wrapper.
 */
public class RegisterBusinessObjectDataTest extends HerdActivitiServiceTaskTest
{
    @Test
    public void testRegisterBusinessObjectDataXml() throws Exception
    {
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataCreateRequest", "${businessObjectDataCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataCreateRequest", xmlHelper.objectToXml(businessObjectDataCreateRequest)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(RegisterBusinessObjectData.VARIABLE_ID, VARIABLE_VALUE_NOT_NULL);
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(RegisterBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testRegisterBusinessObjectDataJson() throws Exception
    {
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = businessObjectDataServiceTestHelper.getNewBusinessObjectDataCreateRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataCreateRequest", "${businessObjectDataCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataCreateRequest", jsonHelper.objectToJson(businessObjectDataCreateRequest)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(RegisterBusinessObjectData.VARIABLE_ID, VARIABLE_VALUE_NOT_NULL);
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(RegisterBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testRegisterBusinessObjectDataWrongContentType() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataCreateRequest", "${businessObjectDataCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "wrong_content_type"));
        parameters.add(buildParameter("businessObjectDataCreateRequest", "some_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"ContentType\" must be a valid value of either \"xml\" or \"json\".");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(RegisterBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    @Test
    public void testRegisterBusinessObjectDataNoRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataCreateRequest", "${businessObjectDataCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataCreateRequest", ""));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataCreateRequest\" must be specified.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(RegisterBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    @Test
    public void testRegisterBusinessObjectDataWrongXmlRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataCreateRequest", "${businessObjectDataCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataCreateRequest", "wrong_xml_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataCreateRequest\" must be valid xml string.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(RegisterBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    @Test
    public void testRegisterBusinessObjectDataWrongJsonRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataCreateRequest", "${businessObjectDataCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataCreateRequest", "wrong_json_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataCreateRequest\" must be valid json string.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(RegisterBusinessObjectData.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }
}
