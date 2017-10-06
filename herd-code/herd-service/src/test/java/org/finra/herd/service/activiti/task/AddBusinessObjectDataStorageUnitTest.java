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

import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitCreateRequest;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the AddBusinessObjectDataStorageUnit Activiti task wrapper.
 */
public class AddBusinessObjectDataStorageUnitTest extends HerdActivitiServiceTaskTest
{
    @Test
    public void testAddBusinessObjectDataStorageUnitInvalidContentType() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageUnitCreateRequest", "${businessObjectDataStorageUnitCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "wrong_content_type"));
        parameters.add(buildParameter("businessObjectDataStorageUnitCreateRequest", "some_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"ContentType\" must be a valid value of either \"xml\" or \"json\".");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(AddBusinessObjectDataStorageUnit.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    @Test
    public void testAddBusinessObjectDataStorageUnitInvalidJsonRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageUnitCreateRequest", "${businessObjectDataStorageUnitCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataStorageUnitCreateRequest", "wrong_json_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataStorageUnitCreateRequest\" must be valid json string.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(AddBusinessObjectDataStorageUnit.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    @Test
    public void testAddBusinessObjectDataStorageUnitInvalidXmlRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageUnitCreateRequest", "${businessObjectDataStorageUnitCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataStorageUnitCreateRequest", "wrong_xml_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataStorageUnitCreateRequest\" must be valid xml string.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(AddBusinessObjectDataStorageUnit.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    @Test
    public void testAddBusinessObjectDataStorageUnitMissingRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageUnitCreateRequest", "${businessObjectDataStorageUnitCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataStorageUnitCreateRequest", ""));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataStorageUnitCreateRequest\" must be specified.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(AddBusinessObjectDataStorageUnit.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    @Test
    public void testAddBusinessObjectDataStorageUnitUsingJsonRequest() throws Exception
    {
        BusinessObjectDataStorageUnitCreateRequest businessObjectDataStorageUnitCreateRequest =
            businessObjectDataServiceTestHelper.getBusinessObjectDataStorageUnitCreateRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageUnitCreateRequest", "${businessObjectDataStorageUnitCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataStorageUnitCreateRequest", jsonHelper.objectToJson(businessObjectDataStorageUnitCreateRequest)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(AddBusinessObjectDataStorageUnit.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testAddBusinessObjectDataStorageUnitUsingXmlRequest() throws Exception
    {
        BusinessObjectDataStorageUnitCreateRequest businessObjectDataStorageUnitCreateRequest =
            businessObjectDataServiceTestHelper.getBusinessObjectDataStorageUnitCreateRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageUnitCreateRequest", "${businessObjectDataStorageUnitCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataStorageUnitCreateRequest", xmlHelper.objectToXml(businessObjectDataStorageUnitCreateRequest)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(AddBusinessObjectDataStorageUnit.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }
}
