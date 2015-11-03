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

import org.finra.dm.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the AddBusinessObjectDataStorageFiles Activiti task wrapper.
 */
public class AddBusinessObjectDataStorageFilesTest extends DmActivitiServiceTaskTest
{

    @Test
    public void testAddBusinessObjectDataStorageFilesXml() throws Exception
    {
        BusinessObjectDataStorageFilesCreateRequest businessObjectDataStorageFilesCreateRequest = getNewBusinessObjectDataStorageFilesCreateRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageFilesCreateRequest", "${businessObjectDataStorageFilesCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataStorageFilesCreateRequest", xmlHelper.objectToXml(businessObjectDataStorageFilesCreateRequest)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(AddBusinessObjectDataStorageFiles.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testAddBusinessObjectDataStorageFilesJson() throws Exception
    {
        BusinessObjectDataStorageFilesCreateRequest businessObjectDataStorageFilesCreateRequest = getNewBusinessObjectDataStorageFilesCreateRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageFilesCreateRequest", "${businessObjectDataStorageFilesCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataStorageFilesCreateRequest", jsonHelper.objectToJson(businessObjectDataStorageFilesCreateRequest)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(AddBusinessObjectDataStorageFiles.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testAddBusinessObjectDataStorageFilesWrongContentType() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageFilesCreateRequest", "${businessObjectDataStorageFilesCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "wrong_content_type"));
        parameters.add(buildParameter("businessObjectDataStorageFilesCreateRequest", "some_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"ContentType\" must be a valid value of either \"xml\" or \"json\".");

        testActivitiServiceTaskFailure(AddBusinessObjectDataStorageFiles.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testAddBusinessObjectDataStorageFilesNoRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageFilesCreateRequest", "${businessObjectDataStorageFilesCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataStorageFilesCreateRequest", ""));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataCreateRequest\" must be specified.");

        testActivitiServiceTaskFailure(AddBusinessObjectDataStorageFiles.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testAddBusinessObjectDataStorageFilesWrongXmlRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageFilesCreateRequest", "${businessObjectDataStorageFilesCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataStorageFilesCreateRequest", "wrong_xml_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataStorageFilesCreateRequest\" must be valid xml string.");

        testActivitiServiceTaskFailure(AddBusinessObjectDataStorageFiles.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testAddBusinessObjectDataStorageFilesWrongJsonRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataStorageFilesCreateRequest", "${businessObjectDataStorageFilesCreateRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataStorageFilesCreateRequest", "wrong_json_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataStorageFilesCreateRequest\" must be valid json string.");

        testActivitiServiceTaskFailure(AddBusinessObjectDataStorageFiles.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }
}
