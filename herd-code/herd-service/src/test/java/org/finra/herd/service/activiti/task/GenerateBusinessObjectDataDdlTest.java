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

import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the GenerateBusinessObjectDataDdl Activiti task wrapper.
 *
 */
public class GenerateBusinessObjectDataDdlTest extends HerdActivitiServiceTaskTest
{
    @Test
    public void testGenerateBusinessObjectDataDdlXml() throws Exception
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataDdlRequest businessObjectDataDdlRequest = getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlRequest", "${businessObjectDataDdlRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataDdlRequest", xmlHelper.objectToXml(businessObjectDataDdlRequest)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GenerateBusinessObjectDataDdl.VARIABLE_DDL, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(GenerateBusinessObjectDataDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }
    
    @Test
    public void testGenerateBusinessObjectDataDdlJson() throws Exception
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectDataDdlTesting();

        BusinessObjectDataDdlRequest businessObjectDataDdlRequest = getTestBusinessObjectDataDdlRequest(UNSORTED_PARTITION_VALUES, CUSTOM_DDL_NAME);
        
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlRequest", "${businessObjectDataDdlRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataDdlRequest", jsonHelper.objectToJson(businessObjectDataDdlRequest)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GenerateBusinessObjectDataDdl.VARIABLE_DDL, VARIABLE_VALUE_NOT_NULL);
        
        testActivitiServiceTaskSuccess(GenerateBusinessObjectDataDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlWrongContentType() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlRequest", "${businessObjectDataDdlRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "wrong_content_type"));
        parameters.add(buildParameter("businessObjectDataDdlRequest", "some_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"ContentType\" must be a valid value of either \"xml\" or \"json\".");

        testActivitiServiceTaskFailure(GenerateBusinessObjectDataDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlNoRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlRequest", "${businessObjectDataDdlRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataDdlRequest", ""));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"businessObjectDataDdlRequest\" must be specified.");

        testActivitiServiceTaskFailure(GenerateBusinessObjectDataDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlWrongXmlRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlRequest", "${businessObjectDataDdlRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataDdlRequest", "wrong_xml_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataDdlRequest\" must be valid xml string.");

        testActivitiServiceTaskFailure(GenerateBusinessObjectDataDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testGenerateBusinessObjectDataDdlWrongJsonRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlRequest", "${businessObjectDataDdlRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataDdlRequest", "wrong_json_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataDdlRequest\" must be valid json string.");

        testActivitiServiceTaskFailure(GenerateBusinessObjectDataDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }
}