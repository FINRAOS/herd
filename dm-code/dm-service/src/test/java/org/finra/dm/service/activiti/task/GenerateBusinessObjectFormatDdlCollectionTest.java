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

import org.finra.dm.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.activiti.ActivitiHelper;

/**
 * Tests the GenerateBusinessObjectFormatDdlCollection Activiti task wrapper.
 */
public class GenerateBusinessObjectFormatDdlCollectionTest extends DmActivitiServiceTaskTest
{
    @Test
    public void testGenerateBusinessObjectFormatDdlCollectionXml() throws Exception
    {
        // Prepare database entities required for testing.
        createDatabaseEntitiesForBusinessObjectFormatDdlCollectionTesting();

        // Prepare the request.
        BusinessObjectFormatDdlCollectionRequest request = getTestBusinessObjectFormatDdlCollectionRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatDdlCollectionRequest", "${businessObjectFormatDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectFormatDdlCollectionRequest", xmlHelper.objectToXml(request)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate
            .put(GenerateBusinessObjectFormatDdlCollection.VARIABLE_DDL_COLLECTION, getExpectedBusinessObjectFormatDdlCollectionResponse().getDdlCollection());

        testActivitiServiceTaskSuccess(GenerateBusinessObjectFormatDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlCollectionJson() throws Exception
    {
        // Prepare database entities required for testing.
        createDatabaseEntitiesForBusinessObjectFormatDdlCollectionTesting();

        // Prepare the request.
        BusinessObjectFormatDdlCollectionRequest request = getTestBusinessObjectFormatDdlCollectionRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatDdlCollectionRequest", "${businessObjectFormatDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectFormatDdlCollectionRequest", jsonHelper.objectToJson(request)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate
            .put(GenerateBusinessObjectFormatDdlCollection.VARIABLE_DDL_COLLECTION, getExpectedBusinessObjectFormatDdlCollectionResponse().getDdlCollection());

        testActivitiServiceTaskSuccess(GenerateBusinessObjectFormatDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlCollectionWrongContentType() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatDdlCollectionRequest", "${businessObjectFormatDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "wrong_content_type"));
        parameters.add(buildParameter("businessObjectFormatDdlCollectionRequest", "some_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "\"ContentType\" must be a valid value of either \"xml\" or \"json\".");

        testActivitiServiceTaskFailure(GenerateBusinessObjectFormatDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlCollectionNoRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatDdlCollectionRequest", "${businessObjectFormatDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectFormatDdlCollectionRequest", BLANK_TEXT));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectFormatDdlCollectionRequest\" must be specified.");

        testActivitiServiceTaskFailure(GenerateBusinessObjectFormatDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlCollectionWrongXmlRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatDdlCollectionRequest", "${businessObjectFormatDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectFormatDdlCollectionRequest", "wrong_xml_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectFormatDdlCollectionRequest\" must be valid xml string.");

        testActivitiServiceTaskFailure(GenerateBusinessObjectFormatDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlCollectionWrongJsonRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatDdlCollectionRequest", "${businessObjectFormatDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectFormatDdlCollectionRequest", "wrong_json_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectFormatDdlCollectionRequest\" must be valid json string.");

        testActivitiServiceTaskFailure(GenerateBusinessObjectFormatDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }
}
