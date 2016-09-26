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

import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionRequest;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the GenerateBusinessObjectDataDdlCollection Activiti task wrapper.
 */
public class GenerateBusinessObjectDataDdlCollectionTest extends HerdActivitiServiceTaskTest
{
    @Test
    public void testGenerateBusinessObjectDataDdlCollectionXml() throws Exception
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(AbstractServiceTest.PARTITION_VALUE);

        // Prepare the request.
        BusinessObjectDataDdlCollectionRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlCollectionRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlCollectionRequest", "${businessObjectDataDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataDdlCollectionRequest", xmlHelper.objectToXml(request)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GenerateBusinessObjectDataDdlCollection.VARIABLE_DDL_COLLECTION,
            businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdlCollectionResponse().getDdlCollection());

        executeWithoutLogging(LogVariables.class, () -> {
            testActivitiServiceTaskSuccess(GenerateBusinessObjectDataDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCollectionJson() throws Exception
    {
        // Prepare database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataDdlTesting(AbstractServiceTest.PARTITION_VALUE);

        // Prepare the request.
        BusinessObjectDataDdlCollectionRequest request = businessObjectDataServiceTestHelper.getTestBusinessObjectDataDdlCollectionRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlCollectionRequest", "${businessObjectDataDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataDdlCollectionRequest", jsonHelper.objectToJson(request)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GenerateBusinessObjectDataDdlCollection.VARIABLE_DDL_COLLECTION,
            businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataDdlCollectionResponse().getDdlCollection());

        executeWithoutLogging(LogVariables.class, () -> {
            testActivitiServiceTaskSuccess(GenerateBusinessObjectDataDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCollectionWrongContentType() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlCollectionRequest", "${businessObjectDataDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "wrong_content_type"));
        parameters.add(buildParameter("businessObjectDataDdlCollectionRequest", "some_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"ContentType\" must be a valid value of either \"xml\" or \"json\".");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(GenerateBusinessObjectDataDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCollectionNoRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlCollectionRequest", "${businessObjectDataDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataDdlCollectionRequest", BLANK_TEXT));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataDdlCollectionRequest\" must be specified.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(GenerateBusinessObjectDataDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCollectionWrongXmlRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlCollectionRequest", "${businessObjectDataDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataDdlCollectionRequest", "wrong_xml_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataDdlCollectionRequest\" must be valid xml string.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(GenerateBusinessObjectDataDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testGenerateBusinessObjectDataDdlCollectionWrongJsonRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataDdlCollectionRequest", "${businessObjectDataDdlCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataDdlCollectionRequest", "wrong_json_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataDdlCollectionRequest\" must be valid json string.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(GenerateBusinessObjectDataDdlCollection.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }
}
