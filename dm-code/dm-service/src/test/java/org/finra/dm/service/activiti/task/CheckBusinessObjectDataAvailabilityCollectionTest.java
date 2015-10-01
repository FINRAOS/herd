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

import org.finra.dm.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.activiti.ActivitiHelper;

/**
 * Tests the CheckBusinessObjectDataAvailabilityCollection Activiti task wrapper.
 */
public class CheckBusinessObjectDataAvailabilityCollectionTest extends DmActivitiServiceTaskTest
{
    @Test
    public void testCheckBusinessObjectDataAvailabilityCollectionXml() throws Exception
    {
        // Prepare database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityCollectionTesting();

        // Prepare the request.
        BusinessObjectDataAvailabilityCollectionRequest request = getTestBusinessObjectDataAvailabilityCollectionRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityCollectionRequest", "${businessObjectDataAvailabilityCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataAvailabilityCollectionRequest", xmlHelper.objectToXml(request)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(CheckBusinessObjectDataAvailabilityCollection.VARIABLE_IS_ALL_DATA_AVAILABLE, true);
        variableValuesToValidate.put(CheckBusinessObjectDataAvailabilityCollection.VARIABLE_IS_ALL_DATA_NOT_AVAILABLE, false);
        variableValuesToValidate
            .put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(getExpectedBusinessObjectDataAvailabilityCollectionResponse()));

        testActivitiServiceTaskSuccess(CheckBusinessObjectDataAvailabilityCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityCollectionJson() throws Exception
    {
        // Prepare database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataAvailabilityCollectionTesting();

        // Prepare the request.
        BusinessObjectDataAvailabilityCollectionRequest request = getTestBusinessObjectDataAvailabilityCollectionRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityCollectionRequest", "${businessObjectDataAvailabilityCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataAvailabilityCollectionRequest", jsonHelper.objectToJson(request)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(CheckBusinessObjectDataAvailabilityCollection.VARIABLE_IS_ALL_DATA_AVAILABLE, true);
        variableValuesToValidate.put(CheckBusinessObjectDataAvailabilityCollection.VARIABLE_IS_ALL_DATA_NOT_AVAILABLE, false);
        variableValuesToValidate
            .put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(getExpectedBusinessObjectDataAvailabilityCollectionResponse()));

        testActivitiServiceTaskSuccess(CheckBusinessObjectDataAvailabilityCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityCollectionWrongContentType() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityCollectionRequest", "${businessObjectDataAvailabilityCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "wrong_content_type"));
        parameters.add(buildParameter("businessObjectDataAvailabilityCollectionRequest", "some_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "\"ContentType\" must be a valid value of either \"xml\" or \"json\".");

        testActivitiServiceTaskFailure(CheckBusinessObjectDataAvailabilityCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityCollectionNoRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityCollectionRequest", "${businessObjectDataAvailabilityCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataAvailabilityCollectionRequest", BLANK_TEXT));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataAvailabilityCollectionRequest\" must be specified.");

        testActivitiServiceTaskFailure(CheckBusinessObjectDataAvailabilityCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityCollectionWrongXmlRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityCollectionRequest", "${businessObjectDataAvailabilityCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataAvailabilityCollectionRequest", "wrong_xml_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataAvailabilityCollectionRequest\" must be valid xml string.");

        testActivitiServiceTaskFailure(CheckBusinessObjectDataAvailabilityCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityCollectionWrongJsonRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityCollectionRequest", "${businessObjectDataAvailabilityCollectionRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataAvailabilityCollectionRequest", "wrong_json_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataAvailabilityCollectionRequest\" must be valid json string.");

        testActivitiServiceTaskFailure(CheckBusinessObjectDataAvailabilityCollection.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }
}
