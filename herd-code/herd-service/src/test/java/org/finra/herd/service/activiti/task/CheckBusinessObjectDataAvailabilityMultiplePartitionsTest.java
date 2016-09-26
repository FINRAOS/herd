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

import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the CheckBusinessObjectDataAvailabilityMultiplePartitions Activiti task wrapper.
 */
public class CheckBusinessObjectDataAvailabilityMultiplePartitionsTest extends HerdActivitiServiceTaskTest
{
    @Test
    public void testCheckBusinessObjectDataAvailabilityMultiplePartitionsXml() throws Exception
    {
        // Prepare test data.
        businessObjectDataAvailabilityTestHelper.createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Prepare a check business object data availability request.
        BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityRequest", "${businessObjectDataAvailabilityRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataAvailabilityRequest", xmlHelper.objectToXml(businessObjectDataAvailabilityRequest)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(CheckBusinessObjectDataAvailabilityMultiplePartitions.VARIABLE_IS_ALL_DATA_AVAILABLE, false);
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(CheckBusinessObjectDataAvailabilityMultiplePartitions.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityMultiplePartitionsJson() throws Exception
    {
        // Prepare test data.
        businessObjectDataAvailabilityTestHelper.createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null);

        // Prepare a check business object data availability request.
        BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest =
            businessObjectDataServiceTestHelper.getTestBusinessObjectDataAvailabilityRequest(UNSORTED_PARTITION_VALUES);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityRequest", "${businessObjectDataAvailabilityRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataAvailabilityRequest", jsonHelper.objectToJson(businessObjectDataAvailabilityRequest)));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(CheckBusinessObjectDataAvailabilityMultiplePartitions.VARIABLE_IS_ALL_DATA_AVAILABLE, false);
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(CheckBusinessObjectDataAvailabilityMultiplePartitions.class.getCanonicalName(), fieldExtensionList, parameters,
            variableValuesToValidate);
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityMultiplePartitionsWrongContentType() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityRequest", "${businessObjectDataAvailabilityRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "wrong_content_type"));
        parameters.add(buildParameter("businessObjectDataAvailabilityRequest", "some_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"ContentType\" must be a valid value of either \"xml\" or \"json\".");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(CheckBusinessObjectDataAvailabilityMultiplePartitions.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityMultiplePartitionsNoRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityRequest", "${businessObjectDataAvailabilityRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataAvailabilityRequest", ""));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataAvailabilityRequest\" must be specified.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(CheckBusinessObjectDataAvailabilityMultiplePartitions.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityMultiplePartitionsWrongXmlRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityRequest", "${businessObjectDataAvailabilityRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "xml"));
        parameters.add(buildParameter("businessObjectDataAvailabilityRequest", "wrong_xml_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataAvailabilityRequest\" must be valid xml string.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(CheckBusinessObjectDataAvailabilityMultiplePartitions.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }

    @Test
    public void testCheckBusinessObjectDataAvailabilityMultiplePartitionsWrongJsonRequest() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDataAvailabilityRequest", "${businessObjectDataAvailabilityRequest}"));

        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("contentType", "json"));
        parameters.add(buildParameter("businessObjectDataAvailabilityRequest", "wrong_json_request"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"BusinessObjectDataAvailabilityRequest\" must be valid json string.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(CheckBusinessObjectDataAvailabilityMultiplePartitions.class.getCanonicalName(), fieldExtensionList, parameters,
                variableValuesToValidate);
        });
    }
}
