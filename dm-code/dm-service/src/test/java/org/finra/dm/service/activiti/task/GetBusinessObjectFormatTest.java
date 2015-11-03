/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
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

import org.finra.dm.model.api.xml.BusinessObjectFormat;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.activiti.ActivitiRuntimeHelper;

/**
 * Test suite for Get Business Object Format Activiti wrapper.
 */
public class GetBusinessObjectFormatTest extends DmActivitiServiceTaskTest
{
    /**
     * This unit test passes all required and optional parameters.
     */
    @Test
    public void testGetBusinessObjectFormat() throws Exception
    {
        // Create and persist a business object format.
        BusinessObjectFormat businessObjectFormat = createTestBusinessObjectFormat();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("namespace", NAMESPACE_CD));
        parameters.add(buildParameter("businessObjectDefinitionName", BOD_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("businessObjectFormatVersion", INITIAL_FORMAT_VERSION.toString()));

        // Retrieve the business object format and validate the returned object.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetBusinessObjectFormat.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(businessObjectFormat));
        testActivitiServiceTaskSuccess(GetBusinessObjectFormat.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test does not pass optional parameters.
     */
    @Test
    public void testGetBusinessObjectFormatMissingOptionalParameters() throws Exception
    {
        // Create and persist a business object format.
        BusinessObjectFormat businessObjectFormat = createTestBusinessObjectFormat();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("namespace", NAMESPACE_CD));
        parameters.add(buildParameter("businessObjectDefinitionName", BOD_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));

        // Retrieve the business object format without specifying the business object format value.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GetBusinessObjectFormat.VARIABLE_JSON_RESPONSE, jsonHelper.objectToJson(businessObjectFormat));
        testActivitiServiceTaskSuccess(GetBusinessObjectFormat.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test covers scenario when business object format service fails due to a missing required parameter.
     */
    @Test
    public void testGetBusinessObjectFormatMissingBusinessObjectDefinitionName() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("namespace", NAMESPACE_CD));

        // Try to get a business object format instance when business object definition name is not specified.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "A business object definition name must be specified.");
        testActivitiServiceTaskFailure(GetBusinessObjectFormat.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This method tests the invalid values for business object format version.
     */
    @Test
    public void testGetBusinessObjectFormatInvalidFormatVersion() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", INVALID_INTEGER_VALUE));

        // Try to get a business object format instance when business object format version is not an integer.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"businessObjectFormatVersion\" must be a valid integer value.");
        testActivitiServiceTaskFailure(GetBusinessObjectFormat.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test covers scenario when business object format service fails due to a non-existing business object format.
     */
    @Test
    public void testGetBusinessObjectFormatNoExists() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("namespace", NAMESPACE_CD));
        parameters.add(buildParameter("businessObjectDefinitionName", BOD_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FORMAT_FILE_TYPE_CODE));
        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));

        // Try to get a non-existing business object format.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, String
            .format(getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION)));
        testActivitiServiceTaskFailure(GetBusinessObjectFormat.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }
}
