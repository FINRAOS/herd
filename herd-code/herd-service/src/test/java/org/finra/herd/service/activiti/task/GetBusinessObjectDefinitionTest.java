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

import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Test suite for Get Business Object Definition Activiti wrapper.
 */
public class GetBusinessObjectDefinitionTest extends HerdActivitiServiceTaskTest
{
    /**
     * This unit test passes all required and optional parameters.
     */
    @Test
    public void testGetBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes());

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("namespace", NAMESPACE_CD));
        parameters.add(buildParameter("businessObjectDefinitionName", BOD_NAME));

        // Run the activiti task.
        testActivitiServiceTaskSuccess(GetBusinessObjectDefinition.class.getCanonicalName(), fieldExtensionList, parameters, null);
    }

    /**
     * This unit test covers scenario when business object definition service fails due to a missing required parameter.
     */
    @Test
    public void testGetBusinessObjectDefinitionMissingBusinessObjectDefinitionName() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        List<Parameter> parameters = new ArrayList<>();

        // Try to get a business object definition instance when object definition name is not specified.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "A namespace must be specified.");
        testActivitiServiceTaskFailure(GetBusinessObjectDefinition.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This unit test covers scenario when business object definition service fails due to a non-existing business object definition.
     */
    @Test
    public void testGetBusinessObjectDefinitionNoExists() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("namespace", NAMESPACE_CD));
        parameters.add(buildParameter("businessObjectDefinitionName", BOD_NAME));

        // Try to get a non-existing business object definition.
        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE,
            String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BOD_NAME, NAMESPACE_CD));
        testActivitiServiceTaskFailure(GetBusinessObjectDefinition.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }
}
