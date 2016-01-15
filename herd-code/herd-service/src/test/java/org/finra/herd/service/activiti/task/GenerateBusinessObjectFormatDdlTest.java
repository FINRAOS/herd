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

import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

public class GenerateBusinessObjectFormatDdlTest extends HerdActivitiServiceTaskTest
{
    @Test
    public void testGenerateBusinessObjectFormatDdl() throws Exception
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        List<FieldExtension> fieldExtensionList = new ArrayList<>(getMandatoryFields());

        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));
        fieldExtensionList.add(buildFieldExtension("customDdlName", "${customDdlName}"));
        fieldExtensionList.add(buildFieldExtension("includeDropTableStatement", "${includeDropTableStatement}"));
        fieldExtensionList.add(buildFieldExtension("includeIfNotExistsOption", "${includeIfNotExistsOption}"));

        List<Parameter> parameters = new ArrayList<>(getMandatoryParameters());

        parameters.add(buildParameter("businessObjectFormatVersion", FORMAT_VERSION.toString()));
        parameters.add(buildParameter("customDdlName", CUSTOM_DDL_NAME));
        parameters.add(buildParameter("includeDropTableStatement", "false"));
        parameters.add(buildParameter("includeIfNotExistsOption", "false"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GenerateBusinessObjectFormatDdl.VARIABLE_DDL, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(GenerateBusinessObjectFormatDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlMissingOptional() throws Exception
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(GenerateBusinessObjectFormatDdl.VARIABLE_DDL, VARIABLE_VALUE_NOT_NULL);

        testActivitiServiceTaskSuccess(GenerateBusinessObjectFormatDdl.class.getCanonicalName(), getMandatoryFields(), getMandatoryParameters(),
            variableValuesToValidate);
    }

    /**
     * This method tests the invalid values for format version
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWithInvalidFormatVersion() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatVersion", "${businessObjectFormatVersion}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("businessObjectFormatVersion", "invalid_integer"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"businessObjectFormatVersion\" must be a valid integer value.");

        testActivitiServiceTaskFailure(GenerateBusinessObjectFormatDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This method tests the invalid values for format version
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWithInvalidIncludeDropTableStatement() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>(getMandatoryFields());
        fieldExtensionList.add(buildFieldExtension("includeDropTableStatement", "${includeDropTableStatement}"));

        List<Parameter> parameters = new ArrayList<>(getMandatoryParameters());
        parameters.add(buildParameter("includeDropTableStatement", "NOT_A_BOOLEAN"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate
            .put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"includeDropTableStatement\" must be a valid boolean value of \"true\" or \"false\".");

        testActivitiServiceTaskFailure(GenerateBusinessObjectFormatDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This method tests the invalid values for format version
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWithInvalidIncludeIfNotExistsOption() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>(getMandatoryFields());
        fieldExtensionList.add(buildFieldExtension("includeIfNotExistsOption", "${includeIfNotExistsOption}"));

        List<Parameter> parameters = new ArrayList<>(getMandatoryParameters());
        parameters.add(buildParameter("includeIfNotExistsOption", "NOT_A_BOOLEAN"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate
            .put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"includeIfNotExistsOption\" must be a valid boolean value of \"true\" or \"false\".");

        testActivitiServiceTaskFailure(GenerateBusinessObjectFormatDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * This method tests the invalid values for format version
     */
    @Test
    public void testGenerateBusinessObjectFormatDdlWithInvalidOutputFormat() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("outputFormat", "${outputFormat}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("outputFormat", "INVALID_OUTPUT_FORMAT"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate
            .put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "No enum constant org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum.INVALID_OUTPUT_FORMAT");

        testActivitiServiceTaskFailure(GenerateBusinessObjectFormatDdl.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    /**
     * Gets the mandatory fields for task
     *
     * @return List<FieldExtension>, mandatory fields
     */
    private List<FieldExtension> getMandatoryFields()
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();

        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectDefinitionName", "${businessObjectDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatUsage", "${businessObjectFormatUsage}"));
        fieldExtensionList.add(buildFieldExtension("businessObjectFormatFileType", "${businessObjectFormatFileType}"));
        fieldExtensionList.add(buildFieldExtension("outputFormat", "${outputFormat}"));
        fieldExtensionList.add(buildFieldExtension("tableName", "${tableName}"));

        return fieldExtensionList;
    }

    /**
     * Gets the mandatory parameters for task
     *
     * @return List<Parameter>, parameters
     */
    private List<Parameter> getMandatoryParameters()
    {
        List<Parameter> parameters = new ArrayList<>();

        parameters.add(buildParameter("namespace", NAMESPACE_CD));
        parameters.add(buildParameter("businessObjectDefinitionName", BOD_NAME));
        parameters.add(buildParameter("businessObjectFormatUsage", FORMAT_USAGE_CODE));
        parameters.add(buildParameter("businessObjectFormatFileType", FileTypeEntity.TXT_FILE_TYPE));
        parameters.add(buildParameter("outputFormat", BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL.value()));
        parameters.add(buildParameter("tableName", TABLE_NAME));

        return parameters;
    }
}