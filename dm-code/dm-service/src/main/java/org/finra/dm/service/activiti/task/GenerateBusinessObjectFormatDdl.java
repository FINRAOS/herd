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

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.dm.model.api.xml.BusinessObjectFormatDdl;
import org.finra.dm.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.dm.service.BusinessObjectFormatService;

/**
 * An Activiti task that generates the business object format DDL.
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespace" stringValue=""/>
 *   <activiti:field name="businessObjectDefinitionName" stringValue=""/>
 *   <activiti:field name="businessObjectFormatUsage" stringValue=""/>
 *   <activiti:field name="businessObjectFormatFileType" stringValue=""/>
 *   <activiti:field name="businessObjectFormatVersion" stringValue=""/>
 *   <activiti:field name="outputFormat" stringValue=""/>
 *   <activiti:field name="tableName" stringValue=""/>
 *   <activiti:field name="customDdlName" stringValue=""/>
 *   <activiti:field name="includeDropTableStatement" stringValue=""/>
 *   <activiti:field name="includeIfNotExistsOption" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
public class GenerateBusinessObjectFormatDdl extends BaseJavaDelegate
{
    public static final String VARIABLE_DDL = "businessObjectFormatDdl";

    private Expression namespace;
    private Expression businessObjectDefinitionName;
    private Expression businessObjectFormatUsage;
    private Expression businessObjectFormatFileType;
    private Expression businessObjectFormatVersion;
    private Expression outputFormat;
    private Expression tableName;
    private Expression customDdlName;
    private Expression includeDropTableStatement;
    private Expression includeIfNotExistsOption;
    private Expression replaceColumns;

    @Autowired
    private BusinessObjectFormatService businessObjectFormatService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String namespace = activitiHelper.getExpressionVariableAsString(this.namespace, execution);
        String businessObjectDefinitionName = activitiHelper.getExpressionVariableAsString(this.businessObjectDefinitionName, execution);
        String businessObjectFormatUsage = activitiHelper.getExpressionVariableAsString(this.businessObjectFormatUsage, execution);
        String businessObjectFormatFileType = activitiHelper.getExpressionVariableAsString(this.businessObjectFormatFileType, execution);
        Integer businessObjectFormatVersion =
            activitiHelper.getExpressionVariableAsInteger(this.businessObjectFormatVersion, execution, "businessObjectFormatVersion", false);

        String outputFormat = activitiHelper.getRequiredExpressionVariableAsString(this.outputFormat, execution, "outputFormat");
        BusinessObjectDataDdlOutputFormatEnum outputFormatEnum = BusinessObjectDataDdlOutputFormatEnum.fromValue(outputFormat);

        String tableName = activitiHelper.getExpressionVariableAsString(this.tableName, execution);
        String customDdlName = activitiHelper.getExpressionVariableAsString(this.customDdlName, execution);
        Boolean includeDropTableStatement = 
            activitiHelper.getExpressionVariableAsBoolean(this.includeDropTableStatement, execution, "includeDropTableStatement", false, false);
        Boolean includeIfNotExistsOption = 
                activitiHelper.getExpressionVariableAsBoolean(this.includeIfNotExistsOption, execution, "includeIfNotExistsOption", false, false);
        Boolean replaceColumns = activitiHelper.getExpressionVariableAsBoolean(this.replaceColumns, execution, "replaceColumns", false, false);

        BusinessObjectFormatDdlRequest businessObjectFormatDdlRequest = new BusinessObjectFormatDdlRequest();
        businessObjectFormatDdlRequest.setNamespace(namespace);
        businessObjectFormatDdlRequest.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectFormatDdlRequest.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectFormatDdlRequest.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectFormatDdlRequest.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectFormatDdlRequest.setOutputFormat(outputFormatEnum);
        businessObjectFormatDdlRequest.setTableName(tableName);
        businessObjectFormatDdlRequest.setCustomDdlName(customDdlName);
        businessObjectFormatDdlRequest.setIncludeDropTableStatement(includeDropTableStatement);
        businessObjectFormatDdlRequest.setIncludeIfNotExistsOption(includeIfNotExistsOption);
        businessObjectFormatDdlRequest.setReplaceColumns(replaceColumns);

        BusinessObjectFormatDdl businessObjectFormatDdl =
            businessObjectFormatService.generateBusinessObjectFormatDdl(businessObjectFormatDdlRequest);

        setTaskWorkflowVariable(execution, VARIABLE_DDL, businessObjectFormatDdl.getDdl());
    }
}
