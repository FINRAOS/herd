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

import java.util.List;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.service.BusinessObjectDataService;

/**
 * An Activiti task that retrieves a business object data.
 * <p/>
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespace" stringValue=""/>
 *   <activiti:field name="businessObjectDefinitionName" stringValue=""/>
 *   <activiti:field name="businessObjectFormatUsage" stringValue=""/>
 *   <activiti:field name="businessObjectFormatFileType" stringValue=""/>
 *   <activiti:field name="partitionKey" stringValue=""/>
 *   <activiti:field name="partitionValue" stringValue=""/>
 *   <activiti:field name="subPartitionValues" stringValue=""/>
 *   <activiti:field name="businessObjectFormatVersion" stringValue=""/>
 *   <activiti:field name="businessObjectDataVersion" stringValue=""/>
 *   <activiti:field name="businessObjectDataStatus" stringValue=""/>
 *   <activiti:field name="includeBusinessObjectDataStatusHistory" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class GetBusinessObjectData extends BaseJavaDelegate
{
    private Expression namespace;

    private Expression businessObjectDefinitionName;

    private Expression businessObjectFormatUsage;

    private Expression businessObjectFormatFileType;

    private Expression partitionKey;

    private Expression partitionValue;

    private Expression subPartitionValues;

    private Expression businessObjectFormatVersion;

    private Expression businessObjectDataVersion;

    private Expression businessObjectDataStatus;

    private Expression includeBusinessObjectDataStatusHistory;

    @Autowired
    private BusinessObjectDataService businessObjectDataService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String namespace = activitiHelper.getExpressionVariableAsString(this.namespace, execution);
        String businessObjectDefinitionName = activitiHelper.getExpressionVariableAsString(this.businessObjectDefinitionName, execution);
        String businessObjectFormatUsage = activitiHelper.getExpressionVariableAsString(this.businessObjectFormatUsage, execution);
        String businessObjectFormatFileType = activitiHelper.getExpressionVariableAsString(this.businessObjectFormatFileType, execution);
        String businessObjectFormatPartitionKey = activitiHelper.getExpressionVariableAsString(this.partitionKey, execution);
        String partitionValue = activitiHelper.getExpressionVariableAsString(this.partitionValue, execution);
        String subPartitionValuesString = activitiHelper.getExpressionVariableAsString(this.subPartitionValues, execution);
        List<String> subPartitionValues = daoHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValuesString);
        Integer businessObjectFormatVersion =
            activitiHelper.getExpressionVariableAsInteger(this.businessObjectFormatVersion, execution, "businessObjectFormatVersion", false);
        Integer businessObjectDataVersion =
            activitiHelper.getExpressionVariableAsInteger(this.businessObjectDataVersion, execution, "businessObjectDataVersion", false);
        String businessObjectDataStatus = activitiHelper.getExpressionVariableAsString(this.businessObjectDataStatus, execution);
        Boolean includeBusinessObjectDataStatusHistory = activitiHelper
            .getExpressionVariableAsBoolean(this.includeBusinessObjectDataStatusHistory, execution, "includeBusinessObjectDataStatusHistory", false, false);

        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(namespace);
        businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectDataKey.setPartitionValue(partitionValue);
        businessObjectDataKey.setSubPartitionValues(subPartitionValues);
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataVersion);

        BusinessObjectData businessObjectData = businessObjectDataService
            .getBusinessObjectData(businessObjectDataKey, businessObjectFormatPartitionKey, businessObjectDataStatus, includeBusinessObjectDataStatusHistory);

        setJsonResponseAsWorkflowVariable(businessObjectData, execution);
    }
}
