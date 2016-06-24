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

import org.finra.herd.model.api.xml.BusinessObjectDataAttribute;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.service.BusinessObjectDataAttributeService;

/**
 * An Activiti task that retrieves a business object data attribute.
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespace" stringValue=""/>
 *   <activiti:field name="businessObjectDefinitionName" stringValue=""/>
 *   <activiti:field name="businessObjectFormatUsage" stringValue=""/>
 *   <activiti:field name="businessObjectFormatFileType" stringValue=""/>
 *   <activiti:field name="businessObjectFormatVersion" stringValue=""/>
 *   <activiti:field name="partitionValue" stringValue=""/>
 *   <activiti:field name="subPartitionValues" stringValue=""/>
 *   <activiti:field name="businessObjectDataVersion" stringValue=""/>
 *   <activiti:field name="businessObjectDataAttributeName" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class GetBusinessObjectDataAttribute extends BaseJavaDelegate
{
    private Expression namespace;
    private Expression businessObjectDefinitionName;
    private Expression businessObjectFormatUsage;
    private Expression businessObjectFormatFileType;
    private Expression businessObjectFormatVersion;
    private Expression partitionValue;
    private Expression subPartitionValues;
    private Expression businessObjectDataVersion;
    private Expression businessObjectDataAttributeName;

    @Autowired
    private BusinessObjectDataAttributeService businessObjectDataAttributeService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String namespace = activitiHelper.getExpressionVariableAsString(this.namespace, execution);
        String businessObjectDefinitionName = activitiHelper.getExpressionVariableAsString(this.businessObjectDefinitionName, execution);
        String businessObjectFormatUsage = activitiHelper.getExpressionVariableAsString(this.businessObjectFormatUsage, execution);
        String businessObjectFormatFileType = activitiHelper.getExpressionVariableAsString(this.businessObjectFormatFileType, execution);
        Integer businessObjectFormatVersion =
            activitiHelper.getExpressionVariableAsInteger(this.businessObjectFormatVersion, execution, "businessObjectFormatVersion", false);
        String partitionValue = activitiHelper.getExpressionVariableAsString(this.partitionValue, execution);
        String subPartitionValuesString = activitiHelper.getExpressionVariableAsString(this.subPartitionValues, execution);
        List<String> subPartitionValues = daoHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValuesString);
        Integer businessObjectDataVersion =
            activitiHelper.getExpressionVariableAsInteger(this.businessObjectDataVersion, execution, "businessObjectDataVersion", false);
        String businessObjectDataAttributeName = activitiHelper.getExpressionVariableAsString(this.businessObjectDataAttributeName, execution);

        BusinessObjectDataAttributeKey businessObjectDataAttributeKey = new BusinessObjectDataAttributeKey();
        businessObjectDataAttributeKey.setNamespace(namespace);
        businessObjectDataAttributeKey.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectDataAttributeKey.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectDataAttributeKey.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectDataAttributeKey.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectDataAttributeKey.setPartitionValue(partitionValue);
        businessObjectDataAttributeKey.setSubPartitionValues(subPartitionValues);
        businessObjectDataAttributeKey.setBusinessObjectDataVersion(businessObjectDataVersion);
        businessObjectDataAttributeKey.setBusinessObjectDataAttributeName(businessObjectDataAttributeName);

        BusinessObjectDataAttribute businessObjectDataAttribute =
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(businessObjectDataAttributeKey);

        setJsonResponseAsWorkflowVariable(businessObjectDataAttribute, execution);
    }
}
