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

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.service.BusinessObjectFormatService;

/**
 * An Activiti task that retrieves a business object format.
 * <p/>
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespace" stringValue=""/>
 *   <activiti:field name="businessObjectDefinitionName" stringValue=""/>
 *   <activiti:field name="businessObjectFormatUsage" stringValue=""/>
 *   <activiti:field name="businessObjectFormatFileType" stringValue=""/>
 *   <activiti:field name="businessObjectFormatVersion" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class GetBusinessObjectFormat extends BaseJavaDelegate
{
    private Expression namespace;
    private Expression businessObjectDefinitionName;
    private Expression businessObjectFormatUsage;
    private Expression businessObjectFormatFileType;
    private Expression businessObjectFormatVersion;

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

        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();
        businessObjectFormatKey.setNamespace(namespace);
        businessObjectFormatKey.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectFormatKey.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectFormatKey.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectFormatKey.setBusinessObjectFormatVersion(businessObjectFormatVersion);

        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey);

        setJsonResponseAsWorkflowVariable(businessObjectFormat, execution);
    }
}
