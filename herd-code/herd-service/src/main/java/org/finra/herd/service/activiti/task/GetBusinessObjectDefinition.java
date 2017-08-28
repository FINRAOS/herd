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

import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.service.BusinessObjectDefinitionService;

/**
 * An Activiti task that retrieves a business object definition.
 * <p/>
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespace" stringValue=""/>
 *   <activiti:field name="businessObjectDefinitionName" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class GetBusinessObjectDefinition extends BaseJavaDelegate
{
    private Expression namespace;
    private Expression businessObjectDefinitionName;

    @Autowired
    private BusinessObjectDefinitionService businessObjectDefinitionService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String namespace = activitiHelper.getExpressionVariableAsString(this.namespace, execution);
        String businessObjectDefinitionName = activitiHelper.getExpressionVariableAsString(this.businessObjectDefinitionName, execution);

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey();
        businessObjectDefinitionKey.setNamespace(namespace);
        businessObjectDefinitionKey.setBusinessObjectDefinitionName(businessObjectDefinitionName);

        BusinessObjectDefinition businessObjectDefinition = businessObjectDefinitionService.getBusinessObjectDefinition(businessObjectDefinitionKey, false);

        setJsonResponseAsWorkflowVariable(businessObjectDefinition, execution);
    }
}
