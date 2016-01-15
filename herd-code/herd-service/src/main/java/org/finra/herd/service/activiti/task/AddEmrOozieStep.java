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
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.EmrOozieStepAddRequest;

/**
 * An Activiti task that adds oozie step to EMR cluster
 * <p/>
 * 
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespaceCode" stringValue="" />
 *   <activiti:field name="emrClusterDefinitionName" stringValue="" />
 *   <activiti:field name="emrClusterName" stringValue="" />
 *   <activiti:field name="stepName" stringValue="" />
 *   <activiti:field name="workflowXmlLocation" stringValue="" />
 *   <activiti:field name="ooziePropertiesFileLocation" stringValue="" />
 *   <activiti:field name="continueOnError" stringValue="" />
 * </extensionElements>
 * </pre>
 */
@Component
public class AddEmrOozieStep extends BaseAddEmrStep
{
    protected Expression workflowXmlLocation;
    protected Expression ooziePropertiesFileLocation;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        // Create the request.
        EmrOozieStepAddRequest request = new EmrOozieStepAddRequest();
        
        populateCommonParams(request, execution);
        
        request.setWorkflowXmlLocation(getWorkflowXmlLocation(execution));
        request.setOoziePropertiesFileLocation(getOoziePropertiesFileLocation(execution));

        addEmrStepAndSetWorkflowVariables(request, execution);
    }
    
    private String getWorkflowXmlLocation(DelegateExecution execution)
    {
        return activitiHelper.getExpressionVariableAsString(workflowXmlLocation, execution);
    }

    private String getOoziePropertiesFileLocation(DelegateExecution execution)
    {
        return activitiHelper.getExpressionVariableAsString(ooziePropertiesFileLocation, execution);
    }
}