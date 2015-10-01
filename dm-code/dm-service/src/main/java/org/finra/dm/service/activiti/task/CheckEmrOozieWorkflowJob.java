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
import org.springframework.stereotype.Component;

import org.finra.dm.model.api.xml.OozieWorkflowJob;
import org.finra.dm.service.EmrService;

/**
 * An Activiti task that checks the oozie workflow job status.
 * <p/>
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespaceCode" stringValue="" />
 *   <activiti:field name="emrClusterDefinitionName" stringValue="" />
 *   <activiti:field name="emrClusterName" stringValue="" />
 *   <activiti:field name="oozieWorkflowId" stringValue="" />
 *   <activiti:field name="verbose" stringValue="" />
 * </extensionElements>
 * </pre>
 */
@Component
public class CheckEmrOozieWorkflowJob extends BaseJavaDelegate
{
    public static final String VARIABLE_ID = "id";

    private Expression namespace;
    private Expression emrClusterDefinitionName;
    private Expression emrClusterName;
    private Expression oozieWorkflowJobId;
    private Expression verbose;
    
    @Autowired
    private EmrService emrService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String namespaceString = activitiHelper.getExpressionVariableAsString(namespace, execution);
        String emrClusterDefinitionNameString = activitiHelper.getExpressionVariableAsString(emrClusterDefinitionName, execution);
        String emrClusterNameString = activitiHelper.getExpressionVariableAsString(emrClusterName, execution);
        String oozieWorkflowJobIdString = activitiHelper.getExpressionVariableAsString(oozieWorkflowJobId, execution);
        
        boolean verboseBoolean = activitiHelper.getExpressionVariableAsBoolean(verbose, execution, "verbose", false, false);
        
        // Get the oozie job status.
        OozieWorkflowJob oozieWorkflowJob = 
            emrService.getEmrOozieWorkflowJob(namespaceString, emrClusterDefinitionNameString, emrClusterNameString, oozieWorkflowJobIdString, verboseBoolean);

        // Set the JSON response as a workflow variable.
        setJsonResponseAsWorkflowVariable(oozieWorkflowJob, execution);
    }
}