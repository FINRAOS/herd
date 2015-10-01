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
import org.finra.dm.model.api.xml.RunOozieWorkflowRequest;
import org.finra.dm.service.EmrService;

/**
 * An Activiti task that runs the oozie workflow.
 * <p/>
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="contentType" stringValue=""/>
 *   <activiti:field name="runOozieWorkflowRequest" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class RunOozieWorkflow extends BaseJavaDelegate
{
    public static final String VARIABLE_ID = "id";

    private Expression contentType;
    private Expression runOozieWorkflowRequest;

    @Autowired
    private EmrService emrService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(contentType, execution, "ContentType").trim();
        String requestString =
            activitiHelper.getRequiredExpressionVariableAsString(runOozieWorkflowRequest, execution, "RunOozieWorkflowRequest").trim();

        RunOozieWorkflowRequest request = getRequestObject(contentTypeString, requestString, RunOozieWorkflowRequest.class);

        // Run the oozie job.
        OozieWorkflowJob oozieWorkflowJob = emrService.runOozieWorkflow(request);

        setTaskWorkflowVariable(execution, VARIABLE_ID, oozieWorkflowJob.getId());
    }
}