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

import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;

/**
 * An Activiti task that gets the EMR cluster details
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespaceCode" stringValue="" />
 *   <activiti:field name="emrClusterDefinitionName" stringValue="" />
 *   <activiti:field name="emrClusterName" stringValue="" />
 *   <activiti:field name="emrClusterId" stringValue="" />
 *   <activiti:field name="emrStepId" stringValue="" />
 *   <activiti:field name="verbose" stringValue="" />
 *   <activiti:field name="retrieveOozieJobs" stringValue="" />
 * </extensionElements>
 * </pre>
 */
@Component
public class CheckEmrCluster extends BaseEmrCluster
{
    private Expression emrClusterId;

    private Expression emrStepId;

    private Expression verbose;

    private Expression retrieveOozieJobs;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = getClusterAlternateKey(execution);

        String emrStepIdString = activitiHelper.getExpressionVariableAsString(emrStepId, execution);
        String emrClusterIdString = activitiHelper.getExpressionVariableAsString(emrClusterId, execution);
        boolean verboseBoolean = activitiHelper.getExpressionVariableAsBoolean(verbose, execution, "verbose", false, false);
        boolean retrieveOozieJobsBoolean = activitiHelper.getExpressionVariableAsBoolean(retrieveOozieJobs, execution, "retrieveOozieJobs", false, false);

        // Gets the EMR cluster details.
        EmrCluster emrCluster = emrService.getCluster(emrClusterAlternateKeyDto, emrClusterIdString, emrStepIdString, verboseBoolean, retrieveOozieJobsBoolean);

        // Set cluster id and status workflow variables based on the result EMR cluster.
        setIdStatusWorkflowVariables(execution, emrCluster);

        // Set the active step details in workflow variables
        if (emrCluster.getActiveStep() != null && emrCluster.getActiveStep().getId() != null)
        {
            setTaskWorkflowVariable(execution, "activeStep_id", emrCluster.getActiveStep().getId());
            setTaskWorkflowVariable(execution, "activeStep_stepName", emrCluster.getActiveStep().getStepName());
            setTaskWorkflowVariable(execution, "activeStep_status", emrCluster.getActiveStep().getStatus());
            if (verboseBoolean)
            {
                setTaskWorkflowVariable(execution, "activeStep_jarLocation", emrCluster.getActiveStep().getJarLocation());
                setTaskWorkflowVariable(execution, "activeStep_mainClass", emrCluster.getActiveStep().getMainClass());
                setTaskWorkflowVariable(execution, "activeStep_scriptArguments",
                    herdStringHelper.buildStringWithDefaultDelimiter(emrCluster.getActiveStep().getScriptArguments()));
                setTaskWorkflowVariable(execution, "activeStep_continueOnError", emrCluster.getActiveStep().getContinueOnError());
            }
        }

        // Set the requested step details in workflow variables
        if (emrCluster.getStep() != null && emrCluster.getStep().getId() != null)
        {
            setTaskWorkflowVariable(execution, "step_id", emrCluster.getStep().getId());
            setTaskWorkflowVariable(execution, "step_stepName", emrCluster.getStep().getStepName());
            setTaskWorkflowVariable(execution, "step_status", emrCluster.getStep().getStatus());
            if (verboseBoolean)
            {
                setTaskWorkflowVariable(execution, "step_jarLocation", emrCluster.getStep().getJarLocation());
                setTaskWorkflowVariable(execution, "step_mainClass", emrCluster.getStep().getMainClass());
                setTaskWorkflowVariable(execution, "step_scriptArguments",
                    herdStringHelper.buildStringWithDefaultDelimiter(emrCluster.getStep().getScriptArguments()));
                setTaskWorkflowVariable(execution, "step_continueOnError", emrCluster.getStep().getContinueOnError());
            }
        }

        // Set the oozie workflows in response
        if (retrieveOozieJobsBoolean)
        {
            setTaskWorkflowVariable(execution, "oozie_workflow_jobs", jsonHelper.objectToJson(emrCluster.getOozieWorkflowJobs()));
        }
    }
}