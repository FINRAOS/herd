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

import org.finra.herd.service.EmrService;
import org.finra.herd.service.helper.EmrStepHelper;
import org.finra.herd.service.helper.EmrStepHelperFactory;

/**
 * Base class for all Activiti "add step" tasks.
 */
public abstract class BaseAddEmrStep extends BaseJavaDelegate
{
    public static final String VARIABLE_EMR_STEP_ID = "emrStepId";

    @Autowired
    private EmrService emrService;

    @Autowired
    private EmrStepHelperFactory emrStepHelperFactory;

    protected Expression namespace;
    protected Expression emrClusterDefinitionName;
    protected Expression emrClusterName;
    protected Expression stepName;
    protected Expression continueOnError;

    protected Expression scriptLocation;
    protected Expression scriptArguments;

    protected Expression emrClusterId;
    protected Expression accountId;

    /**
     * Adds the Step to EMR cluster, and sets the step id as workflow variable.
     *
     * @param request the EmrStepsAddRequest
     * @param execution the DelegateExecution
     *
     * @throws Exception if any problems were encountered.
     */
    protected void addEmrStepAndSetWorkflowVariables(Object request, DelegateExecution execution) throws Exception
    {
        // Add the step.
        Object emrStep = emrService.addStepToCluster(request);

        EmrStepHelper stepHelper = emrStepHelperFactory.getStepHelper(request.getClass().getName());

        String stepId = stepHelper.getStepId(emrStep);

        // Set workflow variables based on the new step.
        // Set the stepId in workflow variable.
        setTaskWorkflowVariable(execution, VARIABLE_EMR_STEP_ID, stepId);
    }

    /**
     * Gets the namespace from the DelegateExecution
     *
     * @param execution the DelegateExecution
     *
     * @return namespace
     */
    protected String getNamespace(DelegateExecution execution)
    {
        return activitiHelper.getExpressionVariableAsString(namespace, execution);
    }

    /**
     * Gets the emr cluster definition name from the DelegateExecution
     *
     * @param execution the DelegateExecution
     *
     * @return emr cluster definition name
     */
    protected String getEmrClusterDefinitionName(DelegateExecution execution)
    {
        return activitiHelper.getExpressionVariableAsString(emrClusterDefinitionName, execution);
    }

    /**
     * Gets the cluster name from the DelegateExecution
     *
     * @param execution the DelegateExecution
     *
     * @return cluster name
     */
    protected String getEmrClusterName(DelegateExecution execution)
    {
        return activitiHelper.getExpressionVariableAsString(emrClusterName, execution);
    }

    /**
     * Gets the step name from the DelegateExecution
     *
     * @param execution the DelegateExecution
     *
     * @return step name
     */
    protected String getStepName(DelegateExecution execution)
    {
        return activitiHelper.getExpressionVariableAsString(stepName, execution);
    }

    /**
     * Gets the continueOnError value from the DelegateExecution
     *
     * @param execution the DelegateExecution
     *
     * @return ContinueOnError
     */
    protected boolean getContinueOnError(DelegateExecution execution)
    {
        return activitiHelper.getExpressionVariableAsBoolean(continueOnError, execution, "ContinueOnError", false, false);
    }

    /**
     * Gets the scriptLocation from the DelegateExecution
     *
     * @param execution the DelegateExecution
     *
     * @return ScriptLocation
     */
    protected String getScriptLocation(DelegateExecution execution)
    {
        return activitiHelper.getExpressionVariableAsString(scriptLocation, execution);
    }

    /**
     * Gets the script arguments from the DelegateExecution, delimited and escaped as a list.
     *
     * @param execution the DelegateExecution
     *
     * @return script arguments
     */
    protected List<String> getScriptArguments(DelegateExecution execution)
    {
        String scriptArgumentsString = activitiHelper.getExpressionVariableAsString(scriptArguments, execution);

        return daoHelper.splitStringWithDefaultDelimiterEscaped(scriptArgumentsString);
    }

    /**
     * Gets the EMR cluster ID
     * 
     * @param execution The delegate execution
     * @return The EMR cluster ID
     */
    protected String getEmrClusterId(DelegateExecution execution)
    {
        return activitiHelper.getExpressionVariableAsString(emrClusterId, execution);
    }

    protected String getAccountId(DelegateExecution execution)
    {
        return activitiHelper.getExpressionVariableAsString(accountId, execution);
    }
    
    /**
     * Populates common parameters.
     *
     * @param request the request.
     * @param execution the delegate execution.
     */
    protected void populateCommonParams(Object request, DelegateExecution execution)
    {
        EmrStepHelper stepHelper = emrStepHelperFactory.getStepHelper(request.getClass().getName());

        stepHelper.setRequestStepName(request, getStepName(execution));
        stepHelper.setRequestContinueOnError(request, getContinueOnError(execution));
        stepHelper.setRequestNamespace(request, getNamespace(execution));
        stepHelper.setRequestEmrClusterDefinitionName(request, getEmrClusterDefinitionName(execution));
        stepHelper.setRequestEmrClusterName(request, getEmrClusterName(execution));
        stepHelper.setRequestEmrClusterId(request, getEmrClusterId(execution));
        stepHelper.setRequestAccountId(request, getAccountId(execution));
    }
}