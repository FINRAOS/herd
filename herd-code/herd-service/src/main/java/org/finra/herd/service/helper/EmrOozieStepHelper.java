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
package org.finra.herd.service.helper;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.EmrOozieStep;
import org.finra.herd.model.api.xml.EmrOozieStepAddRequest;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * The Oozie step helper.
 */
@Component
public class EmrOozieStepHelper extends EmrStepHelper
{
    @Override
    public Object buildResponseFromRequest(Object stepRequest)
    {
        EmrOozieStepAddRequest emrOozieStepAddRequest = (EmrOozieStepAddRequest) stepRequest;
        EmrOozieStep step = new EmrOozieStep();

        step.setNamespace(emrOozieStepAddRequest.getNamespace());
        step.setEmrClusterDefinitionName(emrOozieStepAddRequest.getEmrClusterDefinitionName());
        step.setEmrClusterName(emrOozieStepAddRequest.getEmrClusterName());

        step.setStepName(emrOozieStepAddRequest.getStepName().trim());
        step.setWorkflowXmlLocation(
            emrOozieStepAddRequest.getWorkflowXmlLocation().trim().replaceAll(getS3ManagedReplaceString(), emrHelper.getS3StagingLocation()));
        step.setOoziePropertiesFileLocation(
            emrOozieStepAddRequest.getOoziePropertiesFileLocation().trim().replaceAll(getS3ManagedReplaceString(), emrHelper.getS3StagingLocation()));
        step.setContinueOnError(emrOozieStepAddRequest.isContinueOnError());

        return step;
    }

    @Override
    public StepConfig getEmrStepConfig(Object step)
    {
        EmrOozieStep oozieStep = (EmrOozieStep) step;

        // Hadoop Jar provided by Amazon to run shell script
        String hadoopJarForShellScript = configurationHelper.getProperty(ConfigurationValue.EMR_SHELL_SCRIPT_JAR);

        // Oozie SDK cannot be used at the moment, as the Oozie port 11000 needs to be opened for Oozie SDK usage
        // As a workaround, a custom shell script is used to run the Oozie client to add any oozie job
        // Once Oozie SDK implementation is in place, this custom shell script can be removed
        // Get the custom oozie shell script
        String oozieShellScript = emrHelper.getS3StagingLocation() + configurationHelper.getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER) +
            configurationHelper.getProperty(ConfigurationValue.EMR_OOZIE_RUN_SCRIPT);

        // Default ActionOnFailure is to cancel the execution and wait
        ActionOnFailure actionOnFailure = ActionOnFailure.CANCEL_AND_WAIT;
        if (oozieStep.isContinueOnError() != null && oozieStep.isContinueOnError())
        {
            // Override based on user input
            actionOnFailure = ActionOnFailure.CONTINUE;
        }

        // Add the arguments to the custom shell script
        List<String> argsList = new ArrayList<>();

        // Get the oozie client run shell script
        argsList.add(oozieShellScript);

        // Specify the arguments
        argsList.add(oozieStep.getWorkflowXmlLocation().trim());
        argsList.add(oozieStep.getOoziePropertiesFileLocation().trim());

        // Build the StepConfig object and return
        HadoopJarStepConfig jarConfig = new HadoopJarStepConfig(hadoopJarForShellScript).withArgs(argsList);
        return new StepConfig().withName(oozieStep.getStepName().trim()).withActionOnFailure(actionOnFailure).withHadoopJarStep(jarConfig);
    }

    @Override
    public String getRequestEmrClusterDefinitionName(Object stepRequest)
    {
        return ((EmrOozieStepAddRequest) stepRequest).getEmrClusterDefinitionName();
    }

    @Override
    public String getRequestEmrClusterId(Object stepRequest)
    {
        return ((EmrOozieStepAddRequest) stepRequest).getEmrClusterId();
    }

    @Override
    public String getRequestEmrClusterName(Object stepRequest)
    {
        return ((EmrOozieStepAddRequest) stepRequest).getEmrClusterName();
    }

    @Override
    public String getRequestNamespace(Object stepRequest)
    {
        return ((EmrOozieStepAddRequest) stepRequest).getNamespace();
    }

    @Override
    public String getRequestStepName(Object stepRequest)
    {
        return ((EmrOozieStepAddRequest) stepRequest).getStepName();
    }

    @Override
    public String getStepId(Object step)
    {
        return ((EmrOozieStep) step).getId();
    }

    @Override
    public String getStepRequestType()
    {
        return EmrOozieStepAddRequest.class.getName();
    }

    @Override
    public String getStepType()
    {
        return EmrOozieStep.class.getName();
    }

    @Override
    public Boolean isRequestContinueOnError(Object stepRequest)
    {
        return ((EmrOozieStepAddRequest) stepRequest).isContinueOnError();
    }

    @Override
    public void setRequestContinueOnError(Object stepRequest, Boolean continueOnError)
    {
        ((EmrOozieStepAddRequest) stepRequest).setContinueOnError(continueOnError);
    }

    @Override
    public void setRequestEmrClusterDefinitionName(Object stepRequest, String clusterDefinitionName)
    {
        ((EmrOozieStepAddRequest) stepRequest).setEmrClusterDefinitionName(clusterDefinitionName);
    }

    @Override
    public void setRequestEmrClusterId(Object stepRequest, String emrClusterId)
    {
        ((EmrOozieStepAddRequest) stepRequest).setEmrClusterId(emrClusterId);
    }

    @Override
    public void setRequestEmrClusterName(Object stepRequest, String clusterName)
    {
        ((EmrOozieStepAddRequest) stepRequest).setEmrClusterName(clusterName);
    }

    @Override
    public void setRequestNamespace(Object stepRequest, String namespace)
    {
        ((EmrOozieStepAddRequest) stepRequest).setNamespace(namespace);
    }

    @Override
    public void setRequestStepName(Object stepRequest, String stepName)
    {
        ((EmrOozieStepAddRequest) stepRequest).setStepName(stepName);
    }

    @Override
    public void setStepId(Object step, String stepId)
    {
        ((EmrOozieStep) step).setId(stepId);
    }

    @Override
    public void validateAddStepRequest(Object step)
    {
        EmrOozieStepAddRequest oozieStep = (EmrOozieStepAddRequest) step;

        validateStepName(oozieStep.getStepName());
        validateWorkflowXmlLocation(oozieStep.getWorkflowXmlLocation());
        validateOoziePropertiesFileLocation(oozieStep.getOoziePropertiesFileLocation());
    }

    /**
     * Validates that workflow xml location is specified.
     *
     * @param workflowXmlLocationString the workflow XML location.
     */
    protected void validateWorkflowXmlLocation(String workflowXmlLocationString)
    {
        if (StringUtils.isBlank(workflowXmlLocationString))
        {
            throw new IllegalArgumentException("Workflow XML location must be specified.");
        }
    }

    /**
     * Validates that oozie properties file location is specified.
     *
     * @param ooziePropertiesFileLocationString the Oozie properties file location.
     */
    protected void validateOoziePropertiesFileLocation(String ooziePropertiesFileLocationString)
    {
        if (StringUtils.isBlank(ooziePropertiesFileLocationString))
        {
            throw new IllegalArgumentException("Oozie properties file location must be specified.");
        }
    }

    @Override
    public String getRequestAccountId(Object stepRequest)
    {
        return ((EmrOozieStepAddRequest) stepRequest).getAccountId();
    }

    @Override
    public void setRequestAccountId(Object stepRequest, String accountId)
    {
        ((EmrOozieStepAddRequest) stepRequest).setAccountId(accountId);
    }
}