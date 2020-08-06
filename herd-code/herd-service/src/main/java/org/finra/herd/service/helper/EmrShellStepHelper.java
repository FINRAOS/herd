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
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.EmrShellStep;
import org.finra.herd.model.api.xml.EmrShellStepAddRequest;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * The Shell step helper.
 */
@Component
public class EmrShellStepHelper extends EmrStepHelper
{
    @Override
    public Object buildResponseFromRequest(Object stepRequest, String trustingAccountStagingBucketName)
    {
        EmrShellStepAddRequest emrShellStepAddRequest = (EmrShellStepAddRequest) stepRequest;
        EmrShellStep step = new EmrShellStep();

        step.setNamespace(emrShellStepAddRequest.getNamespace());
        step.setEmrClusterDefinitionName(emrShellStepAddRequest.getEmrClusterDefinitionName());
        step.setEmrClusterName(emrShellStepAddRequest.getEmrClusterName());

        step.setStepName(emrShellStepAddRequest.getStepName().trim());
        step.setScriptLocation(emrShellStepAddRequest.getScriptLocation().trim()
            .replaceAll(getS3ManagedReplaceString(), emrHelper.getS3StagingLocation(trustingAccountStagingBucketName)));
        // Add the script arguments
        if (!CollectionUtils.isEmpty(emrShellStepAddRequest.getScriptArguments()))
        {
            List<String> scriptArguments = new ArrayList<>();
            step.setScriptArguments(scriptArguments);
            for (String argument : emrShellStepAddRequest.getScriptArguments())
            {
                scriptArguments.add(argument.trim());
            }
        }
        step.setContinueOnError(emrShellStepAddRequest.isContinueOnError());

        return step;
    }

    @Override
    public StepConfig getEmrStepConfig(Object step)
    {
        EmrShellStep emrShellStep = (EmrShellStep) step;

        // Hadoop Jar provided by Amazon for running Shell Scripts
        String hadoopJarForShellScript = configurationHelper.getProperty(ConfigurationValue.EMR_SHELL_SCRIPT_JAR_PATH);

        // Default ActionOnFailure is to cancel the execution and wait
        ActionOnFailure actionOnFailure = ActionOnFailure.CANCEL_AND_WAIT;
        if (emrShellStep.isContinueOnError() != null && emrShellStep.isContinueOnError())
        {
            // Override based on user input
            actionOnFailure = ActionOnFailure.CONTINUE;
        }

        // Add the script location
        List<String> argsList = new ArrayList<>();
        argsList.add(emrShellStep.getScriptLocation().trim());

        // Add the script arguments
        if (!CollectionUtils.isEmpty(emrShellStep.getScriptArguments()))
        {
            for (String argument : emrShellStep.getScriptArguments())
            {
                argsList.add(argument.trim());
            }
        }

        // Return the StepConfig object
        HadoopJarStepConfig jarConfig = new HadoopJarStepConfig(hadoopJarForShellScript).withArgs(argsList);
        return new StepConfig().withName(emrShellStep.getStepName().trim()).withActionOnFailure(actionOnFailure).withHadoopJarStep(jarConfig);
    }

    @Override
    public String getRequestEmrClusterDefinitionName(Object step)
    {
        return ((EmrShellStepAddRequest) step).getEmrClusterDefinitionName();
    }

    @Override
    public String getRequestEmrClusterId(Object stepRequest)
    {
        return ((EmrShellStepAddRequest) stepRequest).getEmrClusterId();
    }

    @Override
    public String getRequestEmrClusterName(Object step)
    {
        return ((EmrShellStepAddRequest) step).getEmrClusterName();
    }

    @Override
    public String getRequestNamespace(Object step)
    {
        return ((EmrShellStepAddRequest) step).getNamespace();
    }

    @Override
    public String getRequestStepName(Object step)
    {
        return ((EmrShellStepAddRequest) step).getStepName();
    }

    @Override
    public String getStepId(Object step)
    {
        return ((EmrShellStep) step).getId();
    }

    @Override
    public String getStepRequestType()
    {
        return EmrShellStepAddRequest.class.getName();
    }

    @Override
    public String getStepType()
    {
        return EmrShellStep.class.getName();
    }

    @Override
    public Boolean isRequestContinueOnError(Object step)
    {
        return ((EmrShellStepAddRequest) step).isContinueOnError();
    }

    @Override
    public void setRequestContinueOnError(Object step, Boolean continueOnError)
    {
        ((EmrShellStepAddRequest) step).setContinueOnError(continueOnError);
    }

    @Override
    public void setRequestEmrClusterDefinitionName(Object step, String clusterDefinitionName)
    {
        ((EmrShellStepAddRequest) step).setEmrClusterDefinitionName(clusterDefinitionName);
    }

    @Override
    public void setRequestEmrClusterId(Object stepRequest, String emrClusterId)
    {
        ((EmrShellStepAddRequest) stepRequest).setEmrClusterId(emrClusterId);
    }

    @Override
    public void setRequestEmrClusterName(Object step, String clusterName)
    {
        ((EmrShellStepAddRequest) step).setEmrClusterName(clusterName);
    }

    @Override
    public void setRequestNamespace(Object step, String namespace)
    {
        ((EmrShellStepAddRequest) step).setNamespace(namespace);
    }

    @Override
    public void setRequestStepName(Object step, String stepName)
    {
        ((EmrShellStepAddRequest) step).setStepName(stepName);
    }

    @Override
    public void setStepId(Object step, String stepId)
    {
        ((EmrShellStep) step).setId(stepId);
    }

    @Override
    public void validateAddStepRequest(Object step)
    {
        EmrShellStepAddRequest shellStep = (EmrShellStepAddRequest) step;

        validateStepName(shellStep.getStepName());
        validateScriptLocation(shellStep.getScriptLocation());
    }

    @Override
    public String getRequestAccountId(Object stepRequest)
    {
        return ((EmrShellStepAddRequest) stepRequest).getAccountId();
    }

    @Override
    public void setRequestAccountId(Object stepRequest, String accountId)
    {
        ((EmrShellStepAddRequest) stepRequest).setAccountId(accountId);
    }
}
