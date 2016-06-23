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
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.EmrHiveStep;
import org.finra.herd.model.api.xml.EmrHiveStepAddRequest;

/**
 * The Hive step helper.
 */
@Component
public class EmrHiveStepHelper extends EmrStepHelper
{
    @Override
    public Object buildResponseFromRequest(Object stepRequest)
    {
        EmrHiveStepAddRequest emrHiveStepAddRequest = (EmrHiveStepAddRequest) stepRequest;
        EmrHiveStep step = new EmrHiveStep();

        step.setNamespace(emrHiveStepAddRequest.getNamespace());
        step.setEmrClusterDefinitionName(emrHiveStepAddRequest.getEmrClusterDefinitionName());
        step.setEmrClusterName(emrHiveStepAddRequest.getEmrClusterName());

        step.setStepName(emrHiveStepAddRequest.getStepName().trim());
        step.setScriptLocation(emrHiveStepAddRequest.getScriptLocation().trim().replaceAll(getS3ManagedReplaceString(), emrHelper.getS3StagingLocation()));
        // Add the script arguments
        if (!CollectionUtils.isEmpty(emrHiveStepAddRequest.getScriptArguments()))
        {
            List<String> scriptArguments = new ArrayList<>();
            step.setScriptArguments(scriptArguments);
            for (String argument : emrHiveStepAddRequest.getScriptArguments())
            {
                scriptArguments.add(argument.trim());
            }
        }
        step.setContinueOnError(emrHiveStepAddRequest.isContinueOnError());

        return step;
    }

    @Override
    public StepConfig getEmrStepConfig(Object step)
    {
        EmrHiveStep emrHiveStep = (EmrHiveStep) step;

        // Default ActionOnFailure is to cancel the execution and wait
        ActionOnFailure actionOnFailure = ActionOnFailure.CANCEL_AND_WAIT;

        if (emrHiveStep.isContinueOnError() != null && emrHiveStep.isContinueOnError())
        {
            // Override based on user input
            actionOnFailure = ActionOnFailure.CONTINUE;
        }

        // If there are no arguments to hive script
        if (CollectionUtils.isEmpty(emrHiveStep.getScriptArguments()))
        {
            // Just build the StepConfig object and return
            return new StepConfig().withName(emrHiveStep.getStepName().trim()).withActionOnFailure(actionOnFailure)
                .withHadoopJarStep(new StepFactory().newRunHiveScriptStep(emrHiveStep.getScriptLocation().trim()));
        }
        // If there are arguments specified
        else
        {
            // For each argument, add "-d" option
            List<String> hiveArgs = new ArrayList<>();
            for (String hiveArg : emrHiveStep.getScriptArguments())
            {
                hiveArgs.add("-d");
                hiveArgs.add(hiveArg);
            }
            // Return the StepConfig object
            return new StepConfig().withName(emrHiveStep.getStepName().trim()).withActionOnFailure(actionOnFailure).withHadoopJarStep(
                new StepFactory().newRunHiveScriptStep(emrHiveStep.getScriptLocation().trim(), hiveArgs.toArray(new String[hiveArgs.size()])));
        }
    }

    @Override
    public String getRequestEmrClusterDefinitionName(Object stepRequest)
    {
        return ((EmrHiveStepAddRequest) stepRequest).getEmrClusterDefinitionName();
    }

    @Override
    public String getRequestEmrClusterId(Object stepRequest)
    {
        return ((EmrHiveStepAddRequest) stepRequest).getEmrClusterId();
    }

    @Override
    public String getRequestEmrClusterName(Object stepRequest)
    {
        return ((EmrHiveStepAddRequest) stepRequest).getEmrClusterName();
    }

    @Override
    public String getRequestNamespace(Object stepRequest)
    {
        return ((EmrHiveStepAddRequest) stepRequest).getNamespace();
    }

    @Override
    public String getRequestStepName(Object stepRequest)
    {
        return ((EmrHiveStepAddRequest) stepRequest).getStepName();
    }

    @Override
    public String getStepId(Object step)
    {
        return ((EmrHiveStep) step).getId();
    }

    @Override
    public String getStepRequestType()
    {
        return EmrHiveStepAddRequest.class.getName();
    }

    @Override
    public String getStepType()
    {
        return EmrHiveStep.class.getName();
    }

    @Override
    public Boolean isRequestContinueOnError(Object stepRequest)
    {
        return ((EmrHiveStepAddRequest) stepRequest).isContinueOnError();
    }

    @Override
    public void setRequestContinueOnError(Object stepRequest, Boolean continueOnError)
    {
        ((EmrHiveStepAddRequest) stepRequest).setContinueOnError(continueOnError);
    }

    @Override
    public void setRequestEmrClusterDefinitionName(Object stepRequest, String clusterDefinitionName)
    {
        ((EmrHiveStepAddRequest) stepRequest).setEmrClusterDefinitionName(clusterDefinitionName);
    }

    @Override
    public void setRequestEmrClusterId(Object stepRequest, String emrClusterId)
    {
        ((EmrHiveStepAddRequest) stepRequest).setEmrClusterId(emrClusterId);
    }

    @Override
    public void setRequestEmrClusterName(Object stepRequest, String clusterName)
    {
        ((EmrHiveStepAddRequest) stepRequest).setEmrClusterName(clusterName);
    }

    @Override
    public void setRequestNamespace(Object stepRequest, String namespace)
    {
        ((EmrHiveStepAddRequest) stepRequest).setNamespace(namespace);
    }

    @Override
    public void setRequestStepName(Object stepRequest, String stepName)
    {
        ((EmrHiveStepAddRequest) stepRequest).setStepName(stepName);
    }

    @Override
    public void setStepId(Object step, String stepId)
    {
        ((EmrHiveStep) step).setId(stepId);
    }

    @Override
    public void validateAddStepRequest(Object step)
    {
        EmrHiveStepAddRequest hiveStepRequest = (EmrHiveStepAddRequest) step;

        validateStepName(hiveStepRequest.getStepName());
        validateScriptLocation(hiveStepRequest.getScriptLocation());
    }
}