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

import org.finra.herd.model.api.xml.EmrPigStep;
import org.finra.herd.model.api.xml.EmrPigStepAddRequest;

/**
 * The Pig step helper.
 */
@Component
public class EmrPigStepHelper extends EmrStepHelper
{
    @Override
    public Object buildResponseFromRequest(Object stepRequest)
    {
        EmrPigStepAddRequest emrPigStepAddRequest = (EmrPigStepAddRequest) stepRequest;
        EmrPigStep step = new EmrPigStep();

        step.setNamespace(emrPigStepAddRequest.getNamespace());
        step.setEmrClusterDefinitionName(emrPigStepAddRequest.getEmrClusterDefinitionName());
        step.setEmrClusterName(emrPigStepAddRequest.getEmrClusterName());

        step.setStepName(emrPigStepAddRequest.getStepName().trim());
        step.setScriptLocation(emrPigStepAddRequest.getScriptLocation().trim().replaceAll(getS3ManagedReplaceString(), emrHelper.getS3StagingLocation()));
        // Add the script arguments
        if (!CollectionUtils.isEmpty(emrPigStepAddRequest.getScriptArguments()))
        {
            List<String> scriptArguments = new ArrayList<>();
            step.setScriptArguments(scriptArguments);
            for (String argument : emrPigStepAddRequest.getScriptArguments())
            {
                scriptArguments.add(argument.trim());
            }
        }
        step.setContinueOnError(emrPigStepAddRequest.isContinueOnError());

        return step;
    }

    @Override
    public StepConfig getEmrStepConfig(Object step)
    {
        EmrPigStep pigStep = (EmrPigStep) step;

        // Default ActionOnFailure is to cancel the execution and wait
        ActionOnFailure actionOnFailure = ActionOnFailure.CANCEL_AND_WAIT;

        if (pigStep.isContinueOnError() != null && pigStep.isContinueOnError())
        {
            // Override based on user input
            actionOnFailure = ActionOnFailure.CONTINUE;
        }

        // If there are no arguments to hive script
        if (CollectionUtils.isEmpty(pigStep.getScriptArguments()))
        {
            // Just build the StepConfig object and return
            return new StepConfig().withName(pigStep.getStepName().trim()).withActionOnFailure(actionOnFailure)
                .withHadoopJarStep(new StepFactory().newRunPigScriptStep(pigStep.getScriptLocation().trim()));
        }
        // If there are arguments specified
        else
        {
            return new StepConfig().withName(pigStep.getStepName().trim()).withActionOnFailure(actionOnFailure).withHadoopJarStep(new StepFactory()
                .newRunPigScriptStep(pigStep.getScriptLocation().trim(),
                    pigStep.getScriptArguments().toArray(new String[pigStep.getScriptArguments().size()])));
        }
    }

    @Override
    public String getRequestEmrClusterDefinitionName(Object stepRequest)
    {
        return ((EmrPigStepAddRequest) stepRequest).getEmrClusterDefinitionName();
    }

    @Override
    public String getRequestEmrClusterId(Object stepRequest)
    {
        return ((EmrPigStepAddRequest) stepRequest).getEmrClusterId();
    }

    @Override
    public String getRequestEmrClusterName(Object stepRequest)
    {
        return ((EmrPigStepAddRequest) stepRequest).getEmrClusterName();
    }

    @Override
    public String getRequestNamespace(Object stepRequest)
    {
        return ((EmrPigStepAddRequest) stepRequest).getNamespace();
    }

    @Override
    public String getRequestStepName(Object stepRequest)
    {
        return ((EmrPigStepAddRequest) stepRequest).getStepName();
    }

    @Override
    public String getStepId(Object step)
    {
        return ((EmrPigStep) step).getId();
    }

    @Override
    public String getStepRequestType()
    {
        return EmrPigStepAddRequest.class.getName();
    }

    @Override
    public String getStepType()
    {
        return EmrPigStep.class.getName();
    }

    @Override
    public Boolean isRequestContinueOnError(Object stepRequest)
    {
        return ((EmrPigStepAddRequest) stepRequest).isContinueOnError();
    }

    @Override
    public void setRequestContinueOnError(Object stepRequest, Boolean continueOnError)
    {
        ((EmrPigStepAddRequest) stepRequest).setContinueOnError(continueOnError);
    }

    @Override
    public void setRequestEmrClusterDefinitionName(Object stepRequest, String clusterDefinitionName)
    {
        ((EmrPigStepAddRequest) stepRequest).setEmrClusterDefinitionName(clusterDefinitionName);
    }

    @Override
    public void setRequestEmrClusterId(Object stepRequest, String emrClusterId)
    {
        ((EmrPigStepAddRequest) stepRequest).setEmrClusterId(emrClusterId);
    }

    @Override
    public void setRequestEmrClusterName(Object stepRequest, String clusterName)
    {
        ((EmrPigStepAddRequest) stepRequest).setEmrClusterName(clusterName);
    }

    @Override
    public void setRequestNamespace(Object stepRequest, String namespace)
    {
        ((EmrPigStepAddRequest) stepRequest).setNamespace(namespace);
    }

    @Override
    public void setRequestStepName(Object stepRequest, String stepName)
    {
        ((EmrPigStepAddRequest) stepRequest).setStepName(stepName);
    }

    @Override
    public void setStepId(Object step, String stepId)
    {
        ((EmrPigStep) step).setId(stepId);
    }

    @Override
    public void validateAddStepRequest(Object step)
    {
        EmrPigStepAddRequest pigStepRequest = (EmrPigStepAddRequest) step;

        validateStepName(pigStepRequest.getStepName());
        validateScriptLocation(pigStepRequest.getScriptLocation());
    }
}