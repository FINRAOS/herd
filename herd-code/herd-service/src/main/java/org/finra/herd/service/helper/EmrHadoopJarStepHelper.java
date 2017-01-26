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

import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.EmrHadoopJarStep;
import org.finra.herd.model.api.xml.EmrHadoopJarStepAddRequest;

/**
 * The Hadoop jar step helper.
 */
@Component
public class EmrHadoopJarStepHelper extends EmrStepHelper
{
    @Override
    public Object buildResponseFromRequest(Object stepRequest)
    {
        EmrHadoopJarStepAddRequest emrHadoopJarStepAddRequest = (EmrHadoopJarStepAddRequest) stepRequest;
        EmrHadoopJarStep step = new EmrHadoopJarStep();

        step.setNamespace(emrHadoopJarStepAddRequest.getNamespace());
        step.setEmrClusterDefinitionName(emrHadoopJarStepAddRequest.getEmrClusterDefinitionName());
        step.setEmrClusterName(emrHadoopJarStepAddRequest.getEmrClusterName());


        step.setStepName(emrHadoopJarStepAddRequest.getStepName().trim());
        step.setJarLocation(emrHadoopJarStepAddRequest.getJarLocation().trim().replaceAll(getS3ManagedReplaceString(), emrHelper.getS3StagingLocation()));
        if (emrHadoopJarStepAddRequest.getMainClass() != null)
        {
            step.setMainClass(emrHadoopJarStepAddRequest.getMainClass().trim());
        }

        // Add the script arguments
        if (!CollectionUtils.isEmpty(emrHadoopJarStepAddRequest.getScriptArguments()))
        {
            List<String> scriptArguments = new ArrayList<>();
            step.setScriptArguments(scriptArguments);
            for (String argument : emrHadoopJarStepAddRequest.getScriptArguments())
            {
                scriptArguments.add(argument.trim());
            }
        }
        step.setContinueOnError(emrHadoopJarStepAddRequest.isContinueOnError());

        return step;
    }

    @Override
    public StepConfig getEmrStepConfig(Object step)
    {
        EmrHadoopJarStep hadoopJarStep = (EmrHadoopJarStep) step;

        return emrHelper.getEmrHadoopJarStepConfig(hadoopJarStep.getStepName(), hadoopJarStep.getJarLocation(), hadoopJarStep.getMainClass(),
            hadoopJarStep.getScriptArguments(), hadoopJarStep.isContinueOnError());
    }

    @Override
    public String getRequestEmrClusterDefinitionName(Object stepRequest)
    {
        return ((EmrHadoopJarStepAddRequest) stepRequest).getEmrClusterDefinitionName();
    }

    @Override
    public String getRequestEmrClusterId(Object stepRequest)
    {
        return ((EmrHadoopJarStepAddRequest) stepRequest).getEmrClusterId();
    }

    @Override
    public String getRequestEmrClusterName(Object stepRequest)
    {
        return ((EmrHadoopJarStepAddRequest) stepRequest).getEmrClusterName();
    }

    @Override
    public String getRequestNamespace(Object stepRequest)
    {
        return ((EmrHadoopJarStepAddRequest) stepRequest).getNamespace();
    }

    @Override
    public String getRequestStepName(Object stepRequest)
    {
        return ((EmrHadoopJarStepAddRequest) stepRequest).getStepName();
    }

    @Override
    public String getStepId(Object step)
    {
        return ((EmrHadoopJarStep) step).getId();
    }

    @Override
    public String getStepRequestType()
    {
        return EmrHadoopJarStepAddRequest.class.getName();
    }

    @Override
    public String getStepType()
    {
        return EmrHadoopJarStep.class.getName();
    }

    @Override
    public Boolean isRequestContinueOnError(Object stepRequest)
    {
        return ((EmrHadoopJarStepAddRequest) stepRequest).isContinueOnError();
    }

    @Override
    public void setRequestContinueOnError(Object stepRequest, Boolean continueOnError)
    {
        ((EmrHadoopJarStepAddRequest) stepRequest).setContinueOnError(continueOnError);
    }

    @Override
    public void setRequestEmrClusterDefinitionName(Object stepRequest, String clusterDefinitionName)
    {
        ((EmrHadoopJarStepAddRequest) stepRequest).setEmrClusterDefinitionName(clusterDefinitionName);
    }

    @Override
    public void setRequestEmrClusterId(Object stepRequest, String emrClusterId)
    {
        ((EmrHadoopJarStepAddRequest) stepRequest).setEmrClusterId(emrClusterId);
    }

    @Override
    public void setRequestEmrClusterName(Object stepRequest, String clusterName)
    {
        ((EmrHadoopJarStepAddRequest) stepRequest).setEmrClusterName(clusterName);
    }

    @Override
    public void setRequestNamespace(Object stepRequest, String namespace)
    {
        ((EmrHadoopJarStepAddRequest) stepRequest).setNamespace(namespace);
    }

    @Override
    public void setRequestStepName(Object stepRequest, String stepName)
    {
        ((EmrHadoopJarStepAddRequest) stepRequest).setStepName(stepName);
    }

    @Override
    public void setStepId(Object step, String stepId)
    {
        ((EmrHadoopJarStep) step).setId(stepId);
    }

    @Override
    public void validateAddStepRequest(Object step)
    {
        EmrHadoopJarStepAddRequest hadoopJarStep = (EmrHadoopJarStepAddRequest) step;

        validateStepName(hadoopJarStep.getStepName());
        validateJarLocation(hadoopJarStep.getJarLocation());
    }

    /**
     * Validates that hadoop jar location is specified.
     *
     * @param hadoopJarLocationString the Hadoop JAR location.
     */
    protected void validateJarLocation(String hadoopJarLocationString)
    {
        if (StringUtils.isBlank(hadoopJarLocationString))
        {
            throw new IllegalArgumentException("Hadoop JAR location must be specified.");
        }
    }

    @Override
    public String getRequestAccountId(Object stepRequest)
    {
        return ((EmrHadoopJarStepAddRequest) stepRequest).getAccountId();
    }

    @Override
    public void setRequestAccountId(Object stepRequest, String accountId)
    {
        ((EmrHadoopJarStepAddRequest) stepRequest).setAccountId(accountId);
    }
}