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

import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.EmrHelper;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * Base abstract class for EMR step helpers. All EMR step helpers will extend this class.
 */
public abstract class EmrStepHelper
{
    @Autowired
    protected ConfigurationHelper configurationHelper;

    @Autowired
    protected EmrHelper emrHelper;

    /**
     * This method builds the Step object for the given Step request.
     *
     * @param stepRequest the step request object
     *
     * @return the step object
     */
    public abstract Object buildResponseFromRequest(Object stepRequest);

    /**
     * This method gets the StepConfig object for the given Step.
     *
     * @param step the step object
     *
     * @return the step config object
     */
    public abstract StepConfig getEmrStepConfig(Object step);

    /**
     * Return the cluster definition name.
     *
     * @param stepRequest the Add Step request object.
     *
     * @return the cluster definition name.
     */
    public abstract String getRequestEmrClusterDefinitionName(Object stepRequest);

    /**
     * Gets EMR cluster ID.
     *
     * @param stepRequest The step request
     *
     * @return The EMR cluster ID
     */
    public abstract String getRequestEmrClusterId(Object stepRequest);

    /**
     * Return the cluster name.
     *
     * @param stepRequest the Add Step request object.
     *
     * @return the cluster name.
     */
    public abstract String getRequestEmrClusterName(Object stepRequest);

    /**
     * Return the namespace.
     *
     * @param stepRequest the Add Step request object.
     *
     * @return the namespace.
     */
    public abstract String getRequestNamespace(Object stepRequest);

    /**
     * Return the step name.
     *
     * @param stepRequest the Add Step request object.
     *
     * @return the step name.
     */
    public abstract String getRequestStepName(Object stepRequest);

    /**
     * Return the step Id.
     *
     * @param step the Step object.
     *
     * @return the Step Id.
     */
    public abstract String getStepId(Object step);

    /**
     * Return the type of step request it supports, the name of the step request class.
     *
     * @return the Step request class name
     */
    public abstract String getStepRequestType();

    /**
     * Return the type of step it supports, the name of the step class.
     *
     * @return the Step class name
     */
    public abstract String getStepType();

    /**
     * Return the continue on error.
     *
     * @param stepRequest the Add Step request object.
     *
     * @return the continue on error.
     */
    public abstract Boolean isRequestContinueOnError(Object stepRequest);

    /**
     * Sets the continue on error.
     *
     * @param stepRequest the Add Step request object.
     * @param continueOnError the continue on error value to set.
     */
    public abstract void setRequestContinueOnError(Object stepRequest, Boolean continueOnError);

    /**
     * Sets the cluster definition name.
     *
     * @param stepRequest the Add Step request object.
     * @param clusterDefinitionName the cluster definition name value to set.
     */
    public abstract void setRequestEmrClusterDefinitionName(Object stepRequest, String clusterDefinitionName);

    /**
     * Sets EMR cluster ID.
     *
     * @param stepRequest The step request
     * @param emrClusterId The EMR cluster ID
     */
    public abstract void setRequestEmrClusterId(Object stepRequest, String emrClusterId);

    /**
     * Sets the cluster name.
     *
     * @param stepRequest the Add Step request object.
     * @param clusterName the cluster name value to set.
     */
    public abstract void setRequestEmrClusterName(Object stepRequest, String clusterName);

    /**
     * Sets the namespace.
     *
     * @param stepRequest the Add Step request object.
     * @param namespace the namespace value to set.
     */
    public abstract void setRequestNamespace(Object stepRequest, String namespace);

    /**
     * Sets the step name.
     *
     * @param stepRequest the Add Step request object.
     * @param stepName the step name value to set.
     */
    public abstract void setRequestStepName(Object stepRequest, String stepName);

    /**
     * Sets the step Id.
     *
     * @param step the Step object.
     * @param stepId the step Id value to set.
     */
    public abstract void setStepId(Object step, String stepId);

    /**
     * Validates the step request.
     *
     * @param step request object
     */
    public abstract void validateAddStepRequest(Object step);

    protected String getS3ManagedReplaceString()
    {
        return configurationHelper.getProperty(ConfigurationValue.S3_STAGING_RESOURCE_LOCATION);
    }

    /**
     * Validates that Step name is specified.
     *
     * @param stepName the name of the step.
     */
    protected void validateStepName(String stepName)
    {
        if (StringUtils.isBlank(stepName))
        {
            throw new IllegalArgumentException("Step name must be specified.");
        }
    }

    /**
     * Validates that script location is specified.
     *
     * @param scriptLocationString the script location.
     */
    protected void validateScriptLocation(String scriptLocationString)
    {
        if (StringUtils.isBlank(scriptLocationString))
        {
            throw new IllegalArgumentException("Script location must be specified.");
        }
    }
    
    /**
     * Gets Request accountId
     *
     * @param stepRequest The step request
     *
     * @return The Request account Id
     */
    public abstract String getRequestAccountId(Object stepRequest);
    
    /**
     * Sets Request accountID.
     *
     * @param stepRequest The step request
     * @param accountId The account ID
     */
    public abstract void setRequestAccountId(Object stepRequest, String accountId);

}