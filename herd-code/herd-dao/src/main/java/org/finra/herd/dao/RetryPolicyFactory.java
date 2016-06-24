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
package org.finra.herd.dao;

import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.retry.RetryPolicy.BackoffStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * A factory to create RetryPolicy with autowired components.
 */
@Component
public class RetryPolicyFactory
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private BackoffStrategy backoffStrategy;

    /**
     * Gets the application's default retry policy. The policy uses PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, a SimpleExponentialBackoffStrategy, and the
     * maximum number of attempts is dynamically configurable through AWS_MAX_RETRY_ATTEMPT.
     * 
     * @return RetryPolicy
     */
    public RetryPolicy getRetryPolicy()
    {
        return new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, backoffStrategy,
            configurationHelper.getProperty(ConfigurationValue.AWS_MAX_RETRY_ATTEMPT, Integer.class), true);
    }
}
