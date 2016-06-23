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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy.BackoffStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simplified version of AWS's {@link com.amazonaws.retry.PredefinedRetryPolicies#DEFAULT_BACKOFF_STRATEGY}. Does an exponential backoff starting from 1
 * second.
 */
public class SimpleExponentialBackoffStrategy implements BackoffStrategy
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleExponentialBackoffStrategy.class);

    private static final long SECOND_IN_MS = 1000;

    @Override
    public long delayBeforeNextRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception, int retriesAttempted)
    {
        long delay = (long) Math.pow(2, retriesAttempted) * SECOND_IN_MS;
        LOGGER.warn("delayBeforeNextRetryInMilliseconds={} retriesAttempted={}", delay, retriesAttempted, exception);
        return delay;
    }
}
