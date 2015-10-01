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
package org.finra.dm.dao.helper;

import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.retry.RetryUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

import org.finra.dm.core.helper.DmThreadHelper;
import org.finra.dm.model.dto.ConfigurationValue;

/**
 * Advice that catches various AWS exceptions and retries invoking the method for a configurable amount of time to give the method a chance at succeeding when
 * usage is high. After all the retries have been attempted, the original exception will be thrown.
 */
public abstract class AwsExceptionRetryAdvice
{
    private static final Logger LOGGER = Logger.getLogger(AwsExceptionRetryAdvice.class);

    @Autowired
    protected DmThreadHelper dmThreadHelper;

    @Autowired
    protected DmStringHelper dmStringHelper;

    /**
     * Invokes the method, catches following various exceptions and retries as needed: 1. Throttling exception. 2. 5xx exception. 3. Error codes defined in
     * configuration to be retried.
     *
     * @param pjp the join point.
     *
     * @return the return value of the method at the join point.
     * @throws Throwable if any errors were encountered.
     */
    public Object retryOnException(ProceedingJoinPoint pjp) throws Throwable
    {
        // Get the method name being invoked.
        Class<?> targetClass = pjp.getTarget().getClass();
        MethodSignature targetMethodSignature = (MethodSignature) pjp.getSignature();
        String methodName = targetClass.getName() + "." + targetMethodSignature.getName();

        // Get the max delay in seconds.
        long maxTotalDelay = getMaxRetryDelaySecs() * 1000L;

        // Initialize a retry count to know the number of times we have retried calling the method.
        int retryCount = 0;

        // Get the minimum and maximum retry delay.
        long minAwsDelay = dmStringHelper.getConfigurationValueAsInteger(ConfigurationValue.AWS_MIN_RETRY_DELAY_SECS) * 1000L;
        long maxAwsDelay = dmStringHelper.getConfigurationValueAsInteger(ConfigurationValue.AWS_MAX_RETRY_DELAY_SECS) * 1000L;

        StopWatch totalElapsedTimeStopWatch = new StopWatch();
        totalElapsedTimeStopWatch.start();

        // Loop indefinitely. We will exit the loop if the method returns normally or a non-retryable exception is thrown.
        // If a retryable exception is thrown, the loop will exit after we have exceeded the maximum retry delay seconds.
        while (true)
        {
            try
            {
                // Proceed to the join point (i.e. call the method and let it return normally).
                return pjp.proceed();
            }
            catch (AmazonServiceException ase)
            {
                // Retry if:
                // 1) Is throttling exception.
                // 2) Is 5xx exception.
                // 3) Is an error to be re-tried on as defined in configuration.
                if (RetryUtils.isThrottlingException(ase) || is5xxException(ase) || isRetryableException(ase))
                {
                    long totalElapsedTime = totalElapsedTimeStopWatch.getTime();

                    // It's a retryable exception. Check if we've retried for enough time.
                    if (totalElapsedTime >= maxTotalDelay)
                    {
                        // We've retried for enough time so re-throw the original exception.
                        LOGGER.warn("An exception occurred while calling " + methodName + ". The method has been retried for " + (totalElapsedTime / 1000.0f) +
                            " second(s) and will not be retried anymore since it would exceed the maximum retry delay." + " The exception will now be thrown.");
                        throw ase;
                    }

                    // Get the next delay.
                    long nextSleepDelay = Math.min(((long) Math.pow(2, retryCount) * minAwsDelay), maxAwsDelay);
                    long timeLeftToRetry = maxTotalDelay - totalElapsedTime;

                    // If time left to try is less than next sleep delay, then use time left as next sleep delay to be retried the last time.
                    if (timeLeftToRetry < nextSleepDelay)
                    {
                        nextSleepDelay = timeLeftToRetry;
                    }

                    // Log a warning so we're aware that we are retrying.
                    LOGGER.warn("A retryable exception occurred while calling " + methodName + ". " + (totalElapsedTime / 1000.0f) +
                        " second(s) have elapsed since initial exception and maximum retry delay is " + (maxTotalDelay / 1000.0f) +
                        " second(s). Will retry in " + (nextSleepDelay / 1000.0f) + " second(s).");

                    // We can retry again so increment a counter to keep track of the number of times we retried and recalculate the total elapsed time.
                    retryCount++;

                    // Sleep for the next sleep delay.
                    dmThreadHelper.sleep(nextSleepDelay);
                }
                else
                {
                    // It's not a retryable exception (i.e. some other type of service exception) so just re-throw it.
                    throw ase;
                }
            }
        }
    }

    /**
     * The maximum retry delay in seconds.
     *
     * @return the maximum retry delay in seconds.
     */
    protected abstract int getMaxRetryDelaySecs();

    /**
     * The error codes to retry on.
     *
     * @return error codes delimited by default delimiter.
     */
    protected abstract String getExceptionErrorCodes();

    /*
     * Determines whether the error is one of defined in configuration value to be retried for.
     */
    private boolean isRetryableException(AmazonServiceException ase)
    {
        List<String> errorCodesToRetry = dmStringHelper.splitStringWithDefaultDelimiter(getExceptionErrorCodes());

        return errorCodesToRetry.contains(ase.getErrorCode());
    }

    /*
     * Determines whether the error is a 5xx exception.
     */
    private boolean is5xxException(AmazonServiceException ase)
    {
        return (ase.getStatusCode() >= HttpStatus.INTERNAL_SERVER_ERROR.value() && ase.getStatusCode() < 600);
    }
}