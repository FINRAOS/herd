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
package org.finra.dm.dao;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.MapPropertySource;

import org.finra.dm.dao.helper.AwsEc2ExceptionRetryAdvice;
import org.finra.dm.dao.helper.AwsEmrExceptionRetryAdvice;
import org.finra.dm.dao.helper.AwsS3ExceptionRetryAdvice;
import org.finra.dm.dao.helper.AwsSqsExceptionRetryAdvice;
import org.finra.dm.dao.helper.AwsStsExceptionRetryAdvice;
import org.finra.dm.model.dto.ConfigurationValue;

/**
 * This class tests functionality within the AWS exception retry advice.
 */
public class AwsExceptionRetryAdviceTest extends AbstractDaoTest
{
    @Autowired
    private AwsS3ExceptionRetryAdvice s3Advice;

    @Autowired
    private AwsEmrExceptionRetryAdvice emrAdvice;

    @Autowired
    private AwsEc2ExceptionRetryAdvice ec2Advice;

    @Autowired
    private AwsStsExceptionRetryAdvice stsAdvice;

    @Autowired
    private AwsSqsExceptionRetryAdvice sqsAdvice;

    private MockProceedingJoinPoint proceedingJoinPoint;

    private AmazonServiceException throttlingException;

    private AmazonServiceException fivexxException;

    private AmazonServiceException retryableException;

    private static final String PROPERTY_SOURCE_NAME = "test property source";

    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();

        // Create an Amazon throttling exception.
        throttlingException = new AmazonServiceException("test throttling exception");
        throttlingException.setErrorCode("ThrottlingException");

        // Create an Amazon 5xx exception.
        fivexxException = new AmazonServiceException("test 5xx exception");
        fivexxException.setStatusCode(500);

        // Create an Amazon throttling exception.
        retryableException = new AmazonServiceException("test retryable exception");
        retryableException.setErrorCode("RetryableException");

        // Create a mock proceeding join point.
        proceedingJoinPoint = new MockProceedingJoinPoint();
    }

    @Test
    public void testRetryOnThrottlingExceptionNoException() throws Throwable
    {
        // The proceeding join point should return normally (i.e. success).
        s3Advice.retryOnException(proceedingJoinPoint);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRetryOnThrottlingExceptionNonAmazonServiceException() throws Throwable
    {
        // Set an illegal argument exception to be thrown by the advice when invoking the proceeding join point.
        proceedingJoinPoint.setExceptionToThrow(new IllegalArgumentException());
        s3Advice.retryOnException(proceedingJoinPoint);
    }

    @Test(expected = AmazonServiceException.class)
    public void testRetryOnThrottlingExceptionNormalThrottlingException() throws Throwable
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_S3_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Set a throttling exception to be thrown by the advice when invoking the proceeding join point.
            proceedingJoinPoint.setExceptionToThrow(throttlingException);
            s3Advice.retryOnException(proceedingJoinPoint);
        }
        finally
        {   
            restorePropertySourceInEnvironment();
        }
    }

    @Test(expected = AmazonServiceException.class)
    public void testRetryOnThrottlingExceptionOverriddenConfiguration() throws Throwable
    {
        // Add valid overridden configuration parameters. This can be seen with fewer log messages, but can't be checked by the JUnit.
        modifyS3ConfigParams("3");

        try
        {
            // Set a throttling exception to be thrown by the advice when invoking the proceeding join point.
            proceedingJoinPoint.setExceptionToThrow(throttlingException);
            s3Advice.retryOnException(proceedingJoinPoint);
        }
        finally
        {
            getMutablePropertySources().remove(PROPERTY_SOURCE_NAME);
        }
    }

    @Test(expected = AmazonServiceException.class)
    public void testRetryOn5xxException() throws Throwable
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_S3_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Set a 5xx exception to be thrown by the advice when invoking the proceeding join point.
            proceedingJoinPoint.setExceptionToThrow(fivexxException);
            s3Advice.retryOnException(proceedingJoinPoint);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test(expected = AmazonServiceException.class)
    public void testS3RetryOnRetryableException() throws Throwable
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_S3_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        overrideMap.put(ConfigurationValue.AWS_S3_RETRY_ON_ERROR_CODES.getKey(), "RetryableException");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Set a throttling exception to be thrown by the advice when invoking the proceeding join point.
            proceedingJoinPoint.setExceptionToThrow(retryableException);
            s3Advice.retryOnException(proceedingJoinPoint);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test(expected = AmazonServiceException.class)
    public void testEmrRetryOnRetryableException() throws Throwable
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_EMR_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        overrideMap.put(ConfigurationValue.AWS_EMR_RETRY_ON_ERROR_CODES.getKey(), "RetryableException");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Set a throttling exception to be thrown by the advice when invoking the proceeding join point.
            proceedingJoinPoint.setExceptionToThrow(retryableException);
            emrAdvice.retryOnException(proceedingJoinPoint);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test(expected = AmazonServiceException.class)
    public void testEc2RetryOnRetryableException() throws Throwable
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_EC2_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        overrideMap.put(ConfigurationValue.AWS_EC2_RETRY_ON_ERROR_CODES.getKey(), "RetryableException");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Set a throttling exception to be thrown by the advice when invoking the proceeding join point.
            proceedingJoinPoint.setExceptionToThrow(retryableException);
            ec2Advice.retryOnException(proceedingJoinPoint);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test(expected = AmazonServiceException.class)
    public void testSqsRetryOnRetryableException() throws Throwable
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_SQS_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        overrideMap.put(ConfigurationValue.AWS_SQS_RETRY_ON_ERROR_CODES.getKey(), "RetryableException");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Set a throttling exception to be thrown by the advice when invoking the proceeding join point.
            proceedingJoinPoint.setExceptionToThrow(retryableException);
            sqsAdvice.retryOnException(proceedingJoinPoint);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test(expected = AmazonServiceException.class)
    public void testStsRetryOnRetryableException() throws Throwable
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_STS_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        overrideMap.put(ConfigurationValue.AWS_STS_RETRY_ON_ERROR_CODES.getKey(), "RetryableException");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Set a throttling exception to be thrown by the advice when invoking the proceeding join point.
            proceedingJoinPoint.setExceptionToThrow(retryableException);
            stsAdvice.retryOnException(proceedingJoinPoint);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testRetryOnThrottlingExceptionInvalidConfiguration() throws Throwable
    {
        // Add invalid configuration parameters. This will output warnings which can't be checked by the JUnit.
        modifyS3ConfigParams("invalid_max_retry_delay_secs");

        // The proceeding join point should return normally (i.e. success).
        s3Advice.retryOnException(proceedingJoinPoint);
        
        getMutablePropertySources().remove(PROPERTY_SOURCE_NAME);
    }

    /**
     * Modify the configuration parameters by adding in a new property source to the environment.
     *
     * @param maxRetryDelaySecs the max retry delay in seconds.
     *
     * @throws Exception if any problems were encountered.
     */
    private void modifyS3ConfigParams(String maxRetryDelaySecs) throws Exception
    {
        // Create a map of custom properties with specified values.
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(ConfigurationValue.AWS_S3_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), maxRetryDelaySecs);

        // Add in the new map as a property source to make it available to the environment.
        getMutablePropertySources().addLast(new MapPropertySource(PROPERTY_SOURCE_NAME, propertiesMap));
    }

    public class MockProceedingJoinPoint extends org.finra.dm.core.MockProceedingJoinPoint
    {
        private Exception exceptionToThrow;

        public void setExceptionToThrow(Exception exceptionToThrow)
        {
            this.exceptionToThrow = exceptionToThrow;
        }

        @Override
        public Object proceed() throws Throwable
        {
            // If an exception to throw was configured, then throw it.
            if (exceptionToThrow != null)
            {
                throw exceptionToThrow;
            }

            // Return normally which represents no exception being thrown (i.e. success condition).
            return this;
        }
    }
}
