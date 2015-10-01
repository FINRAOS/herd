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

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.ec2.model.ModifyInstanceAttributeRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.dao.impl.MockAwsOperationsHelper;
import org.finra.dm.model.dto.ConfigurationValue;

/**
 * This class tests that the AwsExceptionRetryAdvice is applied on all AwsOperations.
 */
public class AwsOperationsAdviceTest extends AbstractDaoTest
{
    @Autowired
    private S3Operations s3Operations;

    @Autowired
    private EmrOperations emrOperations;

    @Autowired
    private Ec2Operations ec2Operations;

    @Autowired
    private StsOperations stsOperations;

    @Autowired
    private SqsOperations sqsOperations;

    @Test
    public void testS3RetryAdviceApplied() throws Exception
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_S3_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        modifyPropertySourceInEnvironment(overrideMap);

        // Start a stop watch to keep track of how long the s3Operation method takes.
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try
        {
            // Call the s3Operations to throw a throttling exception.
            s3Operations.getObjectMetadata(null, MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION, null);
        }
        catch (AmazonServiceException ase)
        {
            stopWatch.stop();
            assertTrue(stopWatch.getTime() > 1000L);
        }

        restorePropertySourceInEnvironment();
    }

    @Test
    public void testEmrRetryAdviceApplied() throws Exception
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_EMR_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        modifyPropertySourceInEnvironment(overrideMap);

        // Start a stop watch to keep track of how long the s3Operation method takes.
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try
        {
            RunJobFlowRequest runJobFlowRequest = new RunJobFlowRequest();
            runJobFlowRequest.setAmiVersion(MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION);

            // Call the s3Operations to throw a throttling exception.
            emrOperations.runEmrJobFlow(null, runJobFlowRequest);
        }
        catch (AmazonServiceException ase)
        {
            stopWatch.stop();
            assertTrue(stopWatch.getTime() > 1000L);
        }

        restorePropertySourceInEnvironment();
    }

    @Test
    public void testEc2RetryAdviceApplied() throws Exception
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_EC2_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        modifyPropertySourceInEnvironment(overrideMap);

        // Start a stop watch to keep track of how long the s3Operation method takes.
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try
        {
            ModifyInstanceAttributeRequest modifyInstanceAttributeRequest =
                new ModifyInstanceAttributeRequest().withInstanceId("").withGroups(MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION);


            // Call the s3Operations to throw a throttling exception.
            ec2Operations.modifyInstanceAttribute(null, modifyInstanceAttributeRequest);
        }
        catch (AmazonServiceException ase)
        {
            stopWatch.stop();
            assertTrue(stopWatch.getTime() > 1000L);
        }

        restorePropertySourceInEnvironment();
    }

    @Test
    public void testStsRetryAdviceApplied() throws Exception
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_STS_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        modifyPropertySourceInEnvironment(overrideMap);

        // Start a stop watch to keep track of how long the s3Operation method takes.
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try
        {
            AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest();
            assumeRoleRequest.setPolicy(MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION);

            // Call the s3Operations to throw a throttling exception.
            stsOperations.assumeRole(null, assumeRoleRequest);
        }
        catch (AmazonServiceException ase)
        {
            stopWatch.stop();
            assertTrue(stopWatch.getTime() > 1000L);
        }

        restorePropertySourceInEnvironment();
    }

    @Test
    public void testSqsRetryAdviceApplied() throws Exception
    {
        // Remove property source so that we can test mandatory tags config being null
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.AWS_SQS_EXCEPTION_MAX_RETRY_DURATION_SECS.getKey(), 2);
        modifyPropertySourceInEnvironment(overrideMap);

        // Start a stop watch to keep track of how long the s3Operation method takes.
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try
        {
            // Call the s3Operations to throw a throttling exception.
            sqsOperations.sendSqsTextMessage(new ClientConfiguration(), MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION, null);
        }
        catch (AmazonServiceException ase)
        {
            stopWatch.stop();
            assertTrue(stopWatch.getTime() > 1000L);
        }

        restorePropertySourceInEnvironment();
    }
}
