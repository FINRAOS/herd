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

import static org.finra.herd.core.AbstractCoreTest.EMPTY_STRING;
import static org.finra.herd.dao.AbstractDaoTest.AWS_REGION_NAME_US_EAST_1;
import static org.finra.herd.dao.AbstractDaoTest.AWS_SQS_QUEUE_NAME;
import static org.finra.herd.dao.AbstractDaoTest.AWS_SQS_QUEUE_URL;
import static org.finra.herd.dao.AbstractDaoTest.ERROR_MESSAGE;
import static org.finra.herd.dao.AbstractDaoTest.I_DO_NOT_EXIST;
import static org.finra.herd.dao.AbstractDaoTest.NO_AWS_ACCESS_KEY;
import static org.finra.herd.dao.AbstractDaoTest.NO_AWS_SECRET_KEY;
import static org.finra.herd.dao.AbstractDaoTest.NO_HTTP_PROXY_HOST;
import static org.finra.herd.dao.AbstractDaoTest.NO_HTTP_PROXY_PORT;
import static org.finra.herd.dao.AbstractDaoTest.NO_SESSION_TOKEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.AwsClientFactory;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * This class tests functionality within the HerdJmsDestinationResolver.
 */
public class HerdJmsDestinationResolverTest
{
    @Mock
    private AwsClientFactory awsClientFactory;

    @Mock
    private AwsHelper awsHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @InjectMocks
    private HerdJmsDestinationResolver herdJmsDestinationResolverImpl;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testResolveDestinationInvalidSqsDestinationIdentifier() throws Exception
    {
        // Create AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, NO_HTTP_PROXY_HOST, NO_HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1);

        // Mock objects required for this test.
        AmazonSQS amazonSQS = mock(AmazonSQS.class);
        GetQueueUrlResult getQueueUrlResult = mock(GetQueueUrlResult.class);
        Session session = mock(Session.class);

        // Mock calls to external methods.
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsClientFactory.getAmazonSQSClient(awsParamsDto)).thenReturn(amazonSQS);
        when(amazonSQS.getQueueUrl(EMPTY_STRING)).thenReturn(getQueueUrlResult);
        when(getQueueUrlResult.getQueueUrl()).thenReturn(AWS_SQS_QUEUE_URL);
        when(session.createQueue(AWS_SQS_QUEUE_URL)).thenThrow(new JMSException(ERROR_MESSAGE));

        // Try to call the method under test.
        try
        {
            herdJmsDestinationResolverImpl.resolveDestinationName(session, I_DO_NOT_EXIST, false);
            fail();
        }
        catch (IllegalStateException ex)
        {
            assertEquals("Failed to resolve the SQS queue: \"\".", ex.getMessage());
        }

        // Verify calls to external methods.
        verify(awsHelper).getAwsParamsDto();
        verify(awsClientFactory).getAmazonSQSClient(awsParamsDto);
        verify(amazonSQS).getQueueUrl(EMPTY_STRING);
        verify(getQueueUrlResult).getQueueUrl();
        verify(session).createQueue(AWS_SQS_QUEUE_URL);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testResolveDestinationSqsQueueNoExists() throws Exception
    {
        // Create AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, NO_HTTP_PROXY_HOST, NO_HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1);

        // Mock objects required for this test.
        AmazonSQS amazonSQS = mock(AmazonSQS.class);
        GetQueueUrlResult getQueueUrlResult = mock(GetQueueUrlResult.class);
        Session session = mock(Session.class);

        // Mock calls to external methods.
        when(configurationHelper.getProperty(ConfigurationValue.HERD_NOTIFICATION_SQS_INCOMING_QUEUE_NAME)).thenReturn(AWS_SQS_QUEUE_NAME);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsClientFactory.getAmazonSQSClient(awsParamsDto)).thenReturn(amazonSQS);
        when(amazonSQS.getQueueUrl(AWS_SQS_QUEUE_NAME)).thenReturn(getQueueUrlResult);
        when(getQueueUrlResult.getQueueUrl()).thenReturn(AWS_SQS_QUEUE_URL);
        when(session.createQueue(AWS_SQS_QUEUE_URL)).thenThrow(new JMSException(ERROR_MESSAGE));

        // Try to call the method under test.
        try
        {
            herdJmsDestinationResolverImpl.resolveDestinationName(session, HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING, false);
            fail();
        }
        catch (IllegalStateException ex)
        {
            assertEquals(String.format("Failed to resolve the SQS queue: \"%s\".", AWS_SQS_QUEUE_NAME), ex.getMessage());
        }

        // Verify calls to external methods.
        verify(configurationHelper).getProperty(ConfigurationValue.HERD_NOTIFICATION_SQS_INCOMING_QUEUE_NAME);
        verify(awsHelper).getAwsParamsDto();
        verify(awsClientFactory).getAmazonSQSClient(awsParamsDto);
        verify(amazonSQS).getQueueUrl(AWS_SQS_QUEUE_NAME);
        verify(getQueueUrlResult).getQueueUrl();
        verify(session).createQueue(AWS_SQS_QUEUE_URL);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testResolveDestinationSqsQueueNameNotConfigured()
    {
        // Mock calls to external methods.
        when(configurationHelper.getProperty(ConfigurationValue.HERD_NOTIFICATION_SQS_INCOMING_QUEUE_NAME)).thenReturn(EMPTY_STRING);

        // Try to call the method under test.
        try
        {
            herdJmsDestinationResolverImpl.resolveDestinationName(null, HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING, false);
            fail();
        }
        catch (IllegalStateException ex)
        {
            assertEquals(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.HERD_NOTIFICATION_SQS_INCOMING_QUEUE_NAME.getKey()), ex.getMessage());
        }

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.HERD_NOTIFICATION_SQS_INCOMING_QUEUE_NAME);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testResolveDestinationHerdIncoming() throws Exception
    {
        runTestResolveDestinationTest(HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING, ConfigurationValue.HERD_NOTIFICATION_SQS_INCOMING_QUEUE_NAME);
    }

    @Test
    public void testResolveDestinationStoragePolicy() throws Exception
    {
        runTestResolveDestinationTest(HerdJmsDestinationResolver.SQS_DESTINATION_STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE,
            ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME);
    }

    @Test
    public void testResolveDestinationSampleData() throws Exception
    {
        runTestResolveDestinationTest(HerdJmsDestinationResolver.SQS_DESTINATION_SAMPLE_DATA_QUEUE, ConfigurationValue.SAMPLE_DATA_SQS_QUEUE_NAME);
    }

    @Test
    public void testResolveDestinationSearchIndexUpdate() throws Exception
    {
        runTestResolveDestinationTest(HerdJmsDestinationResolver.SQS_DESTINATION_SEARCH_INDEX_UPDATE_QUEUE,
            ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);
    }

    /**
     * Executes happy path test for the specified SQS destination identifier.
     *
     * @param sqsDestinationIdentifier the SQS destination identifier
     * @param queueNameConfigurationValue the configuration value for the relative SQS destination
     */
    private void runTestResolveDestinationTest(String sqsDestinationIdentifier, ConfigurationValue queueNameConfigurationValue) throws Exception
    {
        // Create AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, NO_HTTP_PROXY_HOST, NO_HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1);

        // Mock objects required for this test.
        AmazonSQS amazonSQS = mock(AmazonSQS.class);
        GetQueueUrlResult getQueueUrlResult = mock(GetQueueUrlResult.class);
        Session session = mock(Session.class);
        Queue queue = mock(Queue.class);

        // Mock calls to external methods.
        when(configurationHelper.getProperty(queueNameConfigurationValue)).thenReturn(AWS_SQS_QUEUE_NAME);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(awsClientFactory.getAmazonSQSClient(awsParamsDto)).thenReturn(amazonSQS);
        when(amazonSQS.getQueueUrl(AWS_SQS_QUEUE_NAME)).thenReturn(getQueueUrlResult);
        when(getQueueUrlResult.getQueueUrl()).thenReturn(AWS_SQS_QUEUE_URL);
        when(session.createQueue(AWS_SQS_QUEUE_URL)).thenReturn(queue);

        // Call the method under test.
        Destination result = herdJmsDestinationResolverImpl.resolveDestinationName(session, sqsDestinationIdentifier, false);

        // Validate the response.
        assertEquals(result, queue);

        // Verify calls to external methods.
        verify(configurationHelper).getProperty(queueNameConfigurationValue);
        verify(awsHelper).getAwsParamsDto();
        verify(awsClientFactory).getAmazonSQSClient(awsParamsDto);
        verify(amazonSQS).getQueueUrl(AWS_SQS_QUEUE_NAME);
        verify(getQueueUrlResult).getQueueUrl();
        verify(session).createQueue(AWS_SQS_QUEUE_URL);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(awsClientFactory, awsHelper, configurationHelper);
    }
}
