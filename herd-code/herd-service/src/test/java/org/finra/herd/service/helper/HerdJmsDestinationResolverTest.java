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

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the HerdJmsDestinationResolver.
 */
public class HerdJmsDestinationResolverTest extends AbstractServiceTest
{
    @Autowired
    HerdJmsDestinationResolver herdJmsDestinationResolver;

    @Test
    public void testResolveDestinationNameNoExists() throws Exception
    {
        try
        {
            herdJmsDestinationResolver.resolveDestinationName(null, "queue_not_exists", false);
            fail("Should throw an IllegalStateException.");
        }
        catch (IllegalStateException ex)
        {
            assertEquals(String.format("Failed to resolve the SQS queue: \"%s\".", ""), ex.getMessage());
        }
    }

    @Test
    public void testResolveDestinationNameNoQueueExists() throws Exception
    {
        try
        {
            herdJmsDestinationResolver.resolveDestinationName(null, HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING, false);
            fail("Should throw an IllegalStateException.");
        }
        catch (IllegalStateException ex)
        {
            assertTrue("\"Failed to resolve the SQS queue:\" error message doesn't match", ex.getMessage().startsWith("Failed to resolve the SQS queue:"));
        }
    }

    @Test
    public void testResolveDestinationNameNoConfig() throws Exception
    {
        // Override configuration.
        removeReloadablePropertySourceFromEnvironment();

        try
        {
            herdJmsDestinationResolver.resolveDestinationName(null, HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING, false);
            fail("Should throw an IllegalStateException.");
        }
        catch (IllegalStateException ex)
        {
            assertEquals(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.HERD_NOTIFICATION_SQS_INCOMING_QUEUE_NAME.getKey()), ex.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testResolveDestinationHerdIncoming() throws Exception
    {
        Session session = mock(Session.class);
        Queue queue = mock(Queue.class);

        // Mock the call to external methods
        when(session.createQueue(any())).thenReturn(queue);

        // Call the method under test
        Destination destination = herdJmsDestinationResolver.resolveDestinationName(session, HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING, false);

        assertThat("Destination is null.", destination, not(nullValue()));

        // Verify the calls to external methods
        verify(session, times(1)).createQueue(any());
    }

    @Test
    public void testResolveDestinationStoragePolicy() throws Exception
    {
        Session session = mock(Session.class);
        Queue queue = mock(Queue.class);

        // Mock the call to external methods
        when(session.createQueue(any())).thenReturn(queue);

        // Call the method under test
        Destination destination =
            herdJmsDestinationResolver.resolveDestinationName(session, HerdJmsDestinationResolver.SQS_DESTINATION_STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE, false);

        assertThat("Destination is null.", destination, not(nullValue()));

        // Verify the calls to external methods
        verify(session, times(1)).createQueue(any());
    }

    @Test
    public void testResolveDestinationSampleData() throws Exception
    {
        Session session = mock(Session.class);
        Queue queue = mock(Queue.class);

        // Mock the call to external methods
        when(session.createQueue(any())).thenReturn(queue);

        // Call the method under test
        Destination destination =
            herdJmsDestinationResolver.resolveDestinationName(session, HerdJmsDestinationResolver.SQS_DESTINATION_SAMPLE_DATA_QUEUE, false);

        assertThat("Destination is null.", destination, not(nullValue()));

        // Verify the calls to external methods
        verify(session, times(1)).createQueue(any());
    }

    @Test
    public void testResolveDestinationSearchIndexUpdate() throws Exception
    {
        Session session = mock(Session.class);
        Queue queue = mock(Queue.class);

        // Mock the call to external methods
        when(session.createQueue(any())).thenReturn(queue);

        // Call the method under test
        Destination destination =
            herdJmsDestinationResolver.resolveDestinationName(session, HerdJmsDestinationResolver.SQS_DESTINATION_SEARCH_INDEX_UPDATE_QUEUE, false);

        assertThat("Destination is null.", destination, not(nullValue()));

        // Verify the calls to external methods
        verify(session, times(1)).createQueue(any());
    }
}
