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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.jpa.JmsMessageEntity;

public class JmsMessageDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetOldestJmsMessage() throws Exception
    {
        // Prepare database entries required for testing.
        List<JmsMessageEntity> jmsMessageEntities = Arrays.asList(jmsMessageDaoTestHelper.createJmsMessageEntity(JMS_QUEUE_NAME, MESSAGE_TEXT),
            jmsMessageDaoTestHelper.createJmsMessageEntity(JMS_QUEUE_NAME_2, MESSAGE_TEXT_2));

        // Retrieve the oldest JMS message.
        JmsMessageEntity oldestJmsMessageEntity = jmsMessageDao.getOldestJmsMessage();

        // Validate the results.
        assertNotNull(oldestJmsMessageEntity);
        assertEquals(jmsMessageEntities.get(0).getId(), oldestJmsMessageEntity.getId());
    }

    @Test
    public void testGetOldestJmsMessageQueueIsEmpty() throws Exception
    {
        // Try to retrieve the oldest JMS message from an empty queue table.
        JmsMessageEntity oldestJmsMessageEntity = jmsMessageDao.getOldestJmsMessage();

        // Validate the results.
        assertNull(oldestJmsMessageEntity);
    }
}
