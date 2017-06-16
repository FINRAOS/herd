/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this message except in compliance with the License.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

public class MessageTypeDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetMessageTypeByCode()
    {
        // Create database entities required for testing.
        messageTypeDaoTestHelper.createMessageTypeEntity(MESSAGE_TYPE);

        // Retrieve the relative message type entities and validate the results.
        assertEquals(MESSAGE_TYPE, messageTypeDao.getMessageTypeByCode(MESSAGE_TYPE).getCode());

        // Test case insensitivity of the message type code.
        assertEquals(MESSAGE_TYPE, messageTypeDao.getMessageTypeByCode(MESSAGE_TYPE.toUpperCase()).getCode());
        assertEquals(MESSAGE_TYPE, messageTypeDao.getMessageTypeByCode(MESSAGE_TYPE.toLowerCase()).getCode());

        // Confirm negative results when using non-existing message type code.
        assertNull(messageTypeDao.getMessageTypeByCode(I_DO_NOT_EXIST));
    }

    @Test
    public void testGetMessageTypeByCodeMultipleRecordsFound()
    {
        // Create relative database entities.
        messageTypeDaoTestHelper.createMessageTypeEntity(MESSAGE_TYPE.toUpperCase());
        messageTypeDaoTestHelper.createMessageTypeEntity(MESSAGE_TYPE.toLowerCase());

        // Try to retrieve message type entity.
        try
        {
            messageTypeDao.getMessageTypeByCode(MESSAGE_TYPE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one message type with code \"%s\".", MESSAGE_TYPE), e.getMessage());
        }
    }
}
