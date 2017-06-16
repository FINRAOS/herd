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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the MessageTypeDaoHelper class.
 */
public class MessageTypeDaoHelperTest extends AbstractServiceTest
{
    @Test
    public void testGetMessageTypeEntity()
    {
        // Create and persist a message type entity.
        MessageTypeEntity messageTypeEntity = messageTypeDaoTestHelper.createMessageTypeEntity(MESSAGE_TYPE);

        // Retrieve the message type entity and validate the result.
        assertEquals(messageTypeEntity, messageTypeDaoHelper.getMessageTypeEntity(MESSAGE_TYPE));

        // Test case insensitivity of the message type code.
        assertEquals(messageTypeEntity, messageTypeDaoHelper.getMessageTypeEntity(MESSAGE_TYPE.toUpperCase()));
        assertEquals(messageTypeEntity, messageTypeDaoHelper.getMessageTypeEntity(MESSAGE_TYPE.toLowerCase()));

        // Try to retrieve a non existing message type.
        try
        {
            messageTypeDaoHelper.getMessageTypeEntity(I_DO_NOT_EXIST);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Message type with code \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }
    }
}
