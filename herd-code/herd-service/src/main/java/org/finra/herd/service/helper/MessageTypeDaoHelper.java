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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.MessageTypeDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.MessageTypeEntity;

/**
 * Helper for message type operations which require DAO.
 */
@Component
public class MessageTypeDaoHelper
{
    @Autowired
    private MessageTypeDao messageTypeDao;

    /**
     * Gets a message type entity by its code and ensure it exists.
     *
     * @param code the message type code (case insensitive)
     *
     * @return the message type entity
     */
    public MessageTypeEntity getMessageTypeEntity(String code)
    {
        MessageTypeEntity messageTypeEntity = messageTypeDao.getMessageTypeByCode(code);

        if (messageTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Message type with code \"%s\" doesn't exist.", code));
        }

        return messageTypeEntity;
    }
}
