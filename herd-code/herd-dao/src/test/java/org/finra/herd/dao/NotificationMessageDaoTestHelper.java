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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.model.jpa.NotificationMessageEntity;

@Component
public class NotificationMessageDaoTestHelper
{
    @Autowired
    private MessageTypeDao messageTypeDao;

    @Autowired
    private MessageTypeDaoTestHelper messageTypeDaoTestHelper;

    @Autowired
    private NotificationMessageDao notificationMessageDao;

    /**
     * Creates and persists a new notification message entity.
     *
     * @param messageDestination the destination of the message
     * @param messageText the text of the message
     *
     * @return the newly created notification message entity
     */
    public NotificationMessageEntity createNotificationMessageEntity(String messageType, String messageDestination, String messageText)
    {
        // Create a message type entity if needed.
        MessageTypeEntity messageTypeEntity = messageTypeDao.getMessageTypeByCode(messageType);
        if (messageTypeEntity == null)
        {
            messageTypeEntity = messageTypeDaoTestHelper.createMessageTypeEntity(messageType);
        }

        // Create a notification message entity.
        NotificationMessageEntity notificationMessageEntity = new NotificationMessageEntity();
        notificationMessageEntity.setMessageType(messageTypeEntity);
        notificationMessageEntity.setMessageDestination(messageDestination);
        notificationMessageEntity.setMessageText(messageText);

        // Persist and return the newly created entity.
        return notificationMessageDao.saveAndRefresh(notificationMessageEntity);
    }
}
