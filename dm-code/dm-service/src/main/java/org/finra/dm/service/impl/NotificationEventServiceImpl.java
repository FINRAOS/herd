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
package org.finra.dm.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.dm.dao.DmDao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.dto.BusinessObjectDataNotificationEventParamsDto;
import org.finra.dm.model.dto.NotificationEventParamsDto;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.dm.model.jpa.NotificationActionEntity;
import org.finra.dm.model.jpa.NotificationEventTypeEntity;
import org.finra.dm.model.jpa.NotificationJobActionEntity;
import org.finra.dm.model.jpa.NotificationTypeEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.service.NotificationActionService;
import org.finra.dm.service.NotificationEventService;
import org.finra.dm.service.helper.DmHelper;
import org.finra.dm.service.helper.NotificationActionFactory;

/**
 * The notification event service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class NotificationEventServiceImpl implements NotificationEventService
{
    private static final Logger LOGGER = Logger.getLogger(NotificationEventServiceImpl.class);

    @Autowired
    private NotificationActionFactory notificationActionFactory;

    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private DmDao dmDao;

    /**
     * Asynchronously handles the notification for the business object data changes.
     *
     * @param eventType the event type
     * @param key the business object data key.
     *
     * @return a future to know the asynchronous state of this method.
     */
    @Async
    public Future<Void> processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EVENT_TYPES_BDATA eventType, BusinessObjectDataKey key)
    {
        processBusinessObjectDataNotificationEventSync(eventType, key);

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they
        // can call "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }

    /**
     * Synchronously handles the notification for the business object data changes.
     *
     * @param eventType the event type
     * @param key the business object data key.
     *
     * @return a list of actions that were performed. For example: Job for a jobAction.
     */
    public List<Object> processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EVENT_TYPES_BDATA eventType, BusinessObjectDataKey key)
    {
        // Retrieve the notifications matching the event type.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationRegistrationEntities =
            dmDao.getBusinessObjectDataNotificationRegistrations(eventType.name(), key);

        BusinessObjectDataEntity businessObjectDataEntity = dmDao.getBusinessObjectDataByAltKey(key);

        List<BusinessObjectDataNotificationRegistrationEntity> notificationRegistrationsToProcess = new ArrayList<>();

        for (BusinessObjectDataNotificationRegistrationEntity notificationRegistration : businessObjectDataNotificationRegistrationEntities)
        {
            if (notificationRegistration.getStorage() == null)
            {
                notificationRegistrationsToProcess.add(notificationRegistration);
            }
            else
            {
                String filterStorageName = notificationRegistration.getStorage().getName();
                for (StorageUnitEntity storageUnitEntity : businessObjectDataEntity.getStorageUnits())
                {
                    if (filterStorageName.equalsIgnoreCase(storageUnitEntity.getStorage().getName()))
                    {
                        notificationRegistrationsToProcess.add(notificationRegistration);
                        break;
                    }
                }
            }
        }

        return processBusinessObjectDataNotifications(eventType.name(), notificationRegistrationsToProcess, key);
    }

    private List<Object> processBusinessObjectDataNotifications(String eventType,
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotifications, BusinessObjectDataKey key)
    {
        List<Object> notificationActions = new ArrayList<>();

        for (BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotification : businessObjectDataNotifications)
        {
            // Retrieve the job notification actions needed to be triggered.
            for (NotificationActionEntity notificationActionEntity : businessObjectDataNotification.getNotificationActions())
            {
                // Trigger the job action.
                if (notificationActionEntity instanceof NotificationJobActionEntity)
                {
                    NotificationJobActionEntity notificationJobActionEntity = (NotificationJobActionEntity) notificationActionEntity;
                    BusinessObjectDataNotificationEventParamsDto notificationEventParams = new BusinessObjectDataNotificationEventParamsDto();
                    notificationEventParams.setBusinessObjectDataNotificationRegistration(businessObjectDataNotification);
                    notificationEventParams.setNotificationJobAction(notificationJobActionEntity);
                    notificationEventParams.setEventType(eventType);
                    notificationEventParams.setBusinessObjectDataKey(key);
                    notificationEventParams
                        .setStorageName(businessObjectDataNotification.getStorage() == null ? null : businessObjectDataNotification.getStorage().getName());

                    notificationActions.add(triggerNotificationAction(NotificationTypeEntity.NOTIFICATION_TYPE_BDATA, eventType, notificationEventParams));
                }
            }
        }

        return notificationActions;
    }

    private Object triggerNotificationAction(String notificationType, String actionType, NotificationEventParamsDto params)
    {
        NotificationActionService actionHandler = notificationActionFactory.getNotificationActionHandler(notificationType, actionType);

        try
        {
            return actionHandler.performNotificationAction(params);
        }
        catch (Exception e)
        {
            // Log the error.
            LOGGER.error("Unexpected error occurred when triggering notification action with " + actionHandler.getIdentifyingInformation(params, dmHelper), e);
        }

        return null;
    }
}
