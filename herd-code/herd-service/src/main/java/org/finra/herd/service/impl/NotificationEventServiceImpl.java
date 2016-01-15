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
package org.finra.herd.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.BusinessObjectDataNotificationRegistrationDao;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.BusinessObjectDataNotificationEventParamsDto;
import org.finra.herd.model.dto.NotificationEventParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.NotificationActionEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationJobActionEntity;
import org.finra.herd.model.jpa.NotificationTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.NotificationActionService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.HerdHelper;
import org.finra.herd.service.helper.NotificationActionFactory;

/**
 * The notification event service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class NotificationEventServiceImpl implements NotificationEventService
{
    private static final Logger LOGGER = Logger.getLogger(NotificationEventServiceImpl.class);

    @Autowired
    private NotificationActionFactory notificationActionFactory;

    @Autowired
    private HerdHelper herdHelper;

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDataNotificationRegistrationDao businessObjectDataNotificationRegistrationDao;

    /**
     * {@inheritDoc}
     */
    @Async
    public Future<Void> processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata eventType, BusinessObjectDataKey key,
        String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        processBusinessObjectDataNotificationEventSync(eventType, key, newBusinessObjectDataStatus, oldBusinessObjectDataStatus);

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they
        // can call "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }

    /**
     * {@inheritDoc}
     */
    public List<Object> processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EventTypesBdata eventType,
        BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        // Retrieve the notifications matching the event type.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationRegistrationEntities =
            businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrations(eventType.name(), businessObjectDataKey,
                newBusinessObjectDataStatus, oldBusinessObjectDataStatus);

        BusinessObjectDataEntity businessObjectDataEntity = herdDao.getBusinessObjectDataByAltKey(businessObjectDataKey);

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

        return processBusinessObjectDataNotifications(eventType.name(), notificationRegistrationsToProcess,
            businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity), newBusinessObjectDataStatus, oldBusinessObjectDataStatus);
    }

    private List<Object> processBusinessObjectDataNotifications(String eventType,
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotifications, BusinessObjectData businessObjectData,
        String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        List<Object> notificationActions = new ArrayList<>();

        // Build a list of partition value that includes primary and sub-partition values, if any are specified in the business object data key.
        List<String> partitionValues = getPartitionValues(businessObjectData);

        // Get a list of partition columns from the associated business object format.
        List<String> partitionColumnNames = null;
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(businessObjectData.getNamespace(), businessObjectData.getBusinessObjectDefinitionName(),
                businessObjectData.getBusinessObjectFormatUsage(), businessObjectData.getBusinessObjectFormatFileType(),
                businessObjectData.getBusinessObjectFormatVersion()));
        if (businessObjectFormatEntity != null)
        {
            // Get business object format model object to directly access schema columns and partitions.
            BusinessObjectFormat businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

            // Proceed only if this format has schema with partition columns specified.
            if (businessObjectFormat.getSchema() != null && !CollectionUtils.isEmpty(businessObjectFormat.getSchema().getPartitions()))
            {
                // Do not provide more partition column names than there are primary and
                // sub-partition values that this business object data is registered with.
                partitionColumnNames = new ArrayList<>();
                List<SchemaColumn> partitionColumns = businessObjectFormat.getSchema().getPartitions();
                for (int i = 0; i < Math.min(partitionValues.size(), partitionColumns.size()); i++)
                {
                    partitionColumnNames.add(partitionColumns.get(i).getName());
                }
            }
        }

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
                    notificationEventParams.setBusinessObjectData(businessObjectData);
                    notificationEventParams.setPartitionColumnNames(partitionColumnNames);
                    notificationEventParams
                        .setStorageName(businessObjectDataNotification.getStorage() == null ? null : businessObjectDataNotification.getStorage().getName());
                    notificationEventParams.setPartitionValues(partitionValues);
                    notificationEventParams.setNewBusinessObjectDataStatus(newBusinessObjectDataStatus);
                    notificationEventParams.setOldBusinessObjectDataStatus(oldBusinessObjectDataStatus);

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
            LOGGER
                .error("Unexpected error occurred when triggering notification action with " + actionHandler.getIdentifyingInformation(params, herdHelper), e);
        }

        return null;
    }

    /**
     * Returns a list of primary and sub-partition values per specified business object data.
     *
     * @param businessObjectData the business object data
     *
     * @return the list of primary and sub-partition values
     */
    private List<String> getPartitionValues(BusinessObjectData businessObjectData)
    {
        List<String> partitionValues = new ArrayList<>();
        partitionValues.add(businessObjectData.getPartitionValue());
        partitionValues.addAll(businessObjectData.getSubPartitionValues());
        return partitionValues;
    }
}
