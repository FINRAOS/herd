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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.BusinessObjectDataNotificationRegistrationDao;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.StorageUnitNotificationRegistrationDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.BusinessObjectDataNotificationEventParamsDto;
import org.finra.herd.model.dto.NotificationEventParamsDto;
import org.finra.herd.model.dto.StorageUnitNotificationEventParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.NotificationActionEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationJobActionEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.model.jpa.NotificationTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity;
import org.finra.herd.service.NotificationActionService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.NotificationActionFactory;

/**
 * The notification event service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class NotificationEventServiceImpl implements NotificationEventService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationEventServiceImpl.class);

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDataNotificationRegistrationDao businessObjectDataNotificationRegistrationDao;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private NotificationActionFactory notificationActionFactory;

    @Autowired
    private StorageUnitNotificationRegistrationDao storageUnitNotificationRegistrationDao;

    @Override
    @Async
    public Future<Void> processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata notificationEventType,
        BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        /*
         * Need to clear the security context here since the current thread may have been reused, which may might have left over its security context. If we do
         * not clear the security context, any subsequent calls may be restricted by the permissions given to the previous thread's security context.
         */
        SecurityContextHolder.clearContext();

        processBusinessObjectDataNotificationEventSync(notificationEventType, businessObjectDataKey, newBusinessObjectDataStatus, oldBusinessObjectDataStatus);

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they
        // can call "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }

    @Override
    public List<Object> processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EventTypesBdata notificationEventType,
        BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        // Retrieve all matching business object data notification registrations with enabled status.
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationRegistrationEntities =
            businessObjectDataNotificationRegistrationDao
                .getBusinessObjectDataNotificationRegistrations(notificationEventType.name(), businessObjectDataKey, newBusinessObjectDataStatus,
                    oldBusinessObjectDataStatus, NotificationRegistrationStatusEntity.ENABLED);

        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);

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

        return processBusinessObjectDataNotifications(notificationEventType.name(), notificationRegistrationsToProcess,
            businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity), newBusinessObjectDataStatus, oldBusinessObjectDataStatus);
    }

    @Override
    @Async
    public Future<Void> processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit notificationEventType,
        BusinessObjectDataKey businessObjectDataKey, String storageName, String newStorageUnitStatus, String oldStorageUnitStatus)
    {
        /*
         * Need to clear the security context here since the current thread may have been reused, which may might have left over its security context. If we do
         * not clear the security context, any subsequent calls may be restricted by the permissions given to the previous thread's security context.
         */
        SecurityContextHolder.clearContext();

        processStorageUnitNotificationEventSync(notificationEventType, businessObjectDataKey, storageName, newStorageUnitStatus, oldStorageUnitStatus);

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they
        // can call "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }

    @Override
    public List<Object> processStorageUnitNotificationEventSync(NotificationEventTypeEntity.EventTypesStorageUnit notificationEventType,
        BusinessObjectDataKey businessObjectDataKey, String storageName, String newStorageUnitStatus, String oldStorageUnitStatus)
    {
        // Retrieve all matching storage unit notification registrations with enabled status.
        List<StorageUnitNotificationRegistrationEntity> storageUnitNotificationRegistrationEntities = storageUnitNotificationRegistrationDao
            .getStorageUnitNotificationRegistrations(notificationEventType.name(), businessObjectDataKey, storageName, newStorageUnitStatus,
                oldStorageUnitStatus, NotificationRegistrationStatusEntity.ENABLED);

        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);

        return processStorageUnitNotifications(notificationEventType.name(), storageUnitNotificationRegistrationEntities,
            businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity), storageName, newStorageUnitStatus, oldStorageUnitStatus);
    }

    private List<Object> processBusinessObjectDataNotifications(String notificationEventType,
        List<BusinessObjectDataNotificationRegistrationEntity> businessObjectDataNotificationRegistrationEntities, BusinessObjectData businessObjectData,
        String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        List<Object> notificationActions = new ArrayList<>();

        // Build a list of partition value that includes primary and sub-partition values, if any are specified in the business object data key.
        List<String> partitionValues = businessObjectDataHelper.getPrimaryAndSubPartitionValues(businessObjectData);

        // Get a list of partition columns from the associated business object format.
        List<String> partitionColumnNames = null;
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
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

        for (BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistration : businessObjectDataNotificationRegistrationEntities)
        {
            // Retrieve the job notification actions needed to be triggered.
            for (NotificationActionEntity notificationActionEntity : businessObjectDataNotificationRegistration.getNotificationActions())
            {
                // Trigger the job action.
                if (notificationActionEntity instanceof NotificationJobActionEntity)
                {
                    NotificationJobActionEntity notificationJobActionEntity = (NotificationJobActionEntity) notificationActionEntity;
                    BusinessObjectDataNotificationEventParamsDto notificationEventParams = new BusinessObjectDataNotificationEventParamsDto();
                    notificationEventParams.setBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistration);
                    notificationEventParams.setNotificationJobAction(notificationJobActionEntity);
                    notificationEventParams.setEventType(notificationEventType);
                    notificationEventParams.setBusinessObjectData(businessObjectData);
                    notificationEventParams.setPartitionColumnNames(partitionColumnNames);
                    notificationEventParams.setStorageName(businessObjectDataNotificationRegistration.getStorage() == null ? null :
                        businessObjectDataNotificationRegistration.getStorage().getName());
                    notificationEventParams.setPartitionValues(partitionValues);
                    notificationEventParams.setNewBusinessObjectDataStatus(newBusinessObjectDataStatus);
                    notificationEventParams.setOldBusinessObjectDataStatus(oldBusinessObjectDataStatus);

                    notificationActions
                        .add(triggerNotificationAction(NotificationTypeEntity.NOTIFICATION_TYPE_BDATA, notificationEventType, notificationEventParams));
                }
            }
        }

        return notificationActions;
    }

    private List<Object> processStorageUnitNotifications(String notificationEventType,
        List<StorageUnitNotificationRegistrationEntity> storageUnitNotificationRegistrationEntities, BusinessObjectData businessObjectData, String storageName,
        String newStorageUnitStatus, String oldStorageUnitStatus)
    {
        List<Object> notificationActions = new ArrayList<>();

        // Build a list of partition value that includes primary and sub-partition values, if any are specified in the business object data key.
        List<String> partitionValues = businessObjectDataHelper.getPrimaryAndSubPartitionValues(businessObjectData);

        // Get a list of partition columns from the associated business object format.
        List<String> partitionColumnNames = null;
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
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

        for (StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistration : storageUnitNotificationRegistrationEntities)
        {
            // Retrieve the job notification actions needed to be triggered.
            for (NotificationActionEntity notificationActionEntity : storageUnitNotificationRegistration.getNotificationActions())
            {
                // Trigger the job action.
                if (notificationActionEntity instanceof NotificationJobActionEntity)
                {
                    NotificationJobActionEntity notificationJobActionEntity = (NotificationJobActionEntity) notificationActionEntity;
                    StorageUnitNotificationEventParamsDto notificationEventParams = new StorageUnitNotificationEventParamsDto();
                    notificationEventParams.setStorageUnitNotificationRegistration(storageUnitNotificationRegistration);
                    notificationEventParams.setNotificationJobAction(notificationJobActionEntity);
                    notificationEventParams.setEventType(notificationEventType);
                    notificationEventParams.setBusinessObjectData(businessObjectData);
                    notificationEventParams.setPartitionColumnNames(partitionColumnNames);
                    notificationEventParams.setStorageName(storageName);
                    notificationEventParams.setPartitionValues(partitionValues);
                    notificationEventParams.setNewStorageUnitStatus(newStorageUnitStatus);
                    notificationEventParams.setOldStorageUnitStatus(oldStorageUnitStatus);

                    notificationActions
                        .add(triggerNotificationAction(NotificationTypeEntity.NOTIFICATION_TYPE_STORAGE_UNIT, notificationEventType, notificationEventParams));
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
            LOGGER.error("Unexpected error occurred when triggering notification action with " +
                actionHandler.getIdentifyingInformation(params, businessObjectDataHelper), e);
        }

        return null;
    }
}
