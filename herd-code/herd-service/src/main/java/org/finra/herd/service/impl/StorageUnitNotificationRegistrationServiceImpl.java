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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.StorageUnitNotificationRegistrationDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.NamespacePermissions;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.StorageUnitNotificationFilter;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistration;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationUpdateRequest;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NotificationActionEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationJobActionEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.StorageUnitNotificationRegistrationService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.FileTypeDaoHelper;
import org.finra.herd.service.helper.JobDefinitionDaoHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NotificationEventTypeDaoHelper;
import org.finra.herd.service.helper.NotificationRegistrationStatusDaoHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageUnitNotificationRegistrationDaoHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

/**
 * The storage unit notification registration service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StorageUnitNotificationRegistrationServiceImpl implements StorageUnitNotificationRegistrationService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private FileTypeDaoHelper fileTypeDaoHelper;

    @Autowired
    private JobDefinitionDaoHelper jobDefinitionDaoHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private NotificationEventTypeDaoHelper notificationEventTypeDaoHelper;

    @Autowired
    private NotificationRegistrationStatusDaoHelper notificationRegistrationStatusDaoHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StorageUnitNotificationRegistrationDao storageUnitNotificationRegistrationDao;

    @Autowired
    private StorageUnitNotificationRegistrationDaoHelper storageUnitNotificationRegistrationDaoHelper;

    @Autowired
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    @NamespacePermissions(
        {@NamespacePermission(fields = "#request?.storageUnitNotificationRegistrationKey?.namespace", permissions = NamespacePermissionEnum.WRITE),
            @NamespacePermission(fields = "#request?.storageUnitNotificationFilter?.namespace", permissions = NamespacePermissionEnum.READ),
            @NamespacePermission(fields = "#request?.jobActions?.![namespace]", permissions = NamespacePermissionEnum.EXECUTE)})
    @Override
    public StorageUnitNotificationRegistration createStorageUnitNotificationRegistration(StorageUnitNotificationRegistrationCreateRequest request)
    {
        // Validate and trim the request parameters.
        validateStorageUnitNotificationRegistrationCreateRequest(request);

        // Get the notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = request.getStorageUnitNotificationRegistrationKey();

        // Retrieve and ensure that notification registration namespace exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(notificationRegistrationKey.getNamespace());

        // Retrieve and validate the notification event type entity.
        NotificationEventTypeEntity notificationEventTypeEntity = getAndValidateNotificationEventTypeEntity(request.getStorageUnitEventType());

        // Validate the notification event type.
        Assert.isTrue(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().equalsIgnoreCase(request.getStorageUnitEventType()),
            String.format("Notification event type \"%s\" is not supported for storage unit notification registration.", request.getStorageUnitEventType()));

        // Get the storage unit notification filter.
        StorageUnitNotificationFilter filter = request.getStorageUnitNotificationFilter();

        // Retrieve and ensure that business object definition exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper
            .getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(filter.getNamespace(), filter.getBusinessObjectDefinitionName()));

        // If specified, retrieve and ensure that file type exists.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(filter.getBusinessObjectFormatFileType()))
        {
            fileTypeEntity = fileTypeDaoHelper.getFileTypeEntity(filter.getBusinessObjectFormatFileType());
        }

        // Retrieve and ensure that storage exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(filter.getStorageName());

        // If specified, retrieve and ensure that new storage unit status exists.
        StorageUnitStatusEntity newStorageUnitStatus = null;
        if (StringUtils.isNotBlank(filter.getNewStorageUnitStatus()))
        {
            newStorageUnitStatus = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(filter.getNewStorageUnitStatus());
        }

        // If specified, retrieve and ensure that old storage unit status exists.
        StorageUnitStatusEntity oldStorageUnitStatus = null;
        if (StringUtils.isNotBlank(filter.getOldStorageUnitStatus()))
        {
            oldStorageUnitStatus = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(filter.getOldStorageUnitStatus());
        }

        // Validate the existence of job definitions.
        // TODO: We need to add a null/empty list check here, if/when list of job actions will become optional (due to addition of other action types).
        for (JobAction jobAction : request.getJobActions())
        {
            // Ensure that job definition exists.
            jobDefinitionDaoHelper.getJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName());
        }

        // If specified, retrieve and validate the notification registration status entity. Otherwise, default it to ENABLED.
        NotificationRegistrationStatusEntity notificationRegistrationStatusEntity = notificationRegistrationStatusDaoHelper
            .getNotificationRegistrationStatusEntity(
                StringUtils.isNotBlank(request.getNotificationRegistrationStatus()) ? request.getNotificationRegistrationStatus() :
                    NotificationRegistrationStatusEntity.ENABLED);

        // Ensure a storage unit notification with the specified name doesn't already exist for the specified namespace.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity =
            storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(notificationRegistrationKey);
        if (storageUnitNotificationRegistrationEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create storage unit notification with name \"%s\" because it already exists for namespace \"%s\".",
                    notificationRegistrationKey.getNotificationName(), notificationRegistrationKey.getNamespace()));
        }

        // Create a storage unit notification registration entity from the request information.
        storageUnitNotificationRegistrationEntity =
            createStorageUnitNotificationEntity(namespaceEntity, notificationEventTypeEntity, businessObjectDefinitionEntity, fileTypeEntity, storageEntity,
                newStorageUnitStatus, oldStorageUnitStatus, request.getStorageUnitNotificationRegistrationKey(), request.getStorageUnitNotificationFilter(),
                request.getJobActions(), notificationRegistrationStatusEntity);

        // Persist the new entity.
        storageUnitNotificationRegistrationEntity = storageUnitNotificationRegistrationDao.saveAndRefresh(storageUnitNotificationRegistrationEntity);

        // Create and return the storage unit notification object from the persisted entity.
        return createStorageUnitNotificationFromEntity(storageUnitNotificationRegistrationEntity);
    }

    @NamespacePermission(fields = "#key?.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public StorageUnitNotificationRegistration deleteStorageUnitNotificationRegistration(NotificationRegistrationKey notificationRegistrationKey)
    {
        // Validate and trim the key.
        validateStorageUnitNotificationRegistrationKey(notificationRegistrationKey);

        // Retrieve and ensure that a storage unit notification exists with the specified key.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity =
            storageUnitNotificationRegistrationDaoHelper.getStorageUnitNotificationRegistrationEntity(notificationRegistrationKey);

        // Delete the storage unit notification.
        storageUnitNotificationRegistrationDao.delete(storageUnitNotificationRegistrationEntity);

        // Create and return the storage unit notification object from the deleted entity.
        return createStorageUnitNotificationFromEntity(storageUnitNotificationRegistrationEntity);
    }

    @NamespacePermission(fields = "#key?.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public StorageUnitNotificationRegistration getStorageUnitNotificationRegistration(NotificationRegistrationKey notificationRegistrationKey)
    {
        // Validate and trim the key.
        validateStorageUnitNotificationRegistrationKey(notificationRegistrationKey);

        // Retrieve and ensure that a storage unit notification exists with the specified key.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity =
            storageUnitNotificationRegistrationDaoHelper.getStorageUnitNotificationRegistrationEntity(notificationRegistrationKey);

        // Create and return the storage unit notification object from the persisted entity.
        return createStorageUnitNotificationFromEntity(storageUnitNotificationRegistrationEntity);
    }

    @Override
    public StorageUnitNotificationRegistrationKeys getStorageUnitNotificationRegistrationsByNamespace(String namespace)
    {
        String namespaceLocal = namespace;

        // Validate and trim the namespace value.
        Assert.hasText(namespaceLocal, "A namespace must be specified.");
        namespaceLocal = namespaceLocal.trim();

        // Ensure that this namespace exists.
        namespaceDaoHelper.getNamespaceEntity(namespaceLocal);

        // Create and populate a list of storage unit notification registration keys.
        StorageUnitNotificationRegistrationKeys storageUnitNotificationKeys = new StorageUnitNotificationRegistrationKeys();
        storageUnitNotificationKeys.getStorageUnitNotificationRegistrationKeys()
            .addAll(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationKeysByNamespace(namespaceLocal));

        return storageUnitNotificationKeys;
    }

    @NamespacePermission(fields = "#storageUnitNotificationFilter?.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public StorageUnitNotificationRegistrationKeys getStorageUnitNotificationRegistrationsByNotificationFilter(
        StorageUnitNotificationFilter storageUnitNotificationFilter)
    {
        // Validate and trim the storage unit notification filter parameters.
        validateStorageUnitNotificationFilterBusinessObjectDefinitionFields(storageUnitNotificationFilter);
        trimStorageUnitNotificationFilterBusinessObjectFormatFields(storageUnitNotificationFilter);

        // Create and populate a list of storage unit notification registration keys.
        StorageUnitNotificationRegistrationKeys storageUnitNotificationKeys = new StorageUnitNotificationRegistrationKeys();
        storageUnitNotificationKeys.getStorageUnitNotificationRegistrationKeys()
            .addAll(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationKeysByNotificationFilter(storageUnitNotificationFilter));

        return storageUnitNotificationKeys;
    }

    @NamespacePermissions({@NamespacePermission(fields = "#key?.namespace", permissions = NamespacePermissionEnum.WRITE),
        @NamespacePermission(fields = "#request?.storageUnitNotificationFilter?.namespace", permissions = NamespacePermissionEnum.READ),
        @NamespacePermission(fields = "#request?.jobActions?.![namespace]", permissions = NamespacePermissionEnum.EXECUTE)})
    @Override
    public StorageUnitNotificationRegistration updateStorageUnitNotificationRegistration(NotificationRegistrationKey notificationRegistrationKey,
        StorageUnitNotificationRegistrationUpdateRequest request)
    {
        // Validate and trim the key.
        validateStorageUnitNotificationRegistrationKey(notificationRegistrationKey);

        // Validate and trim the request parameters.
        validateStorageUnitNotificationRegistrationUpdateRequest(request);

        // Retrieve and ensure that a storage unit notification exists with the specified key.
        StorageUnitNotificationRegistrationEntity oldStorageUnitNotificationRegistrationEntity =
            storageUnitNotificationRegistrationDaoHelper.getStorageUnitNotificationRegistrationEntity(notificationRegistrationKey);
        String oldStorageUnitNotificationRegistrationName = oldStorageUnitNotificationRegistrationEntity.getName();

        // Retrieve the namespace with the specified namespace code.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(notificationRegistrationKey.getNamespace());

        // Retrieve and validate the notification event type entity.
        NotificationEventTypeEntity notificationEventTypeEntity = getAndValidateNotificationEventTypeEntity(request.getStorageUnitEventType());

        // Get the storage unit notification filter.
        StorageUnitNotificationFilter filter = request.getStorageUnitNotificationFilter();

        // Retrieve and ensure that business object definition exists. Since namespace is specified, retrieve a business object definition by it's key.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper
            .getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(filter.getNamespace(), filter.getBusinessObjectDefinitionName()));

        // If specified, retrieve and ensure that file type exists.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(filter.getBusinessObjectFormatFileType()))
        {
            fileTypeEntity = fileTypeDaoHelper.getFileTypeEntity(filter.getBusinessObjectFormatFileType());
        }

        // Retrieve and ensure that storage exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(filter.getStorageName());

        // If specified, retrieve and ensure that new storage unit status exists.
        StorageUnitStatusEntity newStorageUnitStatus = null;
        if (StringUtils.isNotBlank(filter.getNewStorageUnitStatus()))
        {
            newStorageUnitStatus = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(filter.getNewStorageUnitStatus());
        }

        // If specified, retrieve and ensure that old storage unit status exists.
        StorageUnitStatusEntity oldStorageUnitStatus = null;
        if (StringUtils.isNotBlank(filter.getOldStorageUnitStatus()))
        {
            oldStorageUnitStatus = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(filter.getOldStorageUnitStatus());
        }

        // Validate the existence of job definitions.
        // TODO: We need to add a null/empty list check here, if/when list of job actions will become optional (due to addition of other action types).
        for (JobAction jobAction : request.getJobActions())
        {
            // Ensure that job definition exists.
            jobDefinitionDaoHelper.getJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName());
        }

        // Retrieve and validate the notification registration status entity.
        NotificationRegistrationStatusEntity notificationRegistrationStatusEntity =
            notificationRegistrationStatusDaoHelper.getNotificationRegistrationStatusEntity(request.getNotificationRegistrationStatus());

        // Delete the storage unit notification.
        storageUnitNotificationRegistrationDao.delete(oldStorageUnitNotificationRegistrationEntity);

        // Create a storage unit notification registration entity from the request information.
        StorageUnitNotificationRegistrationEntity newStorageUnitNotificationRegistrationEntity =
            createStorageUnitNotificationEntity(namespaceEntity, notificationEventTypeEntity, businessObjectDefinitionEntity, fileTypeEntity, storageEntity,
                newStorageUnitStatus, oldStorageUnitStatus,
                new NotificationRegistrationKey(namespaceEntity.getCode(), oldStorageUnitNotificationRegistrationName),
                request.getStorageUnitNotificationFilter(), request.getJobActions(), notificationRegistrationStatusEntity);

        // Persist the new entity.
        newStorageUnitNotificationRegistrationEntity = storageUnitNotificationRegistrationDao.saveAndRefresh(newStorageUnitNotificationRegistrationEntity);

        // Create and return the storage unit notification object from the persisted entity.
        return createStorageUnitNotificationFromEntity(newStorageUnitNotificationRegistrationEntity);
    }

    /**
     * Creates a new storage unit notification registration entity from the request information.
     *
     * @param namespaceEntity the namespace entity
     * @param notificationEventTypeEntity the notification event type entity
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param fileTypeEntity the file type entity
     * @param storageEntity the storage entity
     * @param newStorageUnitStatusEntity the new storage unit status entity
     * @param oldStorageUnitStatusEntity the old storage unit status entity
     * @param key the storage unit notification registration key
     * @param storageUnitNotificationFilter the storage unit notification filter
     * @param jobActions the list of notification job actions
     * @param notificationRegistrationStatusEntity the notification registration status entity
     *
     * @return the newly created storage unit notification registration entity
     */
    private StorageUnitNotificationRegistrationEntity createStorageUnitNotificationEntity(NamespaceEntity namespaceEntity,
        NotificationEventTypeEntity notificationEventTypeEntity, BusinessObjectDefinitionEntity businessObjectDefinitionEntity, FileTypeEntity fileTypeEntity,
        StorageEntity storageEntity, StorageUnitStatusEntity newStorageUnitStatusEntity, StorageUnitStatusEntity oldStorageUnitStatusEntity,
        NotificationRegistrationKey key, StorageUnitNotificationFilter storageUnitNotificationFilter, List<JobAction> jobActions,
        NotificationRegistrationStatusEntity notificationRegistrationStatusEntity)
    {
        // Create a new entity.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = new StorageUnitNotificationRegistrationEntity();

        storageUnitNotificationRegistrationEntity.setNamespace(namespaceEntity);
        storageUnitNotificationRegistrationEntity.setName(key.getNotificationName());
        storageUnitNotificationRegistrationEntity.setNotificationEventType(notificationEventTypeEntity);
        storageUnitNotificationRegistrationEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        if (StringUtils.isNotBlank(storageUnitNotificationFilter.getBusinessObjectFormatUsage()))
        {
            storageUnitNotificationRegistrationEntity.setUsage(storageUnitNotificationFilter.getBusinessObjectFormatUsage());
        }
        storageUnitNotificationRegistrationEntity.setFileType(fileTypeEntity);
        storageUnitNotificationRegistrationEntity.setBusinessObjectFormatVersion(storageUnitNotificationFilter.getBusinessObjectFormatVersion());
        storageUnitNotificationRegistrationEntity.setStorage(storageEntity);
        storageUnitNotificationRegistrationEntity.setNewStorageUnitStatus(newStorageUnitStatusEntity);
        storageUnitNotificationRegistrationEntity.setOldStorageUnitStatus(oldStorageUnitStatusEntity);
        storageUnitNotificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusEntity);

        // Create the relative entities for job actions.
        // TODO: We need to add a null/empty list check here, if/when list of job actions will become optional (due to addition of other action types).
        List<NotificationActionEntity> notificationActionEntities = new ArrayList<>();
        storageUnitNotificationRegistrationEntity.setNotificationActions(notificationActionEntities);
        for (JobAction jobAction : jobActions)
        {
            // Retrieve and ensure that job definition exists.
            JobDefinitionEntity jobDefinitionEntity = jobDefinitionDaoHelper.getJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName());

            // Create a new entity.
            NotificationJobActionEntity notificationJobActionEntity = new NotificationJobActionEntity();
            notificationActionEntities.add(notificationJobActionEntity);
            notificationJobActionEntity.setJobDefinition(jobDefinitionEntity);
            notificationJobActionEntity.setCorrelationData(jobAction.getCorrelationData());
            notificationJobActionEntity.setNotificationRegistration(storageUnitNotificationRegistrationEntity);
        }

        return storageUnitNotificationRegistrationEntity;
    }

    /**
     * Creates the storage unit notification registration from the persisted entity.
     *
     * @param storageUnitNotificationRegistrationEntity the storage unit notification registration entity
     *
     * @return the storage unit notification registration
     */
    private StorageUnitNotificationRegistration createStorageUnitNotificationFromEntity(
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity)
    {
        // Create the storage unit notification.
        StorageUnitNotificationRegistration storageUnitNotificationRegistration = new StorageUnitNotificationRegistration();

        storageUnitNotificationRegistration.setId(storageUnitNotificationRegistrationEntity.getId());

        storageUnitNotificationRegistration.setStorageUnitNotificationRegistrationKey(
            new NotificationRegistrationKey(storageUnitNotificationRegistrationEntity.getNamespace().getCode(),
                storageUnitNotificationRegistrationEntity.getName()));

        storageUnitNotificationRegistration.setStorageUnitEventType(storageUnitNotificationRegistrationEntity.getNotificationEventType().getCode());

        StorageUnitNotificationFilter filter = new StorageUnitNotificationFilter();
        storageUnitNotificationRegistration.setStorageUnitNotificationFilter(filter);
        // Business object definition entity cannot be null as per storage unit notification registration create request validation.
        filter.setNamespace(storageUnitNotificationRegistrationEntity.getBusinessObjectDefinition().getNamespace().getCode());
        filter.setBusinessObjectDefinitionName(storageUnitNotificationRegistrationEntity.getBusinessObjectDefinition().getName());
        filter.setBusinessObjectFormatUsage(storageUnitNotificationRegistrationEntity.getUsage());
        filter.setBusinessObjectFormatFileType(
            storageUnitNotificationRegistrationEntity.getFileType() != null ? storageUnitNotificationRegistrationEntity.getFileType().getCode() : null);
        filter.setBusinessObjectFormatVersion(storageUnitNotificationRegistrationEntity.getBusinessObjectFormatVersion());
        filter.setStorageName(
            storageUnitNotificationRegistrationEntity.getStorage() != null ? storageUnitNotificationRegistrationEntity.getStorage().getName() : null);
        filter.setNewStorageUnitStatus(storageUnitNotificationRegistrationEntity.getNewStorageUnitStatus() != null ?
            storageUnitNotificationRegistrationEntity.getNewStorageUnitStatus().getCode() : null);
        filter.setOldStorageUnitStatus(storageUnitNotificationRegistrationEntity.getOldStorageUnitStatus() != null ?
            storageUnitNotificationRegistrationEntity.getOldStorageUnitStatus().getCode() : null);

        List<JobAction> jobActions = new ArrayList<>();
        storageUnitNotificationRegistration.setJobActions(jobActions);
        for (NotificationActionEntity notificationActionEntity : storageUnitNotificationRegistrationEntity.getNotificationActions())
        {
            if (notificationActionEntity instanceof NotificationJobActionEntity)
            {
                NotificationJobActionEntity notificationJobActionEntity = (NotificationJobActionEntity) notificationActionEntity;
                JobAction jobAction = new JobAction();
                jobActions.add(jobAction);
                jobAction.setNamespace(notificationJobActionEntity.getJobDefinition().getNamespace().getCode());
                jobAction.setJobName(notificationJobActionEntity.getJobDefinition().getName());
                jobAction.setCorrelationData(notificationJobActionEntity.getCorrelationData());
            }
        }

        storageUnitNotificationRegistration
            .setNotificationRegistrationStatus(storageUnitNotificationRegistrationEntity.getNotificationRegistrationStatus().getCode());

        return storageUnitNotificationRegistration;
    }

    /**
     * Retrieves and validate notification event type entity.
     *
     * @param notificationEventType the notification event type
     */
    private NotificationEventTypeEntity getAndValidateNotificationEventTypeEntity(String notificationEventType)
    {
        // Retrieve and ensure that notification event type exists.
        NotificationEventTypeEntity notificationEventTypeEntity = notificationEventTypeDaoHelper.getNotificationEventTypeEntity(notificationEventType);

        // Validate the notification event type.
        Assert.isTrue(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().equalsIgnoreCase(notificationEventType),
            String.format("Notification event type \"%s\" is not supported for storage unit notification registration.", notificationEventType));

        return notificationEventTypeEntity;
    }

    /**
     * Trims the business object format specific fields in the storage unit notification filter.
     *
     * @param filter the storage unit notification filter
     */
    private void trimStorageUnitNotificationFilterBusinessObjectFormatFields(StorageUnitNotificationFilter filter)
    {
        if (filter.getBusinessObjectFormatUsage() != null)
        {
            filter.setBusinessObjectFormatUsage(filter.getBusinessObjectFormatUsage().trim());
        }

        if (filter.getBusinessObjectFormatFileType() != null)
        {
            filter.setBusinessObjectFormatFileType(filter.getBusinessObjectFormatFileType().trim());
        }
    }

    /**
     * Validates the storage unit notification actions. This method also trims the notification action parameters.
     *
     * @param jobActions the list of notification job actions
     */
    private void validateNotificationActions(List<JobAction> jobActions)
    {
        Assert.notEmpty(jobActions, "At least one notification action must be specified.");

        // Ensure job action isn't a duplicate by using a hash set with lowercase job definition key values for case insensitivity.
        Set<JobAction> validatedJobActionsSet = new LinkedHashSet<>();
        for (JobAction jobAction : jobActions)
        {
            Assert.hasText(jobAction.getNamespace(), "A job action namespace must be specified.");
            jobAction.setNamespace(jobAction.getNamespace().trim());

            Assert.hasText(jobAction.getJobName(), "A job action job name must be specified.");
            jobAction.setJobName(jobAction.getJobName().trim());

            // Create a special version of the job action with the relative job definition key values in lowercase.
            JobAction lowercaseJobDefinitionKey = new JobAction();
            lowercaseJobDefinitionKey.setNamespace(jobAction.getNamespace().toLowerCase());
            lowercaseJobDefinitionKey.setJobName(jobAction.getJobName().toLowerCase());

            if (validatedJobActionsSet.contains(lowercaseJobDefinitionKey))
            {
                throw new IllegalArgumentException(
                    String.format("Duplicate job action {namespace: \"%s\", jobName: \"%s\"} found.", jobAction.getNamespace(), jobAction.getJobName()));
            }

            validatedJobActionsSet.add(lowercaseJobDefinitionKey);
        }
    }

    /**
     * Validates the storage unit notification filter. This method also trims the filter parameters.
     *
     * @param filter the storage unit notification filter
     */
    private void validateStorageUnitNotificationFilter(StorageUnitNotificationFilter filter)
    {
        Assert.notNull(filter, "A storage unit notification filter must be specified.");

        validateStorageUnitNotificationFilterBusinessObjectDefinitionFields(filter);

        trimStorageUnitNotificationFilterBusinessObjectFormatFields(filter);

        Assert.hasText(filter.getStorageName(), "A storage name must be specified.");
        filter.setStorageName(filter.getStorageName().trim());

        if (filter.getNewStorageUnitStatus() != null)
        {
            filter.setNewStorageUnitStatus(filter.getNewStorageUnitStatus().trim());
        }

        if (filter.getOldStorageUnitStatus() != null)
        {
            filter.setOldStorageUnitStatus(filter.getOldStorageUnitStatus().trim());
        }

        // If both new and old storage unit statuses are specified, validate that they are not the same.
        if (StringUtils.isNotBlank(filter.getNewStorageUnitStatus()) && StringUtils.isNotBlank(filter.getOldStorageUnitStatus()))
        {
            Assert.isTrue(!filter.getOldStorageUnitStatus().equalsIgnoreCase(filter.getNewStorageUnitStatus()),
                "The new storage unit status is the same as the old one.");
        }
    }

    /**
     * Validates the business object definition specific fields in the storage unit notification filter. This method also trims the filter parameters.
     *
     * @param filter the storage unit notification filter
     */
    private void validateStorageUnitNotificationFilterBusinessObjectDefinitionFields(StorageUnitNotificationFilter filter)
    {
        Assert.hasText(filter.getNamespace(), "A business object definition namespace must be specified.");
        filter.setNamespace(filter.getNamespace().trim());

        Assert.hasText(filter.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        filter.setBusinessObjectDefinitionName(filter.getBusinessObjectDefinitionName().trim());
    }

    /**
     * Validates the storage unit notification create request. This method also trims the request parameters.
     *
     * @param request the storage unit notification create request
     */
    private void validateStorageUnitNotificationRegistrationCreateRequest(StorageUnitNotificationRegistrationCreateRequest request)
    {
        Assert.notNull(request, "A storage unit notification create request must be specified.");

        validateStorageUnitNotificationRegistrationKey(request.getStorageUnitNotificationRegistrationKey());

        Assert.hasText(request.getStorageUnitEventType(), "A storage unit event type must be specified.");
        request.setStorageUnitEventType(request.getStorageUnitEventType().trim());

        validateStorageUnitNotificationFilter(request.getStorageUnitNotificationFilter());
        validateNotificationActions(request.getJobActions());

        if (request.getNotificationRegistrationStatus() != null)
        {
            request.setNotificationRegistrationStatus(request.getNotificationRegistrationStatus().trim());
        }
    }

    /**
     * Validates the storage unit notification registration key. This method also trims the key parameters.
     *
     * @param notificationRegistrationKey the storage unit notification registration key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateStorageUnitNotificationRegistrationKey(NotificationRegistrationKey notificationRegistrationKey) throws IllegalArgumentException
    {
        Assert.notNull(notificationRegistrationKey, "A storage unit notification registration key must be specified.");
        notificationRegistrationKey.setNamespace(alternateKeyHelper.validateStringParameter("namespace", notificationRegistrationKey.getNamespace()));
        notificationRegistrationKey
            .setNotificationName(alternateKeyHelper.validateStringParameter("notification name", notificationRegistrationKey.getNotificationName()));
    }

    /**
     * Validates the storage unit notification update request. This method also trims the request parameters.
     *
     * @param request the storage unit notification update request
     */
    private void validateStorageUnitNotificationRegistrationUpdateRequest(StorageUnitNotificationRegistrationUpdateRequest request)
    {
        Assert.notNull(request, "A storage unit notification update request must be specified.");

        Assert.hasText(request.getStorageUnitEventType(), "A storage unit event type must be specified.");
        request.setStorageUnitEventType(request.getStorageUnitEventType().trim());

        validateStorageUnitNotificationFilter(request.getStorageUnitNotificationFilter());
        validateNotificationActions(request.getJobActions());

        Assert.hasText(request.getNotificationRegistrationStatus(), "A notification registration status must be specified.");
        request.setNotificationRegistrationStatus(request.getNotificationRegistrationStatus().trim());
    }
}
