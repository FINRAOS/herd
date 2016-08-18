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

import org.finra.herd.dao.BusinessObjectDataNotificationRegistrationDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.NamespacePermissions;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistration;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NotificationActionEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationJobActionEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.service.BusinessObjectDataNotificationRegistrationService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDataNotificationRegistrationDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataStatusDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.FileTypeDaoHelper;
import org.finra.herd.service.helper.JobDefinitionDaoHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NotificationEventTypeDaoHelper;
import org.finra.herd.service.helper.NotificationRegistrationStatusDaoHelper;
import org.finra.herd.service.helper.StorageDaoHelper;

/**
 * The business object data notification service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataNotificationRegistrationServiceImpl implements BusinessObjectDataNotificationRegistrationService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDataNotificationRegistrationDao businessObjectDataNotificationRegistrationDao;

    @Autowired
    private BusinessObjectDataNotificationRegistrationDaoHelper businessObjectDataNotificationRegistrationDaoHelper;

    @Autowired
    private BusinessObjectDataStatusDaoHelper businessObjectDataStatusDaoHelper;

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

    @NamespacePermissions(
        {@NamespacePermission(fields = "#request?.businessObjectDataNotificationRegistrationKey?.namespace", permissions = NamespacePermissionEnum.WRITE),
            @NamespacePermission(fields = "#request?.businessObjectDataNotificationFilter?.namespace", permissions = NamespacePermissionEnum.READ),
            @NamespacePermission(fields = "#request?.jobActions?.![namespace]", permissions = NamespacePermissionEnum.EXECUTE)})
    @Override
    public BusinessObjectDataNotificationRegistration createBusinessObjectDataNotificationRegistration(
        BusinessObjectDataNotificationRegistrationCreateRequest request)
    {
        // Validate and trim the request parameters.
        validateBusinessObjectDataNotificationRegistrationCreateRequest(request);

        // Get the business object notification key.
        NotificationRegistrationKey key = request.getBusinessObjectDataNotificationRegistrationKey();

        // Retrieve and ensure that namespace exists with the specified namespace code.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(key.getNamespace());

        // Retrieve and validate the notification event type entity.
        NotificationEventTypeEntity notificationEventTypeEntity = getAndValidateNotificationEventTypeEntity(request.getBusinessObjectDataEventType());

        // Get the business object data notification filter.
        BusinessObjectDataNotificationFilter filter = request.getBusinessObjectDataNotificationFilter();

        // Retrieve and ensure that business object definition exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper
            .getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(filter.getNamespace(), filter.getBusinessObjectDefinitionName()));

        // If specified, retrieve and ensure that file type exists.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(filter.getBusinessObjectFormatFileType()))
        {
            fileTypeEntity = fileTypeDaoHelper.getFileTypeEntity(filter.getBusinessObjectFormatFileType());
        }

        // If specified, retrieve and ensure that storage exists.
        StorageEntity storageEntity = null;
        if (StringUtils.isNotBlank(filter.getStorageName()))
        {
            storageEntity = storageDaoHelper.getStorageEntity(filter.getStorageName());
        }

        // If specified, retrieve and ensure that new business object data status exists.
        BusinessObjectDataStatusEntity newBusinessObjectDataStatus = null;
        if (StringUtils.isNotBlank(filter.getNewBusinessObjectDataStatus()))
        {
            newBusinessObjectDataStatus = businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(filter.getNewBusinessObjectDataStatus());
        }

        // If specified, retrieve and ensure that old business object data status exists.
        BusinessObjectDataStatusEntity oldBusinessObjectDataStatus = null;
        if (StringUtils.isNotBlank(filter.getOldBusinessObjectDataStatus()))
        {
            oldBusinessObjectDataStatus = businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(filter.getOldBusinessObjectDataStatus());
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

        // Ensure a business object data notification with the specified name doesn't already exist for the specified namespace.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(key);
        if (businessObjectDataNotificationRegistrationEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create business object data notification with name \"%s\" because it already exists for namespace \"%s\".",
                    key.getNotificationName(), key.getNamespace()));
        }

        // Create a business object data notification registration entity from the request information.
        businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationEntity(namespaceEntity, notificationEventTypeEntity, businessObjectDefinitionEntity, fileTypeEntity,
                storageEntity, newBusinessObjectDataStatus, oldBusinessObjectDataStatus, request.getBusinessObjectDataNotificationRegistrationKey(),
                request.getBusinessObjectDataNotificationFilter(), request.getJobActions(), notificationRegistrationStatusEntity);

        // Persist the new entity.
        businessObjectDataNotificationRegistrationEntity =
            businessObjectDataNotificationRegistrationDao.saveAndRefresh(businessObjectDataNotificationRegistrationEntity);

        // Create and return the business object data notification object from the persisted entity.
        return createBusinessObjectDataNotificationFromEntity(businessObjectDataNotificationRegistrationEntity);
    }

    @NamespacePermission(fields = "#key?.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public BusinessObjectDataNotificationRegistration getBusinessObjectDataNotificationRegistration(NotificationRegistrationKey key)
    {
        // Validate and trim the key.
        validateBusinessObjectDataNotificationRegistrationKey(key);

        // Retrieve and ensure that a business object data notification exists with the specified key.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            businessObjectDataNotificationRegistrationDaoHelper.getBusinessObjectDataNotificationRegistrationEntity(key);

        // Create and return the business object data notification object from the persisted entity.
        return createBusinessObjectDataNotificationFromEntity(businessObjectDataNotificationRegistrationEntity);
    }

    @NamespacePermissions({@NamespacePermission(fields = "#key?.namespace", permissions = NamespacePermissionEnum.WRITE),
        @NamespacePermission(fields = "#request?.businessObjectDataNotificationFilter?.namespace", permissions = NamespacePermissionEnum.READ),
        @NamespacePermission(fields = "#request?.jobActions?.![namespace]", permissions = NamespacePermissionEnum.EXECUTE)})
    @Override
    public BusinessObjectDataNotificationRegistration updateBusinessObjectDataNotificationRegistration(NotificationRegistrationKey key,
        BusinessObjectDataNotificationRegistrationUpdateRequest request)
    {
        // Validate and trim the key.
        validateBusinessObjectDataNotificationRegistrationKey(key);

        // Validate and trim the request parameters.
        validateBusinessObjectDataNotificationRegistrationUpdateRequest(request);

        // Retrieve and ensure that a business object data notification exists with the specified key.
        BusinessObjectDataNotificationRegistrationEntity oldBusinessObjectDataNotificationRegistrationEntity =
            businessObjectDataNotificationRegistrationDaoHelper.getBusinessObjectDataNotificationRegistrationEntity(key);
        String oldBusinessObjectDataNotificationRegistrationName = oldBusinessObjectDataNotificationRegistrationEntity.getName();

        // Retrieve the namespace with the specified namespace code.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(key.getNamespace());

        // Retrieve and validate the notification event type entity.
        NotificationEventTypeEntity notificationEventTypeEntity = getAndValidateNotificationEventTypeEntity(request.getBusinessObjectDataEventType());

        // Get the business object data notification filter.
        BusinessObjectDataNotificationFilter filter = request.getBusinessObjectDataNotificationFilter();

        // Retrieve and ensure that business object definition exists. Since namespace is specified, retrieve a business object definition by it's key.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper
            .getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(filter.getNamespace(), filter.getBusinessObjectDefinitionName()));

        // If specified, retrieve and ensure that file type exists.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(filter.getBusinessObjectFormatFileType()))
        {
            fileTypeEntity = fileTypeDaoHelper.getFileTypeEntity(filter.getBusinessObjectFormatFileType());
        }

        // If specified, retrieve and ensure that storage exists.
        StorageEntity storageEntity = null;
        if (StringUtils.isNotBlank(filter.getStorageName()))
        {
            storageEntity = storageDaoHelper.getStorageEntity(filter.getStorageName());
        }

        // If specified, retrieve and ensure that new business object data status exists.
        BusinessObjectDataStatusEntity newBusinessObjectDataStatus = null;
        if (StringUtils.isNotBlank(filter.getNewBusinessObjectDataStatus()))
        {
            newBusinessObjectDataStatus = businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(filter.getNewBusinessObjectDataStatus());
        }

        // If specified, retrieve and ensure that old business object data status exists.
        BusinessObjectDataStatusEntity oldBusinessObjectDataStatus = null;
        if (StringUtils.isNotBlank(filter.getOldBusinessObjectDataStatus()))
        {
            oldBusinessObjectDataStatus = businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(filter.getOldBusinessObjectDataStatus());
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

        // Delete the business object data notification.
        businessObjectDataNotificationRegistrationDao.delete(oldBusinessObjectDataNotificationRegistrationEntity);

        // Create a business object data notification registration entity from the request information.
        BusinessObjectDataNotificationRegistrationEntity newBusinessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationEntity(namespaceEntity, notificationEventTypeEntity, businessObjectDefinitionEntity, fileTypeEntity,
                storageEntity, newBusinessObjectDataStatus, oldBusinessObjectDataStatus,
                new NotificationRegistrationKey(namespaceEntity.getCode(), oldBusinessObjectDataNotificationRegistrationName),
                request.getBusinessObjectDataNotificationFilter(), request.getJobActions(), notificationRegistrationStatusEntity);

        // Persist the new entity.
        newBusinessObjectDataNotificationRegistrationEntity =
            businessObjectDataNotificationRegistrationDao.saveAndRefresh(newBusinessObjectDataNotificationRegistrationEntity);

        // Create and return the business object data notification object from the persisted entity.
        return createBusinessObjectDataNotificationFromEntity(newBusinessObjectDataNotificationRegistrationEntity);
    }

    @NamespacePermission(fields = "#key?.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectDataNotificationRegistration deleteBusinessObjectDataNotificationRegistration(NotificationRegistrationKey key)
    {
        // Validate and trim the key.
        validateBusinessObjectDataNotificationRegistrationKey(key);

        // Retrieve and ensure that a business object data notification exists with the specified key.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            businessObjectDataNotificationRegistrationDaoHelper.getBusinessObjectDataNotificationRegistrationEntity(key);

        // Delete the business object data notification.
        businessObjectDataNotificationRegistrationDao.delete(businessObjectDataNotificationRegistrationEntity);

        // Create and return the business object data notification object from the deleted entity.
        return createBusinessObjectDataNotificationFromEntity(businessObjectDataNotificationRegistrationEntity);
    }

    @Override
    public BusinessObjectDataNotificationRegistrationKeys getBusinessObjectDataNotificationRegistrationsByNamespace(String namespace)
    {
        String namespaceLocal = namespace;

        // Validate and trim the namespace value.
        Assert.hasText(namespaceLocal, "A namespace must be specified.");
        namespaceLocal = namespaceLocal.trim();

        // Ensure that this namespace exists.
        namespaceDaoHelper.getNamespaceEntity(namespaceLocal);

        // Create and populate a list of business object data notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys businessObjectDataNotificationKeys = new BusinessObjectDataNotificationRegistrationKeys();
        businessObjectDataNotificationKeys.getBusinessObjectDataNotificationRegistrationKeys()
            .addAll(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationKeysByNamespace(namespaceLocal));

        return businessObjectDataNotificationKeys;
    }

    @NamespacePermission(fields = "#businessObjectDataNotificationFilter?.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public BusinessObjectDataNotificationRegistrationKeys getBusinessObjectDataNotificationRegistrationsByNotificationFilter(
        BusinessObjectDataNotificationFilter businessObjectDataNotificationFilter)
    {
        // Validate and trim the business object data notification filter parameters.
        validateBusinessObjectDataNotificationFilterBusinessObjectDefinitionFields(businessObjectDataNotificationFilter);
        trimBusinessObjectDataNotificationFilterBusinessObjectFormatFields(businessObjectDataNotificationFilter);

        // Create and populate a list of business object data notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys businessObjectDataNotificationKeys = new BusinessObjectDataNotificationRegistrationKeys();
        businessObjectDataNotificationKeys.getBusinessObjectDataNotificationRegistrationKeys().addAll(businessObjectDataNotificationRegistrationDao
            .getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(businessObjectDataNotificationFilter));

        return businessObjectDataNotificationKeys;
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
        Assert.isTrue(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name().equalsIgnoreCase(notificationEventType) ||
            NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG.name().equalsIgnoreCase(notificationEventType),
            String.format("Notification event type \"%s\" is not supported for business object data notification registration.", notificationEventType));

        return notificationEventTypeEntity;
    }

    /**
     * Validates the business object data notification create request. This method also trims the request parameters.
     *
     * @param request the business object data notification create request
     */
    private void validateBusinessObjectDataNotificationRegistrationCreateRequest(BusinessObjectDataNotificationRegistrationCreateRequest request)
    {
        Assert.notNull(request, "A business object data notification create request must be specified.");

        validateBusinessObjectDataNotificationRegistrationKey(request.getBusinessObjectDataNotificationRegistrationKey());

        Assert.hasText(request.getBusinessObjectDataEventType(), "A business object data event type must be specified.");
        request.setBusinessObjectDataEventType(request.getBusinessObjectDataEventType().trim());

        validateBusinessObjectDataNotificationFilter(request.getBusinessObjectDataNotificationFilter(), request.getBusinessObjectDataEventType());
        validateNotificationActions(request.getJobActions());

        if (request.getNotificationRegistrationStatus() != null)
        {
            request.setNotificationRegistrationStatus(request.getNotificationRegistrationStatus().trim());
        }
    }

    /**
     * Validates the business object data notification update request. This method also trims the request parameters.
     *
     * @param request the business object data notification update request
     */
    private void validateBusinessObjectDataNotificationRegistrationUpdateRequest(BusinessObjectDataNotificationRegistrationUpdateRequest request)
    {
        Assert.notNull(request, "A business object data notification update request must be specified.");

        Assert.hasText(request.getBusinessObjectDataEventType(), "A business object data event type must be specified.");
        request.setBusinessObjectDataEventType(request.getBusinessObjectDataEventType().trim());

        Assert.hasText(request.getNotificationRegistrationStatus(), "A notification registration status must be specified.");
        request.setNotificationRegistrationStatus(request.getNotificationRegistrationStatus().trim());

        validateBusinessObjectDataNotificationFilter(request.getBusinessObjectDataNotificationFilter(), request.getBusinessObjectDataEventType());
        validateNotificationActions(request.getJobActions());
    }

    /**
     * Validates the storage unit notification registration key. This method also trims the key parameters.
     *
     * @param notificationRegistrationKey the storage unit notification registration key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDataNotificationRegistrationKey(NotificationRegistrationKey notificationRegistrationKey) throws IllegalArgumentException
    {
        Assert.notNull(notificationRegistrationKey, "A business object data notification registration key must be specified.");
        notificationRegistrationKey.setNamespace(alternateKeyHelper.validateStringParameter("namespace", notificationRegistrationKey.getNamespace()));
        notificationRegistrationKey
            .setNotificationName(alternateKeyHelper.validateStringParameter("notification name", notificationRegistrationKey.getNotificationName()));
    }

    /**
     * Validates the business object data notification filter. This method also trims the filter parameters.
     *
     * @param filter the business object data notification filter
     */
    private void validateBusinessObjectDataNotificationFilter(BusinessObjectDataNotificationFilter filter, String businessObjectDataEventType)
    {
        Assert.notNull(filter, "A business object data notification filter must be specified.");

        validateBusinessObjectDataNotificationFilterBusinessObjectDefinitionFields(filter);

        trimBusinessObjectDataNotificationFilterBusinessObjectFormatFields(filter);

        if (filter.getStorageName() != null)
        {
            filter.setStorageName(filter.getStorageName().trim());
        }

        if (filter.getNewBusinessObjectDataStatus() != null)
        {
            filter.setNewBusinessObjectDataStatus(filter.getNewBusinessObjectDataStatus().trim());
        }

        if (filter.getOldBusinessObjectDataStatus() != null)
        {
            filter.setOldBusinessObjectDataStatus(filter.getOldBusinessObjectDataStatus().trim());

            // Fail if an old business object data status is specified for the business object data registration event.
            if (NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name().equalsIgnoreCase(businessObjectDataEventType) &&
                StringUtils.isNotBlank(filter.getOldBusinessObjectDataStatus()))
            {
                throw new IllegalArgumentException(
                    "The old business object data status cannot be specified with a business object data registration event type.");
            }
        }

        // If both new and old business object data statuses are specified, validate that they are not the same.
        if (StringUtils.isNotBlank(filter.getNewBusinessObjectDataStatus()) && StringUtils.isNotBlank(filter.getOldBusinessObjectDataStatus()))
        {
            Assert.isTrue(!filter.getOldBusinessObjectDataStatus().equalsIgnoreCase(filter.getNewBusinessObjectDataStatus()),
                "The new business object data status is the same as the old one.");
        }
    }

    /**
     * Validates the business object definition specific fields in the business object data notification filter. This method also trims the filter parameters.
     *
     * @param filter the business object data notification filter
     */
    private void validateBusinessObjectDataNotificationFilterBusinessObjectDefinitionFields(BusinessObjectDataNotificationFilter filter)
    {
        Assert.hasText(filter.getNamespace(), "A business object definition namespace must be specified.");
        filter.setNamespace(filter.getNamespace().trim());

        Assert.hasText(filter.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        filter.setBusinessObjectDefinitionName(filter.getBusinessObjectDefinitionName().trim());
    }

    /**
     * Trims the business object format specific fields in the business object data notification filter.
     *
     * @param filter the business object data notification filter
     */
    private void trimBusinessObjectDataNotificationFilterBusinessObjectFormatFields(BusinessObjectDataNotificationFilter filter)
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
     * Validates the business object data notification actions. This method also trims the notification action parameters.
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
     * Creates a new business object data notification registration entity from the request information.
     *
     * @param namespaceEntity the namespace entity
     * @param notificationEventTypeEntity the notification event type entity
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param fileTypeEntity the file type entity
     * @param storageEntity the storage entity
     * @param newBusinessObjectDataStatusEntity the new business object data status entity
     * @param oldBusinessObjectDataStatusEntity the old business object data status entity
     * @param key the business object data notification registration key
     * @param businessObjectDataNotificationFilter the business object data notification filter
     * @param jobActions the list of notification job actions
     * @param notificationRegistrationStatusEntity the notification registration status entity
     *
     * @return the newly created business object data notification registration entity
     */
    private BusinessObjectDataNotificationRegistrationEntity createBusinessObjectDataNotificationEntity(NamespaceEntity namespaceEntity,
        NotificationEventTypeEntity notificationEventTypeEntity, BusinessObjectDefinitionEntity businessObjectDefinitionEntity, FileTypeEntity fileTypeEntity,
        StorageEntity storageEntity, BusinessObjectDataStatusEntity newBusinessObjectDataStatusEntity,
        BusinessObjectDataStatusEntity oldBusinessObjectDataStatusEntity, NotificationRegistrationKey key,
        BusinessObjectDataNotificationFilter businessObjectDataNotificationFilter, List<JobAction> jobActions,
        NotificationRegistrationStatusEntity notificationRegistrationStatusEntity)
    {
        // Create a new entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            new BusinessObjectDataNotificationRegistrationEntity();

        businessObjectDataNotificationRegistrationEntity.setNamespace(namespaceEntity);
        businessObjectDataNotificationRegistrationEntity.setName(key.getNotificationName());
        businessObjectDataNotificationRegistrationEntity.setNotificationEventType(notificationEventTypeEntity);
        businessObjectDataNotificationRegistrationEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        if (StringUtils.isNotBlank(businessObjectDataNotificationFilter.getBusinessObjectFormatUsage()))
        {
            businessObjectDataNotificationRegistrationEntity.setUsage(businessObjectDataNotificationFilter.getBusinessObjectFormatUsage());
        }
        businessObjectDataNotificationRegistrationEntity.setFileType(fileTypeEntity);
        businessObjectDataNotificationRegistrationEntity.setBusinessObjectFormatVersion(businessObjectDataNotificationFilter.getBusinessObjectFormatVersion());
        businessObjectDataNotificationRegistrationEntity.setStorage(storageEntity);
        businessObjectDataNotificationRegistrationEntity.setNewBusinessObjectDataStatus(newBusinessObjectDataStatusEntity);
        businessObjectDataNotificationRegistrationEntity.setOldBusinessObjectDataStatus(oldBusinessObjectDataStatusEntity);
        businessObjectDataNotificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusEntity);

        // Create the relative entities for job actions.
        // TODO: We need to add a null/empty list check here, if/when list of job actions will become optional (due to addition of other action types).
        List<NotificationActionEntity> notificationActionEntities = new ArrayList<>();
        businessObjectDataNotificationRegistrationEntity.setNotificationActions(notificationActionEntities);
        for (JobAction jobAction : jobActions)
        {
            // Retrieve and ensure that job definition exists.
            JobDefinitionEntity jobDefinitionEntity = jobDefinitionDaoHelper.getJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName());

            // Create a new entity.
            NotificationJobActionEntity notificationJobActionEntity = new NotificationJobActionEntity();
            notificationActionEntities.add(notificationJobActionEntity);
            notificationJobActionEntity.setJobDefinition(jobDefinitionEntity);
            notificationJobActionEntity.setCorrelationData(jobAction.getCorrelationData());
            notificationJobActionEntity.setNotificationRegistration(businessObjectDataNotificationRegistrationEntity);
        }

        return businessObjectDataNotificationRegistrationEntity;
    }

    /**
     * Creates the business object data notification registration from the persisted entity.
     *
     * @param businessObjectDataNotificationRegistrationEntity the business object data notification registration entity
     *
     * @return the business object data notification registration
     */
    private BusinessObjectDataNotificationRegistration createBusinessObjectDataNotificationFromEntity(
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity)
    {
        // Create the business object data notification.
        BusinessObjectDataNotificationRegistration businessObjectDataNotificationRegistration = new BusinessObjectDataNotificationRegistration();

        businessObjectDataNotificationRegistration.setId(businessObjectDataNotificationRegistrationEntity.getId());

        businessObjectDataNotificationRegistration.setBusinessObjectDataNotificationRegistrationKey(
            new NotificationRegistrationKey(businessObjectDataNotificationRegistrationEntity.getNamespace().getCode(),
                businessObjectDataNotificationRegistrationEntity.getName()));

        businessObjectDataNotificationRegistration
            .setBusinessObjectDataEventType(businessObjectDataNotificationRegistrationEntity.getNotificationEventType().getCode());

        BusinessObjectDataNotificationFilter filter = new BusinessObjectDataNotificationFilter();
        businessObjectDataNotificationRegistration.setBusinessObjectDataNotificationFilter(filter);
        // Business object definition entity cannot be null as per business object data notification registration create request validation.
        filter.setNamespace(businessObjectDataNotificationRegistrationEntity.getBusinessObjectDefinition().getNamespace().getCode());
        filter.setBusinessObjectDefinitionName(businessObjectDataNotificationRegistrationEntity.getBusinessObjectDefinition().getName());
        filter.setBusinessObjectFormatUsage(businessObjectDataNotificationRegistrationEntity.getUsage());
        filter.setBusinessObjectFormatFileType(
            businessObjectDataNotificationRegistrationEntity.getFileType() != null ? businessObjectDataNotificationRegistrationEntity.getFileType().getCode() :
                null);
        filter.setBusinessObjectFormatVersion(businessObjectDataNotificationRegistrationEntity.getBusinessObjectFormatVersion());
        filter.setStorageName(
            businessObjectDataNotificationRegistrationEntity.getStorage() != null ? businessObjectDataNotificationRegistrationEntity.getStorage().getName() :
                null);
        filter.setNewBusinessObjectDataStatus(businessObjectDataNotificationRegistrationEntity.getNewBusinessObjectDataStatus() != null ?
            businessObjectDataNotificationRegistrationEntity.getNewBusinessObjectDataStatus().getCode() : null);
        filter.setOldBusinessObjectDataStatus(businessObjectDataNotificationRegistrationEntity.getOldBusinessObjectDataStatus() != null ?
            businessObjectDataNotificationRegistrationEntity.getOldBusinessObjectDataStatus().getCode() : null);

        List<JobAction> jobActions = new ArrayList<>();
        businessObjectDataNotificationRegistration.setJobActions(jobActions);
        for (NotificationActionEntity notificationActionEntity : businessObjectDataNotificationRegistrationEntity.getNotificationActions())
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

        businessObjectDataNotificationRegistration
            .setNotificationRegistrationStatus(businessObjectDataNotificationRegistrationEntity.getNotificationRegistrationStatus().getCode());

        return businessObjectDataNotificationRegistration;
    }
}
