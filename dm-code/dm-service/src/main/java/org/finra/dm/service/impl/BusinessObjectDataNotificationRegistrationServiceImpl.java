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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.dm.dao.DmDao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.AlreadyExistsException;
import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.dm.model.jpa.FileTypeEntity;
import org.finra.dm.model.jpa.JobDefinitionEntity;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.jpa.NotificationActionEntity;
import org.finra.dm.model.jpa.NotificationEventTypeEntity;
import org.finra.dm.model.jpa.NotificationJobActionEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationFilter;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistration;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKey;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKeys;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.dm.model.api.xml.JobAction;
import org.finra.dm.service.BusinessObjectDataNotificationRegistrationService;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.DmHelper;

/**
 * The business object data notification service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataNotificationRegistrationServiceImpl implements BusinessObjectDataNotificationRegistrationService
{
    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private DmDao dmDao;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    /**
     * Creates a new business object data notification.
     *
     * @param request the information needed to create a business object data notification
     *
     * @return the newly created business object data notification
     */
    @Override
    public BusinessObjectDataNotificationRegistration createBusinessObjectDataNotificationRegistration(
        BusinessObjectDataNotificationRegistrationCreateRequest request)
    {
        // Validate and trim the request parameters.
        validateBusinessObjectDataNotificationRegistrationCreateRequest(request);

        // Get the business object notification key.
        BusinessObjectDataNotificationRegistrationKey key = request.getBusinessObjectDataNotificationRegistrationKey();

        // Retrieve and ensure that namespace exists with the specified namespace code.
        NamespaceEntity namespaceEntity = dmDaoHelper.getNamespaceEntity(key.getNamespace());

        // Retrieve and ensure that notification event type exists.
        NotificationEventTypeEntity notificationEventTypeEntity = dmDaoHelper.getNotificationEventTypeEntity(request.getBusinessObjectDataEventType());

        // Get the business object data notification filter.
        BusinessObjectDataNotificationFilter filter = request.getBusinessObjectDataNotificationFilter();

        // Retrieve and ensure that business object definition exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity;
        if (StringUtils.isBlank(filter.getNamespace()))
        {
            // If namespace is not specified, retrieve the legacy business object definition by it's name only.
            businessObjectDefinitionEntity = dmDaoHelper.getLegacyBusinessObjectDefinitionEntity(filter.getBusinessObjectDefinitionName());

            // Update the filter with the retrieved namespace.
            filter.setNamespace(businessObjectDefinitionEntity.getNamespace().getCode());
        }
        else
        {
            // Since namespace is specified, retrieve a business object definition by it's key.
            businessObjectDefinitionEntity =
                dmDaoHelper.getBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(filter.getNamespace(), filter.getBusinessObjectDefinitionName()));
        }

        // If specified, retrieve and ensure that file type exists.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(filter.getBusinessObjectFormatFileType()))
        {
            fileTypeEntity = dmDaoHelper.getFileTypeEntity(filter.getBusinessObjectFormatFileType());
        }

        // If specified, retrieve and ensure that storage exists.
        StorageEntity storageEntity = null;
        if (StringUtils.isNotBlank(filter.getStorageName()))
        {
            storageEntity = dmDaoHelper.getStorageEntity(filter.getStorageName());
        }

        // Ensure a business object data notification with the specified name doesn't already exist for the specified namespace.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            dmDao.getBusinessObjectDataNotificationByAltKey(key);
        if (businessObjectDataNotificationRegistrationEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create business object data notification with name \"%s\" because it already exists for namespace \"%s\".",
                    key.getNotificationName(), key.getNamespace()));
        }

        // Create a business object data notification registration entity from the request information.
        businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationEntity(namespaceEntity, notificationEventTypeEntity, businessObjectDefinitionEntity, fileTypeEntity,
                storageEntity, request);

        // Persist the new entity.
        businessObjectDataNotificationRegistrationEntity = dmDao.saveAndRefresh(businessObjectDataNotificationRegistrationEntity);

        // Create and return the business object data notification object from the persisted entity.
        return createBusinessObjectDataNotificationFromEntity(businessObjectDataNotificationRegistrationEntity);
    }

    /**
     * Gets an existing business object data notification by key.
     *
     * @param key the business object data notification registration key
     *
     * @return the business object data notification information
     */
    @Override
    public BusinessObjectDataNotificationRegistration getBusinessObjectDataNotificationRegistration(BusinessObjectDataNotificationRegistrationKey key)
    {
        // Validate and trim the key.
        dmHelper.validateBusinessObjectDataNotificationRegistrationKey(key);

        // Retrieve and ensure that a business object data notification exists with the specified key.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            dmDaoHelper.getBusinessObjectDataNotificationEntity(key);

        // Create and return the business object data notification object from the persisted entity.
        return createBusinessObjectDataNotificationFromEntity(businessObjectDataNotificationRegistrationEntity);
    }

    /**
     * Deletes an existing business object data notification by key.
     *
     * @param key the business object data notification registration key
     *
     * @return the business object data notification that got deleted
     */
    @Override
    public BusinessObjectDataNotificationRegistration deleteBusinessObjectDataNotificationRegistration(BusinessObjectDataNotificationRegistrationKey key)
    {
        // Validate and trim the key.
        dmHelper.validateBusinessObjectDataNotificationRegistrationKey(key);

        // Retrieve and ensure that a business object data notification exists with the specified key.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            dmDaoHelper.getBusinessObjectDataNotificationEntity(key);

        // Delete the business object data notification.
        dmDao.delete(businessObjectDataNotificationRegistrationEntity);

        // Create and return the business object data notification object from the deleted entity.
        return createBusinessObjectDataNotificationFromEntity(businessObjectDataNotificationRegistrationEntity);
    }

    /**
     * Gets a list of keys for all existing business object data notifications.
     *
     * @param namespaceCode the namespace code.
     *
     * @return the business object data notification registration keys
     */
    @Override
    public BusinessObjectDataNotificationRegistrationKeys getBusinessObjectDataNotificationRegistrations(String namespaceCode)
    {
        String namespaceCodeLocal = namespaceCode;

        // Validate and trim the namespace value.
        Assert.hasText(namespaceCodeLocal, "A namespace must be specified.");
        namespaceCodeLocal = namespaceCodeLocal.trim();

        // Ensure that this namespace exists.
        dmDaoHelper.getNamespaceEntity(namespaceCodeLocal);

        // Create and populate a list of business object data notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys businessObjectDataNotificationKeys = new BusinessObjectDataNotificationRegistrationKeys();
        businessObjectDataNotificationKeys.getBusinessObjectDataNotificationRegistrationKeys()
            .addAll(dmDao.getBusinessObjectDataNotificationRegistrationKeys(namespaceCodeLocal));

        return businessObjectDataNotificationKeys;
    }

    /**
     * Validates the business object data notification create request. This method also trims the request parameters.
     *
     * @param request the business object data notification create request
     */
    private void validateBusinessObjectDataNotificationRegistrationCreateRequest(BusinessObjectDataNotificationRegistrationCreateRequest request)
    {
        Assert.notNull(request, "A business object data notification create request must be specified.");

        dmHelper.validateBusinessObjectDataNotificationRegistrationKey(request.getBusinessObjectDataNotificationRegistrationKey());

        Assert.hasText(request.getBusinessObjectDataEventType(), "A business object data event type must be specified.");
        request.setBusinessObjectDataEventType(request.getBusinessObjectDataEventType().trim());

        validateBusinessObjectDataNotificationFilter(request.getBusinessObjectDataNotificationFilter());
        validateNotificationActions(request.getJobActions());
    }

    /**
     * Validates the business object data notification filter. This method also trims the filter parameters.
     *
     * @param filter the business object data notification filter
     */
    private void validateBusinessObjectDataNotificationFilter(BusinessObjectDataNotificationFilter filter)
    {
        Assert.notNull(filter, "A business object data notification filter must be specified.");

        if (filter.getNamespace() != null)
        {
            filter.setNamespace(filter.getNamespace().trim());
        }

        Assert.hasText(filter.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        filter.setBusinessObjectDefinitionName(filter.getBusinessObjectDefinitionName().trim());

        if (filter.getBusinessObjectFormatUsage() != null)
        {
            filter.setBusinessObjectFormatUsage(filter.getBusinessObjectFormatUsage().trim());
        }

        if (filter.getBusinessObjectFormatFileType() != null)
        {
            filter.setBusinessObjectFormatFileType(filter.getBusinessObjectFormatFileType().trim());
        }

        if (filter.getStorageName() != null)
        {
            filter.setStorageName(filter.getStorageName().trim());
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
     * @param request the business object data notification create request
     *
     * @return the newly created business object data notification registration entity
     */
    private BusinessObjectDataNotificationRegistrationEntity createBusinessObjectDataNotificationEntity(NamespaceEntity namespaceEntity,
        NotificationEventTypeEntity notificationEventTypeEntity, BusinessObjectDefinitionEntity businessObjectDefinitionEntity, FileTypeEntity fileTypeEntity,
        StorageEntity storageEntity, BusinessObjectDataNotificationRegistrationCreateRequest request)
    {
        // Create a new entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            new BusinessObjectDataNotificationRegistrationEntity();

        businessObjectDataNotificationRegistrationEntity.setNamespace(namespaceEntity);
        businessObjectDataNotificationRegistrationEntity.setName(request.getBusinessObjectDataNotificationRegistrationKey().getNotificationName());
        businessObjectDataNotificationRegistrationEntity.setNotificationEventType(notificationEventTypeEntity);
        businessObjectDataNotificationRegistrationEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDataNotificationRegistrationEntity.setUsage(request.getBusinessObjectDataNotificationFilter().getBusinessObjectFormatUsage());
        businessObjectDataNotificationRegistrationEntity.setFileType(fileTypeEntity);
        businessObjectDataNotificationRegistrationEntity
            .setBusinessObjectFormatVersion(request.getBusinessObjectDataNotificationFilter().getBusinessObjectFormatVersion());
        businessObjectDataNotificationRegistrationEntity.setStorage(storageEntity);

        // Create the relative entities for job actions.
        // TODO: We need to add a null/empty list check here, if/when list of job actions will become optional (due to addition of other action types).
        List<NotificationActionEntity> notificationActionEntities = new ArrayList<>();
        businessObjectDataNotificationRegistrationEntity.setNotificationActions(notificationActionEntities);
        for (JobAction jobAction : request.getJobActions())
        {
            // Retrieve and ensure that job definition exists.
            JobDefinitionEntity jobDefinitionEntity = dmDaoHelper.getJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName());

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
            new BusinessObjectDataNotificationRegistrationKey(businessObjectDataNotificationRegistrationEntity.getNamespace().getCode(),
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

        return businessObjectDataNotificationRegistration;
    }
}
