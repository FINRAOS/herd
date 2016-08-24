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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.JobAction;
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
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

@Component
public class NotificationRegistrationDaoTestHelper
{
    @Autowired
    private BusinessObjectDataNotificationRegistrationDao businessObjectDataNotificationRegistrationDao;

    @Autowired
    private BusinessObjectDataStatusDao businessObjectDataStatusDao;

    @Autowired
    private BusinessObjectDataStatusDaoTestHelper businessObjectDataStatusDaoTestHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoTestHelper businessObjectDefinitionDaoTestHelper;

    @Autowired
    private FileTypeDao fileTypeDao;

    @Autowired
    private FileTypeDaoTestHelper fileTypeDaoTestHelper;

    @Autowired
    private JobDefinitionDao jobDefinitionDao;

    @Autowired
    private JobDefinitionDaoTestHelper jobDefinitionDaoTestHelper;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    @Autowired
    private NotificationEventTypeDao notificationEventTypeDao;

    @Autowired
    private NotificationRegistrationStatusDao notificationRegistrationStatusDao;

    @Autowired
    private StorageDao storageDao;

    @Autowired
    private StorageDaoTestHelper storageDaoTestHelper;

    @Autowired
    private StorageUnitNotificationRegistrationDao storageUnitNotificationRegistrationDao;

    @Autowired
    private StorageUnitStatusDao storageUnitStatusDao;

    @Autowired
    private StorageUnitStatusDaoTestHelper storageUnitStatusDaoTestHelper;

    /**
     * Creates and persists a business object data notification registration entity.
     *
     * @param notificationRegistrationKey the business object data notification registration key
     * @param notificationEventTypeCode the notification event type
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param storageName the storage name
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status
     * @param jobActions the list of job actions
     * @param notificationRegistrationStatus the notification registration status
     *
     * @return the newly created business object data notification registration entity
     */
    public BusinessObjectDataNotificationRegistrationEntity createBusinessObjectDataNotificationRegistrationEntity(
        NotificationRegistrationKey notificationRegistrationKey, String notificationEventTypeCode, String businessObjectDefinitionNamespace,
        String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion,
        String storageName, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus, List<JobAction> jobActions,
        String notificationRegistrationStatus)
    {
        // Create a namespace entity if needed.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(notificationRegistrationKey.getNamespace());
        if (namespaceEntity == null)
        {
            namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(notificationRegistrationKey.getNamespace());
        }

        // Create a notification event type entity if needed.
        NotificationEventTypeEntity notificationEventTypeEntity = notificationEventTypeDao.getNotificationEventTypeByCode(notificationEventTypeCode);
        if (notificationEventTypeEntity == null)
        {
            notificationEventTypeEntity = createNotificationEventTypeEntity(notificationEventTypeCode);
        }

        // Create a business object definition entity if needed.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDao
            .getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
        if (businessObjectDefinitionEntity == null)
        {
            businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName, AbstractDaoTest.DATA_PROVIDER_NAME,
                    AbstractDaoTest.BDEF_DESCRIPTION);
        }

        // Create a business object format file type entity if needed.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(businessObjectFormatFileType))
        {
            fileTypeEntity = fileTypeDao.getFileTypeByCode(businessObjectFormatFileType);
            if (fileTypeEntity == null)
            {
                fileTypeEntity = fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create a storage entity if needed.
        StorageEntity storageEntity = null;
        if (StringUtils.isNotBlank(storageName))
        {
            storageEntity = storageDao.getStorageByName(storageName);
            if (storageEntity == null)
            {
                storageEntity = storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3);
            }
        }

        // Create a business object status entity for new status if needed.
        BusinessObjectDataStatusEntity newBusinessObjectDataStatusEntity = null;
        if (StringUtils.isNotBlank(newBusinessObjectDataStatus))
        {
            newBusinessObjectDataStatusEntity = businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(newBusinessObjectDataStatus);
            if (newBusinessObjectDataStatusEntity == null)
            {
                newBusinessObjectDataStatusEntity = businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(newBusinessObjectDataStatus);
            }
        }

        // Create a business object status entity for new status if needed.
        BusinessObjectDataStatusEntity oldBusinessObjectDataStatusEntity = null;
        if (StringUtils.isNotBlank(oldBusinessObjectDataStatus))
        {
            oldBusinessObjectDataStatusEntity = businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(oldBusinessObjectDataStatus);
            if (oldBusinessObjectDataStatusEntity == null)
            {
                oldBusinessObjectDataStatusEntity = businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(oldBusinessObjectDataStatus);
            }
        }

        // Create a notification registration status entity if needed.
        NotificationRegistrationStatusEntity notificationRegistrationStatusEntity =
            notificationRegistrationStatusDao.getNotificationRegistrationStatus(notificationRegistrationStatus);
        if (notificationRegistrationStatusEntity == null)
        {
            notificationRegistrationStatusEntity = createNotificationRegistrationStatusEntity(notificationRegistrationStatus);
        }

        // Create a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            new BusinessObjectDataNotificationRegistrationEntity();

        businessObjectDataNotificationRegistrationEntity.setNamespace(namespaceEntity);
        businessObjectDataNotificationRegistrationEntity.setName(notificationRegistrationKey.getNotificationName());
        businessObjectDataNotificationRegistrationEntity.setNotificationEventType(notificationEventTypeEntity);
        businessObjectDataNotificationRegistrationEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDataNotificationRegistrationEntity.setUsage(businessObjectFormatUsage);
        businessObjectDataNotificationRegistrationEntity.setFileType(fileTypeEntity);
        businessObjectDataNotificationRegistrationEntity.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectDataNotificationRegistrationEntity.setStorage(storageEntity);
        businessObjectDataNotificationRegistrationEntity.setNewBusinessObjectDataStatus(newBusinessObjectDataStatusEntity);
        businessObjectDataNotificationRegistrationEntity.setOldBusinessObjectDataStatus(oldBusinessObjectDataStatusEntity);
        businessObjectDataNotificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusEntity);

        if (!CollectionUtils.isEmpty(jobActions))
        {
            List<NotificationActionEntity> notificationActionEntities = new ArrayList<>();
            businessObjectDataNotificationRegistrationEntity.setNotificationActions(notificationActionEntities);

            for (JobAction jobAction : jobActions)
            {
                // Create a job definition entity if needed.
                JobDefinitionEntity jobDefinitionEntity = jobDefinitionDao.getJobDefinitionByAltKey(jobAction.getNamespace(), jobAction.getJobName());
                if (jobDefinitionEntity == null)
                {
                    jobDefinitionEntity = jobDefinitionDaoTestHelper.createJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName(),
                        String.format("Description of \"%s.%s\" job definition.", jobAction.getNamespace(), jobAction.getJobName()),
                        String.format("%s.%s.%s", jobAction.getNamespace(), jobAction.getJobName(), AbstractDaoTest.ACTIVITI_ID));
                }

                NotificationJobActionEntity notificationJobActionEntity = new NotificationJobActionEntity();
                notificationActionEntities.add(notificationJobActionEntity);
                notificationJobActionEntity.setNotificationRegistration(businessObjectDataNotificationRegistrationEntity);
                notificationJobActionEntity.setJobDefinition(jobDefinitionEntity);
                notificationJobActionEntity.setCorrelationData(jobAction.getCorrelationData());
            }
        }

        return businessObjectDataNotificationRegistrationDao.saveAndRefresh(businessObjectDataNotificationRegistrationEntity);
    }

    /**
     * Creates and persists a new notification event type entity.
     *
     * @param code the notification event type code
     *
     * @return the newly created notification event type entity
     */
    public NotificationEventTypeEntity createNotificationEventTypeEntity(String code)
    {
        NotificationEventTypeEntity notificationEventTypeEntity = new NotificationEventTypeEntity();
        notificationEventTypeEntity.setCode(code);
        notificationEventTypeEntity.setDescription(String.format("Description of \"%s\".", code));
        return notificationEventTypeDao.saveAndRefresh(notificationEventTypeEntity);
    }

    /**
     * Creates and persists a new notification registration status entity.
     *
     * @param code the notification registration status code
     *
     * @return the newly created notification registration status entity
     */
    public NotificationRegistrationStatusEntity createNotificationRegistrationStatusEntity(String code)
    {
        NotificationRegistrationStatusEntity notificationRegistrationStatusEntity = new NotificationRegistrationStatusEntity();
        notificationRegistrationStatusEntity.setCode(code);
        notificationRegistrationStatusEntity.setDescription(String.format("Description of \"%s\".", code));
        return notificationRegistrationStatusDao.saveAndRefresh(notificationRegistrationStatusEntity);
    }

    /**
     * Creates and persists a storage unit notification registration entity.
     *
     * @param notificationRegistrationKey the business object data notification registration key
     * @param notificationEventTypeCode the notification event type
     * @param businessObjectDefinitionNamespace the business object definition namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param storageName the storage name
     * @param newStorageUnitStatus the new storage unit status
     * @param oldStorageUnitStatus the old storage unit status
     * @param jobActions the list of job actions
     * @param notificationRegistrationStatus the notification registration status
     *
     * @return the newly created storage unit notification registration entity
     */
    public StorageUnitNotificationRegistrationEntity createStorageUnitNotificationRegistrationEntity(NotificationRegistrationKey notificationRegistrationKey,
        String notificationEventTypeCode, String businessObjectDefinitionNamespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String storageName, String newStorageUnitStatus, String oldStorageUnitStatus,
        List<JobAction> jobActions, String notificationRegistrationStatus)
    {
        // Create a namespace entity if needed.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(notificationRegistrationKey.getNamespace());
        if (namespaceEntity == null)
        {
            namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(notificationRegistrationKey.getNamespace());
        }

        // Create a notification event type entity if needed.
        NotificationEventTypeEntity notificationEventTypeEntity = notificationEventTypeDao.getNotificationEventTypeByCode(notificationEventTypeCode);
        if (notificationEventTypeEntity == null)
        {
            notificationEventTypeEntity = createNotificationEventTypeEntity(notificationEventTypeCode);
        }

        // Create a business object definition entity if needed.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDao
            .getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
        if (businessObjectDefinitionEntity == null)
        {
            businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName, AbstractDaoTest.DATA_PROVIDER_NAME,
                    AbstractDaoTest.BDEF_DESCRIPTION);
        }

        // Create a business object format file type entity if needed.
        FileTypeEntity fileTypeEntity = null;
        if (StringUtils.isNotBlank(businessObjectFormatFileType))
        {
            fileTypeEntity = fileTypeDao.getFileTypeByCode(businessObjectFormatFileType);
            if (fileTypeEntity == null)
            {
                fileTypeEntity = fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create a storage entity.
        StorageEntity storageEntity = storageDao.getStorageByName(storageName);
        if (storageEntity == null)
        {
            storageEntity = storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3);
        }

        // Create a business object status entity for new status if needed.
        StorageUnitStatusEntity newStorageUnitStatusEntity = null;
        if (StringUtils.isNotBlank(newStorageUnitStatus))
        {
            newStorageUnitStatusEntity = storageUnitStatusDao.getStorageUnitStatusByCode(newStorageUnitStatus);
            if (newStorageUnitStatusEntity == null)
            {
                newStorageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(newStorageUnitStatus);
            }
        }

        // Create a business object status entity for new status if needed.
        StorageUnitStatusEntity oldStorageUnitStatusEntity = null;
        if (StringUtils.isNotBlank(oldStorageUnitStatus))
        {
            oldStorageUnitStatusEntity = storageUnitStatusDao.getStorageUnitStatusByCode(oldStorageUnitStatus);
            if (oldStorageUnitStatusEntity == null)
            {
                oldStorageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(oldStorageUnitStatus);
            }
        }

        // Create a notification registration status entity if needed.
        NotificationRegistrationStatusEntity notificationRegistrationStatusEntity =
            notificationRegistrationStatusDao.getNotificationRegistrationStatus(notificationRegistrationStatus);
        if (notificationRegistrationStatusEntity == null)
        {
            notificationRegistrationStatusEntity = createNotificationRegistrationStatusEntity(notificationRegistrationStatus);
        }

        // Create a business object data notification registration entity.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = new StorageUnitNotificationRegistrationEntity();

        storageUnitNotificationRegistrationEntity.setNamespace(namespaceEntity);
        storageUnitNotificationRegistrationEntity.setName(notificationRegistrationKey.getNotificationName());
        storageUnitNotificationRegistrationEntity.setNotificationEventType(notificationEventTypeEntity);
        storageUnitNotificationRegistrationEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        storageUnitNotificationRegistrationEntity.setUsage(businessObjectFormatUsage);
        storageUnitNotificationRegistrationEntity.setFileType(fileTypeEntity);
        storageUnitNotificationRegistrationEntity.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        storageUnitNotificationRegistrationEntity.setStorage(storageEntity);
        storageUnitNotificationRegistrationEntity.setNewStorageUnitStatus(newStorageUnitStatusEntity);
        storageUnitNotificationRegistrationEntity.setOldStorageUnitStatus(oldStorageUnitStatusEntity);
        storageUnitNotificationRegistrationEntity.setNotificationRegistrationStatus(notificationRegistrationStatusEntity);

        if (!CollectionUtils.isEmpty(jobActions))
        {
            List<NotificationActionEntity> notificationActionEntities = new ArrayList<>();
            storageUnitNotificationRegistrationEntity.setNotificationActions(notificationActionEntities);

            for (JobAction jobAction : jobActions)
            {
                // Create a job definition entity if needed.
                JobDefinitionEntity jobDefinitionEntity = jobDefinitionDao.getJobDefinitionByAltKey(jobAction.getNamespace(), jobAction.getJobName());
                if (jobDefinitionEntity == null)
                {
                    jobDefinitionEntity = jobDefinitionDaoTestHelper.createJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName(),
                        String.format("Description of \"%s.%s\" job definition.", jobAction.getNamespace(), jobAction.getJobName()),
                        String.format("%s.%s.%s", jobAction.getNamespace(), jobAction.getJobName(), AbstractDaoTest.ACTIVITI_ID));
                }

                NotificationJobActionEntity notificationJobActionEntity = new NotificationJobActionEntity();
                notificationActionEntities.add(notificationJobActionEntity);
                notificationJobActionEntity.setNotificationRegistration(storageUnitNotificationRegistrationEntity);
                notificationJobActionEntity.setJobDefinition(jobDefinitionEntity);
                notificationJobActionEntity.setCorrelationData(jobAction.getCorrelationData());
            }
        }

        return storageUnitNotificationRegistrationDao.saveAndRefresh(storageUnitNotificationRegistrationEntity);
    }

    /**
     * Returns a list of test business object data notification registration keys expected to be returned by the relative
     * getNotificationRegistrationKeysByNamespace() method.
     *
     * @return the list of expected notification registration keys
     */
    public List<NotificationRegistrationKey> getExpectedNotificationRegistrationKeys()
    {
        List<NotificationRegistrationKey> keys = new ArrayList<>();

        keys.add(new NotificationRegistrationKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.NOTIFICATION_NAME));
        keys.add(new NotificationRegistrationKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.NOTIFICATION_NAME_2));

        return keys;
    }

    /**
     * Returns a list of test job actions.
     *
     * @return the list of test job actions
     */
    public List<JobAction> getTestJobActions()
    {
        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(AbstractDaoTest.JOB_NAMESPACE, AbstractDaoTest.JOB_NAME, AbstractDaoTest.CORRELATION_DATA));
        jobActions.add(new JobAction(AbstractDaoTest.JOB_NAMESPACE_2, AbstractDaoTest.JOB_NAME_2, AbstractDaoTest.CORRELATION_DATA_2));
        return jobActions;
    }

    /**
     * Returns a list of test job actions.
     *
     * @return the list of test job actions
     */
    public List<JobAction> getTestJobActions2()
    {
        List<JobAction> jobActions = new ArrayList<>();
        jobActions.add(new JobAction(AbstractDaoTest.JOB_NAMESPACE_3, AbstractDaoTest.JOB_NAME_3, AbstractDaoTest.CORRELATION_DATA_3));
        return jobActions;
    }

    /**
     * Returns a list of test notification registration keys.
     *
     * @return the list of test notification registration keys
     */
    public List<NotificationRegistrationKey> getTestNotificationRegistrationKeys()
    {
        List<NotificationRegistrationKey> keys = new ArrayList<>();

        keys.add(new NotificationRegistrationKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.NOTIFICATION_NAME));
        keys.add(new NotificationRegistrationKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.NOTIFICATION_NAME_2));
        keys.add(new NotificationRegistrationKey(AbstractDaoTest.NAMESPACE_2, AbstractDaoTest.NOTIFICATION_NAME));
        keys.add(new NotificationRegistrationKey(AbstractDaoTest.NAMESPACE_2, AbstractDaoTest.NOTIFICATION_NAME_2));

        return keys;
    }
}
