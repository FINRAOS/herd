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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.BusinessObjectDataStatusDao;
import org.finra.herd.dao.BusinessObjectDataStatusDaoTestHelper;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectDefinitionDaoTestHelper;
import org.finra.herd.dao.FileTypeDaoTestHelper;
import org.finra.herd.dao.JobDefinitionDaoTestHelper;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.NamespaceDaoTestHelper;
import org.finra.herd.dao.NotificationEventTypeDao;
import org.finra.herd.dao.NotificationRegistrationDaoTestHelper;
import org.finra.herd.dao.StorageDaoTestHelper;
import org.finra.herd.dao.StorageUnitStatusDao;
import org.finra.herd.dao.StorageUnitStatusDaoTestHelper;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

@Component
public class NotificationRegistrationServiceTestHelper
{
    @Autowired
    private BusinessObjectDataStatusDao businessObjectDataStatusDao;

    @Autowired
    private BusinessObjectDataStatusDaoTestHelper businessObjectDataStatusDaoTestHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoTestHelper businessObjectDefinitionDaoTestHelper;

    @Autowired
    private FileTypeDaoTestHelper fileTypeDaoTestHelper;

    @Autowired
    private JobDefinitionDaoTestHelper jobDefinitionDaoTestHelper;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    @Autowired
    private NotificationEventTypeDao notificationEventTypeDao;

    @Autowired
    private NotificationRegistrationDaoTestHelper notificationRegistrationDaoTestHelper;

    @Autowired
    private StorageDaoTestHelper storageDaoTestHelper;

    @Autowired
    private StorageUnitStatusDao storageUnitStatusDao;

    @Autowired
    private StorageUnitStatusDaoTestHelper storageUnitStatusDaoTestHelper;

    /**
     * Create and persist database entities required for testing.
     */
    public void createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting()
    {
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(AbstractServiceTest.NAMESPACE, Arrays
            .asList(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(),
                NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG.name(), AbstractServiceTest.NOTIFICATION_EVENT_TYPE),
            AbstractServiceTest.BDEF_NAMESPACE, AbstractServiceTest.BDEF_NAME, Arrays.asList(AbstractServiceTest.FORMAT_FILE_TYPE_CODE),
            Arrays.asList(AbstractServiceTest.STORAGE_NAME), Arrays.asList(AbstractServiceTest.BDATA_STATUS, AbstractServiceTest.BDATA_STATUS_2),
            notificationRegistrationDaoTestHelper.getTestJobActions());
    }

    /**
     * Create and persist database entities required for testing.
     *
     * @param namespace the namespace of the business object data notification registration
     * @param notificationEventTypes the list of notification event types
     * @param businessObjectDefinitionNamespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param fileTypes the list of file types
     * @param storageNames the list of storage names
     * @param businessObjectDataStatuses the list of business object data statuses
     * @param jobActions the list of job actions
     */
    public void createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(String namespace, List<String> notificationEventTypes,
        String businessObjectDefinitionNamespace, String businessObjectDefinitionName, List<String> fileTypes, List<String> storageNames,
        List<String> businessObjectDataStatuses, List<JobAction> jobActions)
    {
        // Create a namespace entity, if not exists.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespace);
        if (namespaceEntity == null)
        {
            namespaceDaoTestHelper.createNamespaceEntity(namespace);
        }

        // Create specified notification event types, if not exist.
        if (!CollectionUtils.isEmpty(notificationEventTypes))
        {
            for (String notificationEventType : notificationEventTypes)
            {
                NotificationEventTypeEntity notificationEventTypeEntity = notificationEventTypeDao.getNotificationEventTypeByCode(notificationEventType);
                if (notificationEventTypeEntity == null)
                {
                    notificationRegistrationDaoTestHelper.createNotificationEventTypeEntity(notificationEventType);
                }
            }
        }

        // Create specified business object definition, if not exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDao
            .getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
        if (businessObjectDefinitionEntity == null)
        {
            // Create and persist a business object definition entity.
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName, AbstractServiceTest.DATA_PROVIDER_NAME,
                    AbstractServiceTest.BDEF_DESCRIPTION);
        }

        // Create specified file type entities, if not exist.
        if (!CollectionUtils.isEmpty(fileTypes))
        {
            for (String businessObjectFormatFileType : fileTypes)
            {
                fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create specified storage entities, if not exist.
        if (!CollectionUtils.isEmpty(storageNames))
        {
            for (String storageName : storageNames)
            {
                storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3);
            }
        }

        // Create specified business object data status entities, if not exist.
        if (!CollectionUtils.isEmpty(businessObjectDataStatuses))
        {
            for (String businessObjectDataStatus : businessObjectDataStatuses)
            {
                BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
                    businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(businessObjectDataStatus);
                if (businessObjectDataStatusEntity == null)
                {
                    businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(businessObjectDataStatus);
                }
            }
        }

        // Create specified job definition entities.
        if (!CollectionUtils.isEmpty(jobActions))
        {
            for (JobAction jobAction : jobActions)
            {
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName(),
                    String.format("Description of \"%s.%s\" job definition.", jobAction.getNamespace(), jobAction.getJobName()),
                    String.format("%s.%s.%s", jobAction.getNamespace(), jobAction.getJobName(), AbstractServiceTest.ACTIVITI_ID));
            }
        }
    }

    /**
     * Create and persist database entities required for testing.
     */
    public void createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting()
    {
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(AbstractServiceTest.NAMESPACE,
            Arrays.asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), AbstractServiceTest.NOTIFICATION_EVENT_TYPE),
            AbstractServiceTest.BDEF_NAMESPACE, AbstractServiceTest.BDEF_NAME, Arrays.asList(AbstractServiceTest.FORMAT_FILE_TYPE_CODE),
            Arrays.asList(AbstractServiceTest.STORAGE_NAME), Arrays.asList(AbstractServiceTest.STORAGE_UNIT_STATUS, AbstractServiceTest.STORAGE_UNIT_STATUS_2),
            notificationRegistrationDaoTestHelper.getTestJobActions());
    }

    /**
     * Create and persist database entities required for testing.
     *
     * @param namespace the namespace of the storage unit notification registration
     * @param notificationEventTypes the list of notification event types
     * @param businessObjectDefinitionNamespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param fileTypes the list of file types
     * @param storageNames the list of storage names
     * @param storageUnitStatuses the list of storage unit statuses
     * @param jobActions the list of job actions
     */
    public void createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(String namespace, List<String> notificationEventTypes,
        String businessObjectDefinitionNamespace, String businessObjectDefinitionName, List<String> fileTypes, List<String> storageNames,
        List<String> storageUnitStatuses, List<JobAction> jobActions)
    {
        // Create a namespace entity, if not exists.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespace);
        if (namespaceEntity == null)
        {
            namespaceDaoTestHelper.createNamespaceEntity(namespace);
        }

        // Create specified notification event types, if not exist.
        if (!CollectionUtils.isEmpty(notificationEventTypes))
        {
            for (String notificationEventType : notificationEventTypes)
            {
                NotificationEventTypeEntity notificationEventTypeEntity = notificationEventTypeDao.getNotificationEventTypeByCode(notificationEventType);
                if (notificationEventTypeEntity == null)
                {
                    notificationRegistrationDaoTestHelper.createNotificationEventTypeEntity(notificationEventType);
                }
            }
        }

        // Create specified business object definition, if not exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDao
            .getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(businessObjectDefinitionNamespace, businessObjectDefinitionName));
        if (businessObjectDefinitionEntity == null)
        {
            // Create and persist a business object definition entity.
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(businessObjectDefinitionNamespace, businessObjectDefinitionName, AbstractServiceTest.DATA_PROVIDER_NAME,
                    AbstractServiceTest.BDEF_DESCRIPTION);
        }

        // Create specified file type entities, if not exist.
        if (!CollectionUtils.isEmpty(fileTypes))
        {
            for (String businessObjectFormatFileType : fileTypes)
            {
                fileTypeDaoTestHelper.createFileTypeEntity(businessObjectFormatFileType);
            }
        }

        // Create specified storage entities, if not exist.
        if (!CollectionUtils.isEmpty(storageNames))
        {
            for (String storageName : storageNames)
            {
                storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3);
            }
        }

        // Create specified business object data status entities, if not exist.
        if (!CollectionUtils.isEmpty(storageUnitStatuses))
        {
            for (String storageUnitStatus : storageUnitStatuses)
            {
                StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDao.getStorageUnitStatusByCode(storageUnitStatus);
                if (storageUnitStatusEntity == null)
                {
                    storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(storageUnitStatus);
                }
            }
        }

        // Create specified job definition entities.
        if (!CollectionUtils.isEmpty(jobActions))
        {
            for (JobAction jobAction : jobActions)
            {
                jobDefinitionDaoTestHelper.createJobDefinitionEntity(jobAction.getNamespace(), jobAction.getJobName(),
                    String.format("Description of \"%s.%s\" job definition.", jobAction.getNamespace(), jobAction.getJobName()),
                    String.format("%s.%s.%s", jobAction.getNamespace(), jobAction.getJobName(), AbstractServiceTest.ACTIVITI_ID));
            }
        }
    }

    /**
     * Validates business object data notification registration key against specified arguments.
     *
     * @param expectedNamespaceCode the expected namespace code
     * @param expectedNotificationName the expected notification name
     * @param actualBusinessObjectDataNotificationRegistrationKey the business object data notification registration key object instance to be validated
     */
    public void validateBusinessObjectDataNotificationRegistrationKey(String expectedNamespaceCode, String expectedNotificationName,
        NotificationRegistrationKey actualBusinessObjectDataNotificationRegistrationKey)
    {
        assertNotNull(actualBusinessObjectDataNotificationRegistrationKey);
        assertEquals(expectedNamespaceCode, actualBusinessObjectDataNotificationRegistrationKey.getNamespace());
        assertEquals(expectedNotificationName, actualBusinessObjectDataNotificationRegistrationKey.getNotificationName());
    }
}
