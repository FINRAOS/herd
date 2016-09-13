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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.StorageUnitNotificationFilter;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistration;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationUpdateRequest;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationRegistrationStatusEntity;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity;

/**
 * This class tests various functionality within the storage unit notification registration REST controller.
 */
public class StorageUnitNotificationRegistrationServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting();

        // Create a storage unit notification registration for storage unit status change notification event with DISABLED status.
        StorageUnitNotificationRegistration result = storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(
            new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                    STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                NotificationRegistrationStatusEntity.DISABLED));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(result.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.DISABLED), result);
    }

    @Test
    public void testCreateStorageUnitNotificationRegistrationAlreadyExists()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Try to create a storage unit notification when it already exists.
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
                    NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                        STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an AlreadyExistsException when storage unit notification already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create storage unit notification with name \"%s\" because it already exists for namespace \"%s\".",
                notificationRegistrationKey.getNotificationName(), notificationRegistrationKey.getNamespace()), e.getMessage());
        }
    }

    @Test
    public void testCreateStorageUnitNotificationRegistrationDuplicateJobActions()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting();

        // Try to create a storage unit notification with duplicate job actions.
        StorageUnitNotificationRegistrationCreateRequest request =
            new StorageUnitNotificationRegistrationCreateRequest(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                    STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), Arrays.asList(new JobAction(NAMESPACE.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA),
                new JobAction(NAMESPACE.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA)), NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an IllegalArgumentException when create request contains duplicate job actions.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate job action {namespace: \"%s\", jobName: \"%s\"} found.", request.getJobActions().get(1).getNamespace(),
                request.getJobActions().get(1).getJobName()), e.getMessage());
        }
    }

    @Test
    public void testCreateStorageUnitNotificationRegistrationInvalidParameters()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        StorageUnitNotificationRegistrationCreateRequest request;

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting();

        // Try to create a storage unit notification using non-existing namespace.
        request = new StorageUnitNotificationRegistrationCreateRequest(new NotificationRegistrationKey("I_DO_NOT_EXIST", NOTIFICATION_NAME),
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing namespace.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", request.getStorageUnitNotificationRegistrationKey().getNamespace()), e.getMessage());
        }

        // Try to create a storage unit notification when namespace contains a forward slash character.
        request = new StorageUnitNotificationRegistrationCreateRequest(new NotificationRegistrationKey(addSlash(NAMESPACE), NOTIFICATION_NAME),
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an IllegalArgumentException when namespace contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a storage unit notification when notification name contains a forward slash character.
        request = new StorageUnitNotificationRegistrationCreateRequest(new NotificationRegistrationKey(NAMESPACE, addSlash(NOTIFICATION_NAME)),
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an IllegalArgumentException when notification name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Notification name can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a storage unit notification using non-existing notification event type.
        request = new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey, "I_DO_NOT_EXIST",
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing notification event type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Notification event type with code \"%s\" doesn't exist.", request.getStorageUnitEventType()), e.getMessage());
        }

        // Try to create a storage unit notification using non-supported notification event type.
        request = new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an IllegalArgumentException when using non-supported notification event type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Notification event type \"%s\" is not supported for storage unit notification registration.", request.getStorageUnitEventType()),
                e.getMessage());
        }

        // Try to create a storage unit notification using non-existing business object definition name.
        request = new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object definition name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                request.getStorageUnitNotificationFilter().getBusinessObjectDefinitionName(), request.getStorageUnitNotificationFilter().getNamespace()),
                e.getMessage());
        }

        // Try to create a storage unit notification using non-existing business object format file type.
        request = new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS,
                STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object format file type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("File type with code \"%s\" doesn't exist.", request.getStorageUnitNotificationFilter().getBusinessObjectFormatFileType()),
                e.getMessage());
        }

        // Try to create a storage unit notification using non-existing storage name.
        request = new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST",
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getStorageUnitNotificationFilter().getStorageName()), e.getMessage());
        }

        // Try to create a storage unit notification using non-existing new storage unit status.
        request = new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                "I_DO_NOT_EXIST", STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing new storage unit status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage unit status \"%s\" doesn't exist.", request.getStorageUnitNotificationFilter().getNewStorageUnitStatus()),
                e.getMessage());
        }

        // Try to create a storage unit notification using non-existing old storage unit status.
        request = new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, "I_DO_NOT_EXIST"), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing old storage unit status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage unit status \"%s\" doesn't exist.", request.getStorageUnitNotificationFilter().getOldStorageUnitStatus()),
                e.getMessage());
        }

        // Try to create a storage unit notification when using new and old storage unit statuses that are the same (case-insensitive).
        request = new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS.toUpperCase(), STORAGE_UNIT_STATUS.toLowerCase()), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an IllegalArgumentException when using new and old storage unit statuses that are the same");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The new storage unit status is the same as the old one.", e.getMessage());
        }

        // Try to create a storage unit notification using non-existing job definition.
        request = new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), Arrays.asList(new JobAction(NAMESPACE, "I_DO_NOT_EXIST", CORRELATION_DATA)),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing job definition.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Job definition with namespace \"%s\" and job name \"%s\" doesn't exist.", request.getJobActions().get(0).getNamespace(),
                request.getJobActions().get(0).getJobName()), e.getMessage());
        }

        // Try to create a storage unit notification using non-existing notification registration status.
        request = new StorageUnitNotificationRegistrationCreateRequest(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(), "I_DO_NOT_EXIST");
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing notification registration status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("The notification registration status \"%s\" doesn't exist.", request.getNotificationRegistrationStatus()),
                e.getMessage());
        }
    }

    @Test
    public void testCreateStorageUnitNotificationRegistrationLowerCaseParameters()
    {
        // Create and persist the relative database entities using upper case values.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE.toUpperCase(),
            Arrays.asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()), BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(),
            Arrays.asList(FORMAT_FILE_TYPE_CODE.toUpperCase()), Arrays.asList(STORAGE_NAME.toUpperCase()),
            Arrays.asList(STORAGE_UNIT_STATUS.toUpperCase(), STORAGE_UNIT_STATUS_2.toUpperCase()),
            Arrays.asList(new JobAction(NAMESPACE.toUpperCase(), JOB_NAME.toUpperCase(), BLANK_TEXT)));

        // Create a storage unit notification using lower case input parameters.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(new NotificationRegistrationKey(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toLowerCase()),
                    NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toLowerCase(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                        FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, STORAGE_NAME.toLowerCase(), STORAGE_UNIT_STATUS.toLowerCase(),
                        STORAGE_UNIT_STATUS_2.toLowerCase()),
                    Arrays.asList(new JobAction(NAMESPACE.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA.toLowerCase())),
                    NotificationRegistrationStatusEntity.ENABLED.toLowerCase()));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(),
            new NotificationRegistrationKey(NAMESPACE.toUpperCase(), NOTIFICATION_NAME.toLowerCase()),
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, STORAGE_NAME.toUpperCase(), STORAGE_UNIT_STATUS.toUpperCase(),
                STORAGE_UNIT_STATUS_2.toUpperCase()),
            Arrays.asList(new JobAction(NAMESPACE.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA.toLowerCase())),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testCreateStorageUnitNotificationRegistrationMissingOptionalParametersPassedAsNulls()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting();

        // Create a storage unit notification without specifying any of the optional parameters (passing null values).
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .createStorageUnitNotificationRegistration(new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, null, null, null, STORAGE_NAME, null, null),
                Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, null)), null));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, null, null, null, STORAGE_NAME, null, null),
            Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, null)), NotificationRegistrationStatusEntity.ENABLED),
            resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testCreateStorageUnitNotificationRegistrationMissingOptionalParametersPassedAsWhitespace()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting();

        // Create a storage unit notification without specifying any of the optional parameters (passing whitespace characters).
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .createStorageUnitNotificationRegistration(new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT, BLANK_TEXT, null, STORAGE_NAME, BLANK_TEXT, BLANK_TEXT),
                Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, BLANK_TEXT)), BLANK_TEXT));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, null, null, null, STORAGE_NAME, null, null),
            Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, BLANK_TEXT)), NotificationRegistrationStatusEntity.ENABLED),
            resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testCreateStorageUnitNotificationRegistrationMissingRequiredParameters()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Try to create a storage unit notification instance when namespace is not specified.
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(new NotificationRegistrationKey(BLANK_TEXT, NOTIFICATION_NAME),
                    NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                        STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to create a storage unit notification instance when notification name is not specified.
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(new NotificationRegistrationKey(NAMESPACE, BLANK_TEXT),
                    NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                        STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when notification name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A notification name must be specified.", e.getMessage());
        }

        // Try to create a storage unit notification instance when storage unit notification event type is not specified.
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey, BLANK_TEXT,
                    new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                        STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage unit notification event type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage unit event type must be specified.", e.getMessage());
        }

        // Try to create a storage unit notification instance when object definition name is not specified.
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
                    NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                        STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to create a storage unit notification instance when storage name is not specified.
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
                    NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT,
                        STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }

        // Try to create a storage unit notification instance when job actions are not specified.
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
                    NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                        STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), null, NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when job actions are not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one notification action must be specified.", e.getMessage());
        }

        // Try to create a storage unit notification instance when job action job namespace is not specified.
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
                    NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                        STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), Arrays.asList(new JobAction(BLANK_TEXT, JOB_NAME, CORRELATION_DATA)),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when job action job namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A job action namespace must be specified.", e.getMessage());
        }

        // Try to create a storage unit notification instance when job action job name is not specified.
        try
        {
            storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(notificationRegistrationKey,
                    NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                        STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), Arrays.asList(new JobAction(NAMESPACE, BLANK_TEXT, CORRELATION_DATA)),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when job action job name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A job action job name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateStorageUnitNotificationRegistrationTrimParameters()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting();

        // Create a storage unit notification using input parameters with leading and trailing empty spaces.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .createStorageUnitNotificationRegistration(new StorageUnitNotificationRegistrationCreateRequest(
                new NotificationRegistrationKey(addWhitespace(NAMESPACE), addWhitespace(NOTIFICATION_NAME)),
                addWhitespace(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()),
                new StorageUnitNotificationFilter(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                    addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(STORAGE_NAME), addWhitespace(STORAGE_UNIT_STATUS),
                    addWhitespace(STORAGE_UNIT_STATUS_2)),
                Arrays.asList(new JobAction(addWhitespace(JOB_NAMESPACE), addWhitespace(JOB_NAME), addWhitespace(CORRELATION_DATA))),
                addWhitespace(NotificationRegistrationStatusEntity.ENABLED)));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(),
            new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, addWhitespace(CORRELATION_DATA))),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testCreateStorageUnitNotificationRegistrationUpperCaseParameters()
    {
        // Create and persist the relative database entities using lower case values.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE.toLowerCase(),
            Arrays.asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()), BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(),
            Arrays.asList(FORMAT_FILE_TYPE_CODE.toLowerCase()), Arrays.asList(STORAGE_NAME.toLowerCase()),
            Arrays.asList(STORAGE_UNIT_STATUS.toLowerCase(), STORAGE_UNIT_STATUS_2.toLowerCase()),
            Arrays.asList(new JobAction(NAMESPACE.toLowerCase(), JOB_NAME.toLowerCase(), BLANK_TEXT)));

        // Create a storage unit notification using upper case input parameters.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .createStorageUnitNotificationRegistration(
                new StorageUnitNotificationRegistrationCreateRequest(new NotificationRegistrationKey(NAMESPACE.toUpperCase(), NOTIFICATION_NAME.toUpperCase()),
                    NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toUpperCase(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                        FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, STORAGE_NAME.toUpperCase(), STORAGE_UNIT_STATUS.toUpperCase(),
                        STORAGE_UNIT_STATUS_2.toUpperCase()),
                    Arrays.asList(new JobAction(NAMESPACE.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA.toUpperCase())),
                    NotificationRegistrationStatusEntity.ENABLED.toUpperCase()));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(),
            new NotificationRegistrationKey(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toUpperCase()),
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, STORAGE_NAME.toLowerCase(), STORAGE_UNIT_STATUS.toLowerCase(),
                STORAGE_UNIT_STATUS_2.toLowerCase()),
            Arrays.asList(new JobAction(NAMESPACE.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA.toUpperCase())),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testDeleteStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Validate that this storage unit notification exists.
        assertNotNull(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(notificationRegistrationKey));

        // Delete this storage unit notification.
        StorageUnitNotificationRegistration deletedStorageUnitNotificationRegistration =
            storageUnitNotificationRegistrationService.deleteStorageUnitNotificationRegistration(notificationRegistrationKey);

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(storageUnitNotificationRegistrationEntity.getId(),
            new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME), NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), deletedStorageUnitNotificationRegistration);

        // Ensure that this storage unit notification is no longer there.
        assertNull(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(notificationRegistrationKey));
    }

    @Test
    public void testDeleteStorageUnitNotificationRegistrationLowerCaseParameters()
    {
        // Create and persist a storage unit notification registration entity using upper case alternate key values.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE.toUpperCase(), NOTIFICATION_NAME.toUpperCase());
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Validate that this storage unit notification exists.
        assertNotNull(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(notificationRegistrationKey));

        // Delete this storage unit notification using lower case input parameters.
        StorageUnitNotificationRegistration deletedStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .deleteStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(storageUnitNotificationRegistrationEntity.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), deletedStorageUnitNotificationRegistration);

        // Ensure that this storage unit notification is no longer there.
        assertNull(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(notificationRegistrationKey));
    }

    @Test
    public void testDeleteStorageUnitNotificationRegistrationMissingRequiredParameters()
    {
        // Try to delete a storage unit notification when namespace is not specified.
        try
        {
            storageUnitNotificationRegistrationService
                .deleteStorageUnitNotificationRegistration(new NotificationRegistrationKey(BLANK_TEXT, NOTIFICATION_NAME));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to delete a storage unit notification when notification name is not specified.
        try
        {
            storageUnitNotificationRegistrationService.deleteStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when notification name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A notification name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteStorageUnitNotificationRegistrationStorageUnitNotificationNoExists()
    {
        // Try to delete a non-existing storage unit notification.
        try
        {
            storageUnitNotificationRegistrationService.deleteStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME));
            fail("Should throw an ObjectNotFoundException when trying to delete a non-existing storage unit notification.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("Storage unit notification registration with name \"%s\" does not exist for \"%s\" namespace.", NOTIFICATION_NAME, NAMESPACE),
                e.getMessage());
        }
    }

    @Test
    public void testDeleteStorageUnitNotificationRegistrationTrimParameters()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Validate that this storage unit notification exists.
        assertNotNull(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(notificationRegistrationKey));

        // Delete this storage unit notification using input parameters with leading and trailing empty spaces.
        StorageUnitNotificationRegistration deletedStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .deleteStorageUnitNotificationRegistration(new NotificationRegistrationKey(addWhitespace(NAMESPACE), addWhitespace(NOTIFICATION_NAME)));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(storageUnitNotificationRegistrationEntity.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), deletedStorageUnitNotificationRegistration);

        // Ensure that this storage unit notification is no longer there.
        assertNull(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(notificationRegistrationKey));
    }

    @Test
    public void testDeleteStorageUnitNotificationRegistrationUpperCaseParameters()
    {
        // Create and persist a storage unit notification registration entity using lower case alternate key values.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toLowerCase());
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Validate that this storage unit notification exists.
        assertNotNull(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(notificationRegistrationKey));

        // Delete this storage unit notification using upper case input parameters.
        StorageUnitNotificationRegistration deletedStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .deleteStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE.toUpperCase(), NOTIFICATION_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(storageUnitNotificationRegistrationEntity.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), deletedStorageUnitNotificationRegistration);

        // Ensure that this storage unit notification is no longer there.
        assertNull(storageUnitNotificationRegistrationDao.getStorageUnitNotificationRegistrationByAltKey(notificationRegistrationKey));
    }

    @Test
    public void testGetStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve the storage unit notification registration.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration =
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistration(notificationRegistrationKey);

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationLowerCaseParameters()
    {
        // Create and persist a storage unit notification registration entity using upper case alternate key values.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE.toUpperCase(), NOTIFICATION_NAME.toUpperCase());
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve the storage unit notification using lower case input parameters.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .getStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(storageUnitNotificationRegistrationEntity.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationMissingRequiredParameters()
    {
        // Try to get a storage unit notification registration when namespace is not specified.
        try
        {
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistration(new NotificationRegistrationKey(BLANK_TEXT, NOTIFICATION_NAME));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to get a storage unit notification when notification name is not specified.
        try
        {
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when notification name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A notification name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationNoExists()
    {
        // Try to retrieve a non-existing storage unit notification.
        try
        {
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME));
            fail("Should throw an ObjectNotFoundException when trying to retrieve a non-existing storage unit notification.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("Storage unit notification registration with name \"%s\" does not exist for \"%s\" namespace.", NOTIFICATION_NAME, NAMESPACE),
                e.getMessage());
        }
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationTrimParameters()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve the storage unit notification using input parameters with leading and trailing empty spaces.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .getStorageUnitNotificationRegistration(new NotificationRegistrationKey(addWhitespace(NAMESPACE), addWhitespace(NOTIFICATION_NAME)));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(storageUnitNotificationRegistrationEntity.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationUpperCaseParameters()
    {
        // Create and persist a storage unit notification registration entity using lower case alternate key values.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toLowerCase());
        StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistrationEntity = notificationRegistrationDaoTestHelper
            .createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve the storage unit notification using upper case input parameters.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .getStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE.toUpperCase(), NOTIFICATION_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(storageUnitNotificationRegistrationEntity.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNamespace()
    {
        // Create and persist storage unit notification entities.
        for (NotificationRegistrationKey notificationRegistrationKey : notificationRegistrationDaoTestHelper.getTestNotificationRegistrationKeys())
        {
            notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, null, STORAGE_NAME, null, null, notificationRegistrationDaoTestHelper.getTestJobActions(),
                NotificationRegistrationStatusEntity.ENABLED);
        }

        // Retrieve a list of storage unit notification registration keys.
        StorageUnitNotificationRegistrationKeys resultKeys =
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(notificationRegistrationDaoTestHelper.getExpectedNotificationRegistrationKeys(), resultKeys.getStorageUnitNotificationRegistrationKeys());
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNamespaceLowerCaseParameters()
    {
        // Create and persist storage unit notification entities using upper case alternate key values.
        for (NotificationRegistrationKey notificationRegistrationKey : notificationRegistrationDaoTestHelper.getTestNotificationRegistrationKeys())
        {
            NotificationRegistrationKey notificationRegistrationKeyUpperCase =
                new NotificationRegistrationKey(notificationRegistrationKey.getNamespace().toUpperCase(),
                    notificationRegistrationKey.getNotificationName().toUpperCase());
            notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKeyUpperCase,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        }

        // Retrieve a list of storage unit notification registration keys using lower case namespace code value.
        StorageUnitNotificationRegistrationKeys resultKeys =
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNamespace(NAMESPACE.toLowerCase());

        // Validate the returned object.
        List<NotificationRegistrationKey> expectedKeys = new ArrayList<>();
        for (NotificationRegistrationKey origKey : notificationRegistrationDaoTestHelper.getExpectedNotificationRegistrationKeys())
        {
            NotificationRegistrationKey expectedKey = new NotificationRegistrationKey();
            expectedKeys.add(expectedKey);
            expectedKey.setNamespace(origKey.getNamespace().toUpperCase());
            expectedKey.setNotificationName(origKey.getNotificationName().toUpperCase());
        }
        assertEquals(expectedKeys, resultKeys.getStorageUnitNotificationRegistrationKeys());
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNamespaceMissingRequiredParameters()
    {
        // Try to get storage unit notifications when namespace is not specified.
        try
        {
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNamespace(BLANK_TEXT);
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNamespaceNamespaceNoExists()
    {
        // Try to retrieve storage unit notifications for a non-existing namespace.
        try
        {
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNamespace(NAMESPACE);
            fail("Should throw an ObjectNotFoundException when using non-existing namespace.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", NAMESPACE), e.getMessage());
        }
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNamespaceStorageUnitNotificationsNoExist()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting();

        // Retrieve an empty list of storage unit notification registration keys.
        StorageUnitNotificationRegistrationKeys resultKeys =
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(new ArrayList<NotificationRegistrationKey>(), resultKeys.getStorageUnitNotificationRegistrationKeys());
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNamespaceTrimParameters()
    {
        // Create and persist storage unit notification entities.
        for (NotificationRegistrationKey notificationRegistrationKey : notificationRegistrationDaoTestHelper.getTestNotificationRegistrationKeys())
        {
            notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        }

        // Retrieve a list of storage unit notification registration keys using input parameters with leading and trailing empty spaces.
        StorageUnitNotificationRegistrationKeys resultKeys =
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNamespace(addWhitespace(NAMESPACE));

        // Validate the returned object.
        assertEquals(notificationRegistrationDaoTestHelper.getExpectedNotificationRegistrationKeys(), resultKeys.getStorageUnitNotificationRegistrationKeys());
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNamespaceUpperCaseParameters()
    {
        // Create and persist storage unit notification entities using lower case alternate key values.
        for (NotificationRegistrationKey notificationRegistrationKey : notificationRegistrationDaoTestHelper.getTestNotificationRegistrationKeys())
        {
            NotificationRegistrationKey notificationRegistrationKeyLowerCase =
                new NotificationRegistrationKey(notificationRegistrationKey.getNamespace().toLowerCase(),
                    notificationRegistrationKey.getNotificationName().toLowerCase());
            notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKeyLowerCase,
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2,
                notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        }

        // Retrieve a list of storage unit notification registration keys using upper case namespace code value.
        StorageUnitNotificationRegistrationKeys resultKeys =
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNamespace(NAMESPACE.toUpperCase());

        // Validate the returned object.
        List<NotificationRegistrationKey> expectedKeys = new ArrayList<>();
        for (NotificationRegistrationKey origKey : notificationRegistrationDaoTestHelper.getExpectedNotificationRegistrationKeys())
        {
            NotificationRegistrationKey expectedKey = new NotificationRegistrationKey();
            expectedKeys.add(expectedKey);
            expectedKey.setNamespace(origKey.getNamespace().toLowerCase());
            expectedKey.setNotificationName(origKey.getNotificationName().toLowerCase());
        }
        assertEquals(expectedKeys, resultKeys.getStorageUnitNotificationRegistrationKeys());
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilter()
    {
        // Create a storage unit notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of storage unit notification registration keys.
        assertEquals(new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)), storageUnitNotificationRegistrationService
            .getStorageUnitNotificationRegistrationsByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilterInvalidParameters()
    {
        // Create a storage unit notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of storage unit notification registration keys.
        assertEquals(new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)), storageUnitNotificationRegistrationService
            .getStorageUnitNotificationRegistrationsByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));

        // Try invalid values for all input parameters.
        assertTrue(storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNotificationFilter(
            new StorageUnitNotificationFilter("I_DO_NO_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)).getStorageUnitNotificationRegistrationKeys().isEmpty());
        assertTrue(storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNotificationFilter(
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, "I_DO_NO_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)).getStorageUnitNotificationRegistrationKeys().isEmpty());
        assertTrue(storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNotificationFilter(
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NO_EXIST", FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)).getStorageUnitNotificationRegistrationKeys().isEmpty());
        assertTrue(storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNotificationFilter(
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NO_EXIST", NO_FORMAT_VERSION, NO_STORAGE_NAME,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)).getStorageUnitNotificationRegistrationKeys().isEmpty());
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilterLowerCaseParameters()
    {
        // Create a storage unit notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of storage unit notification registration keys using lower case input parameters.
        assertEquals(new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)), storageUnitNotificationRegistrationService
            .getStorageUnitNotificationRegistrationsByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                    FORMAT_FILE_TYPE_CODE.toLowerCase(), NO_FORMAT_VERSION, NO_STORAGE_NAME, NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilterMissingOptionalParameters()
    {
        // Create a storage unit notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of storage unit notification registration keys by not specifying optional parameters.
        assertEquals(new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)), storageUnitNotificationRegistrationService
            .getStorageUnitNotificationRegistrationsByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));
        assertEquals(new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)), storageUnitNotificationRegistrationService
            .getStorageUnitNotificationRegistrationsByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilterMissingRequiredParameters()
    {
        // Try to get storage unit notification registrations when business object definition namespace is not specified.
        try
        {
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNotificationFilter(
                new StorageUnitNotificationFilter(NO_BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS));
            fail("Should throw an IllegalArgumentException when business object definition namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition namespace must be specified.", e.getMessage());
        }

        // Try to get storage unit notification registrations when business object definition name is not specified.
        try
        {
            storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE, NO_BDEF_NAME, NO_FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_STORAGE_NAME,
                    NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilterTrimParameters()
    {
        // Create a storage unit notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of storage unit notification registration keys using input parameters with leading and trailing empty spaces.
        assertEquals(new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)), storageUnitNotificationRegistrationService
            .getStorageUnitNotificationRegistrationsByNotificationFilter(
                new StorageUnitNotificationFilter(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                    addWhitespace(FORMAT_FILE_TYPE_CODE), NO_FORMAT_VERSION, NO_STORAGE_NAME, NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));
    }

    @Test
    public void testGetStorageUnitNotificationRegistrationsByNotificationFilterUpperCaseParameters()
    {
        // Create a storage unit notification registration key.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Retrieve a list of storage unit notification registration keys using upper case input parameters.
        assertEquals(new StorageUnitNotificationRegistrationKeys(Arrays.asList(notificationRegistrationKey)), storageUnitNotificationRegistrationService
            .getStorageUnitNotificationRegistrationsByNotificationFilter(
                new StorageUnitNotificationFilter(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                    FORMAT_FILE_TYPE_CODE.toUpperCase(), NO_FORMAT_VERSION, NO_STORAGE_NAME, NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS)));
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistration()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create database entities required for testing.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE, Arrays
            .asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()), BDEF_NAMESPACE_2, BDEF_NAME_2,
            Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME, STORAGE_NAME_2),
            Arrays.asList(STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS_3, STORAGE_UNIT_STATUS_4),
            notificationRegistrationDaoTestHelper.getTestJobActions2());

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Update the storage unit notification registration.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2,
                        STORAGE_NAME_2, STORAGE_UNIT_STATUS_3, NO_STORAGE_UNIT_STATUS), notificationRegistrationDaoTestHelper.getTestJobActions2(),
                    NotificationRegistrationStatusEntity.DISABLED));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2,
                STORAGE_UNIT_STATUS_3, NO_STORAGE_UNIT_STATUS), notificationRegistrationDaoTestHelper.getTestJobActions2(),
            NotificationRegistrationStatusEntity.DISABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistrationInvalidParameters()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        StorageUnitNotificationRegistrationUpdateRequest request;

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE, Arrays
            .asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), NOTIFICATION_EVENT_TYPE), BDEF_NAMESPACE_2, BDEF_NAME_2,
            Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME, STORAGE_NAME_2),
            Arrays.asList(STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS_3, STORAGE_UNIT_STATUS_4),
            notificationRegistrationDaoTestHelper.getTestJobActions2());

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Try to update a storage unit notification using non-existing notification event type.
        request = new StorageUnitNotificationRegistrationUpdateRequest("I_DO_NOT_EXIST",
            new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS,
                STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing notification event type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Notification event type with code \"%s\" doesn't exist.", request.getStorageUnitEventType()), e.getMessage());
        }

        // Try to update a storage unit notification using non-supported notification event type.
        request = new StorageUnitNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE,
            new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS,
                STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
            fail("Should throw an IllegalArgumentException when using non-supported notification event type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Notification event type \"%s\" is not supported for storage unit notification registration.", request.getStorageUnitEventType()),
                e.getMessage());
        }

        // Try to update a storage unit notification using non-existing business object definition name.
        request = new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object definition name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                request.getStorageUnitNotificationFilter().getBusinessObjectDefinitionName(), request.getStorageUnitNotificationFilter().getNamespace()),
                e.getMessage());
        }

        // Try to update a storage unit notification using non-existing business object format file type.
        request = new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS,
                STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(), NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object format file type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("File type with code \"%s\" doesn't exist.", request.getStorageUnitNotificationFilter().getBusinessObjectFormatFileType()),
                e.getMessage());
        }

        // Try to update a storage unit notification using non-existing storage name.
        request = new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST",
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getStorageUnitNotificationFilter().getStorageName()), e.getMessage());
        }

        // Try to update a storage unit notification using non-existing new storage unit status.
        request = new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                "I_DO_NOT_EXIST", STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing new storage unit status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage unit status \"%s\" doesn't exist.", request.getStorageUnitNotificationFilter().getNewStorageUnitStatus()),
                e.getMessage());
        }

        // Try to update a storage unit notification using non-existing old storage unit status.
        request = new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, "I_DO_NOT_EXIST"), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing old storage unit status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage unit status \"%s\" doesn't exist.", request.getStorageUnitNotificationFilter().getOldStorageUnitStatus()),
                e.getMessage());
        }

        // Try to create a storage unit notification when using new and old storage unit statuses that are the same (case-insensitive).
        request = new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS.toUpperCase(), STORAGE_UNIT_STATUS.toLowerCase()), notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
            fail("Should throw an IllegalArgumentException when using new and old storage unit statuses that are the same");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The new storage unit status is the same as the old one.", e.getMessage());
        }

        // Try to update a storage unit notification registration using non-existing job definition.
        request = new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), Arrays.asList(new JobAction(NAMESPACE, "I_DO_NOT_EXIST", CORRELATION_DATA)),
            NotificationRegistrationStatusEntity.ENABLED);
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing job definition.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Job definition with namespace \"%s\" and job name \"%s\" doesn't exist.", request.getJobActions().get(0).getNamespace(),
                request.getJobActions().get(0).getJobName()), e.getMessage());
        }

        // Try to update a storage unit notification using non-existing notification registration status.
        request = new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(), "I_DO_NOT_EXIST");
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(notificationRegistrationKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing notification registration status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("The notification registration status \"%s\" doesn't exist.", request.getNotificationRegistrationStatus()),
                e.getMessage());
        }
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistrationLowerCaseParameters()
    {
        // Create database entities required for testing using upper case alternate key values.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE.toUpperCase(), Arrays
            .asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toUpperCase(),
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toUpperCase()), BDEF_NAMESPACE_2.toUpperCase(),
            BDEF_NAME_2.toUpperCase(), Arrays.asList(FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE_2.toUpperCase()),
            Arrays.asList(STORAGE_NAME.toUpperCase(), STORAGE_NAME_2.toUpperCase()), Arrays
            .asList(STORAGE_UNIT_STATUS.toUpperCase(), STORAGE_UNIT_STATUS_2.toUpperCase(), STORAGE_UNIT_STATUS_3.toUpperCase(),
                STORAGE_UNIT_STATUS_4.toUpperCase()), Arrays.asList(new JobAction(JOB_NAMESPACE_2.toUpperCase(), JOB_NAME_2.toUpperCase(), null)));

        // Create and persist a storage unit notification registration entity using upper case alternate key values.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE.toUpperCase(), NOTIFICATION_NAME.toUpperCase());
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toUpperCase(), BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(),
            FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, STORAGE_NAME.toUpperCase(), STORAGE_UNIT_STATUS.toUpperCase(),
            STORAGE_UNIT_STATUS_2.toUpperCase(), Arrays.asList(new JobAction(JOB_NAMESPACE.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA)),
            NotificationRegistrationStatusEntity.ENABLED);

        // Update the storage unit notification using lower case input parameters.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toLowerCase()),
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toLowerCase(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2.toLowerCase(), BDEF_NAME_2.toLowerCase(), FORMAT_USAGE_CODE_2.toLowerCase(),
                        FORMAT_FILE_TYPE_CODE_2.toLowerCase(), FORMAT_VERSION_2, STORAGE_NAME_2.toLowerCase(), STORAGE_UNIT_STATUS_3.toLowerCase(),
                        NO_STORAGE_UNIT_STATUS),
                    Arrays.asList(new JobAction(JOB_NAMESPACE_2.toLowerCase(), JOB_NAME_2.toLowerCase(), CORRELATION_DATA_2.toLowerCase())), "enabled"));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toUpperCase(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE_2.toUpperCase(), BDEF_NAME_2.toUpperCase(), FORMAT_USAGE_CODE_2.toLowerCase(),
                FORMAT_FILE_TYPE_CODE_2.toUpperCase(), FORMAT_VERSION_2, STORAGE_NAME_2.toUpperCase(), STORAGE_UNIT_STATUS_3.toUpperCase(),
                NO_STORAGE_UNIT_STATUS),
            Arrays.asList(new JobAction(JOB_NAMESPACE_2.toUpperCase(), JOB_NAME_2.toUpperCase(), CORRELATION_DATA_2.toLowerCase())),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistrationMissingOptionalParametersPassedAsNulls()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create other database entities required for testing.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE, Arrays
            .asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()), BDEF_NAMESPACE_2, BDEF_NAME_2, null, null,
            Arrays.asList(STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions());

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Update the storage unit notification without specifying any of the optional parameters (passing whitespace characters).
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .updateStorageUnitNotificationRegistration(notificationRegistrationKey,
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, null, null, null, STORAGE_NAME, null, null),
                    Arrays.asList(new JobAction(JOB_NAMESPACE_2, JOB_NAME_2, null)), NotificationRegistrationStatusEntity.ENABLED));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, null, null, null, STORAGE_NAME, null, null),
            Arrays.asList(new JobAction(JOB_NAMESPACE_2, JOB_NAME_2, null)), NotificationRegistrationStatusEntity.ENABLED),
            resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistrationMissingOptionalParametersPassedAsWhitespace()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create other database entities required for testing.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE, Arrays
            .asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()), BDEF_NAMESPACE_2, BDEF_NAME_2, null, null,
            Arrays.asList(STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions());

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Update the storage unit notification without specifying any of the optional parameters (passing whitespace characters).
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .updateStorageUnitNotificationRegistration(notificationRegistrationKey,
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, BLANK_TEXT, BLANK_TEXT, null, STORAGE_NAME, BLANK_TEXT, BLANK_TEXT),
                    Arrays.asList(new JobAction(JOB_NAMESPACE_2, JOB_NAME_2, BLANK_TEXT)), NotificationRegistrationStatusEntity.ENABLED));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, null, null, null, STORAGE_NAME, null, null),
            Arrays.asList(new JobAction(JOB_NAMESPACE_2, JOB_NAME_2, BLANK_TEXT)), NotificationRegistrationStatusEntity.ENABLED),
            resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistrationMissingRequiredParameters()
    {
        // Try to update a storage unit notification registration when namespace is not specified.
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(BLANK_TEXT, NOTIFICATION_NAME),
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2,
                        STORAGE_NAME_2, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions2(),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to update a storage unit notification registration when notification name is not specified.
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE, BLANK_TEXT),
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2,
                        STORAGE_NAME_2, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions2(),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when notification name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A notification name must be specified.", e.getMessage());
        }

        // Try to update a storage unit notification registration when storage name is not specified.
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT,
                        STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions(),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }

        // Try to update a storage unit notification registration when notification status is not specified.
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2,
                        STORAGE_NAME_2, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2), notificationRegistrationDaoTestHelper.getTestJobActions2(), BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when notification registration status is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A notification registration status must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistrationNoExists()
    {
        // Try to update a non-existing storage unit notification registration.
        try
        {
            storageUnitNotificationRegistrationService.updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2,
                        STORAGE_NAME_2, STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS), notificationRegistrationDaoTestHelper.getTestJobActions2(),
                    NotificationRegistrationStatusEntity.ENABLED));
            fail("Should throw an ObjectNotFoundException when trying to update a non-existing storage unit notification.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("Storage unit notification registration with name \"%s\" does not exist for \"%s\" namespace.", NOTIFICATION_NAME, NAMESPACE),
                e.getMessage());
        }
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistrationTrimParameters()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create database entities required for testing.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE, Arrays
            .asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()), BDEF_NAMESPACE_2, BDEF_NAME_2,
            Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME, STORAGE_NAME_2),
            Arrays.asList(STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS_3, STORAGE_UNIT_STATUS_4),
            notificationRegistrationDaoTestHelper.getTestJobActions2());

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Update the storage unit notification using input parameters with leading and trailing empty spaces.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(addWhitespace(NAMESPACE), addWhitespace(NOTIFICATION_NAME)),
                new StorageUnitNotificationRegistrationUpdateRequest(
                    addWhitespace(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()),
                    new StorageUnitNotificationFilter(addWhitespace(BDEF_NAMESPACE_2), addWhitespace(BDEF_NAME_2), addWhitespace(FORMAT_USAGE_CODE_2),
                        addWhitespace(FORMAT_FILE_TYPE_CODE_2), FORMAT_VERSION_2, addWhitespace(STORAGE_NAME_2), addWhitespace(STORAGE_UNIT_STATUS_3),
                        NO_STORAGE_UNIT_STATUS),
                    Arrays.asList(new JobAction(addWhitespace(JOB_NAMESPACE_2), addWhitespace(JOB_NAME_2), addWhitespace(CORRELATION_DATA_2))),
                    BLANK_TEXT + NotificationRegistrationStatusEntity.ENABLED + BLANK_TEXT));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2,
                STORAGE_UNIT_STATUS_3, NO_STORAGE_UNIT_STATUS), Arrays.asList(new JobAction(JOB_NAMESPACE_2, JOB_NAME_2, addWhitespace(CORRELATION_DATA_2))),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistrationUpperCaseParameters()
    {
        // Create database entities required for testing using lower case alternate key values.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE.toLowerCase(), Arrays
            .asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toLowerCase(),
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toLowerCase()), BDEF_NAMESPACE_2.toLowerCase(),
            BDEF_NAME_2.toLowerCase(), Arrays.asList(FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE_2.toLowerCase()),
            Arrays.asList(STORAGE_NAME.toLowerCase(), STORAGE_NAME_2.toLowerCase()), Arrays
            .asList(STORAGE_UNIT_STATUS.toLowerCase(), STORAGE_UNIT_STATUS_2.toLowerCase(), STORAGE_UNIT_STATUS_3.toLowerCase(),
                STORAGE_UNIT_STATUS_4.toLowerCase()), Arrays.asList(new JobAction(JOB_NAMESPACE_2.toLowerCase(), JOB_NAME_2.toLowerCase(), null)));

        // Create and persist a storage unit notification registration entity using lower case alternate key values.
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toLowerCase());
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toLowerCase(), BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(),
            FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, STORAGE_NAME.toLowerCase(), STORAGE_UNIT_STATUS.toLowerCase(),
            STORAGE_UNIT_STATUS_2.toLowerCase(), Arrays.asList(new JobAction(JOB_NAMESPACE.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA)),
            NotificationRegistrationStatusEntity.ENABLED);

        // Update the storage unit notification using upper case input parameters.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE.toUpperCase(), NOTIFICATION_NAME.toUpperCase()),
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toUpperCase(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2.toUpperCase(), BDEF_NAME_2.toUpperCase(), FORMAT_USAGE_CODE_2.toUpperCase(),
                        FORMAT_FILE_TYPE_CODE_2.toUpperCase(), FORMAT_VERSION_2, STORAGE_NAME_2.toUpperCase(), STORAGE_UNIT_STATUS_3.toUpperCase(),
                        NO_STORAGE_UNIT_STATUS),
                    Arrays.asList(new JobAction(JOB_NAMESPACE_2.toUpperCase(), JOB_NAME_2.toUpperCase(), CORRELATION_DATA_2.toUpperCase())),
                    NotificationRegistrationStatusEntity.ENABLED));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(),
            new NotificationRegistrationKey(NAMESPACE.toLowerCase(), NOTIFICATION_NAME.toLowerCase()),
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name().toLowerCase(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE_2.toLowerCase(), BDEF_NAME_2.toLowerCase(), FORMAT_USAGE_CODE_2.toUpperCase(),
                FORMAT_FILE_TYPE_CODE_2.toLowerCase(), FORMAT_VERSION_2, STORAGE_NAME_2.toLowerCase(), STORAGE_UNIT_STATUS_3.toLowerCase(),
                NO_STORAGE_UNIT_STATUS),
            Arrays.asList(new JobAction(JOB_NAMESPACE_2.toLowerCase(), JOB_NAME_2.toLowerCase(), CORRELATION_DATA_2.toUpperCase())),
            NotificationRegistrationStatusEntity.ENABLED), resultStorageUnitNotificationRegistration);
    }

    @Test
    public void testUpdateStorageUnitNotificationRegistrationWithDisabledStatus()
    {
        NotificationRegistrationKey notificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME);

        // Create database entities required for testing.
        createDatabaseEntitiesForStorageUnitNotificationRegistrationTesting(NAMESPACE, Arrays
            .asList(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name()), BDEF_NAMESPACE_2, BDEF_NAME_2,
            Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME, STORAGE_NAME_2),
            Arrays.asList(STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, STORAGE_UNIT_STATUS_3, STORAGE_UNIT_STATUS_4),
            notificationRegistrationDaoTestHelper.getTestJobActions2());

        // Create and persist a storage unit notification registration entity.
        notificationRegistrationDaoTestHelper.createStorageUnitNotificationRegistrationEntity(notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(), BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, STORAGE_UNIT_STATUS, STORAGE_UNIT_STATUS_2, notificationRegistrationDaoTestHelper.getTestJobActions(),
            NotificationRegistrationStatusEntity.ENABLED);

        // Update the storage unit notification registration.
        StorageUnitNotificationRegistration resultStorageUnitNotificationRegistration = storageUnitNotificationRegistrationService
            .updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(NAMESPACE, NOTIFICATION_NAME),
                new StorageUnitNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
                    new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2,
                        STORAGE_NAME_2, STORAGE_UNIT_STATUS_3, NO_STORAGE_UNIT_STATUS), notificationRegistrationDaoTestHelper.getTestJobActions2(),
                    NotificationRegistrationStatusEntity.DISABLED));

        // Validate the returned object.
        assertEquals(new StorageUnitNotificationRegistration(resultStorageUnitNotificationRegistration.getId(), notificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name(),
            new StorageUnitNotificationFilter(BDEF_NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2,
                STORAGE_UNIT_STATUS_3, NO_STORAGE_UNIT_STATUS), notificationRegistrationDaoTestHelper.getTestJobActions2(),
            NotificationRegistrationStatusEntity.DISABLED), resultStorageUnitNotificationRegistration);
    }
}
