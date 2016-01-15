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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistration;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationUpdateRequest;
import org.finra.herd.model.api.xml.JobAction;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;

/**
 * This class tests various functionality within the business object data notification registration REST controller.
 */
public class BusinessObjectDataNotificationRegistrationServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(createBusinessObjectDataNotificationRegistrationCreateRequest(
                businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationMissingRequiredParameters()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Try to create a business object data notification instance when namespace is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(new NotificationRegistrationKey(BLANK_TEXT, NOTIFICATION_NAME),
                    NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS,
                    BDATA_STATUS_2, getTestJobActions()));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to create a business object data notification instance when notification name is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(new NotificationRegistrationKey(NAMESPACE_CD, BLANK_TEXT),
                    NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS,
                    BDATA_STATUS_2, getTestJobActions()));
            fail("Should throw an IllegalArgumentException when notification name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A notification name must be specified.", e.getMessage());
        }

        // Try to create a business object data notification instance when business object data notification event type is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, BLANK_TEXT, NAMESPACE_CD, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions()));
            fail("Should throw an IllegalArgumentException when business object data notification event type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data event type must be specified.", e.getMessage());
        }

        // Try to create a business object data notification instance when object definition name is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                    NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                    getTestJobActions()));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to create a business object data notification instance when job actions are not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                    NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, null));
            fail("Should throw an IllegalArgumentException when job actions are not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At least one notification action must be specified.", e.getMessage());
        }

        // Try to create a business object data notification instance when job action namespace is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                    NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, Arrays.asList(
                        new JobAction(BLANK_TEXT, JOB_NAME, CORRELATION_DATA))));
            fail("Should throw an IllegalArgumentException when job action namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A job action namespace must be specified.", e.getMessage());
        }

        // Try to create a business object data notification instance when business object format version is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                    NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, Arrays.asList(
                        new JobAction(NAMESPACE_CD, BLANK_TEXT, CORRELATION_DATA))));
            fail("Should throw an IllegalArgumentException when job action job name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A job action job name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationMissingOptionalParametersPassedAsWhitespace()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create a business object definition.
        createBusinessObjectDefinitionEntity(BOD_NAMESPACE, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(createBusinessObjectDataNotificationRegistrationCreateRequest(
                businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, BLANK_TEXT, BLANK_TEXT, null, BLANK_TEXT,
                BLANK_TEXT, BLANK_TEXT, Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, BLANK_TEXT))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME, null,
                null, null, null, null, null), Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, BLANK_TEXT)), "ENABLED"),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationMissingOptionalParametersPassedAsNulls()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create a business object definition.
        createBusinessObjectDefinitionEntity(BOD_NAMESPACE, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification without specifying any of the optional parameters (passing null values).
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(createBusinessObjectDataNotificationRegistrationCreateRequest(
                businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, null, null, null, null, null, null, Arrays
                    .asList(new JobAction(JOB_NAMESPACE, JOB_NAME, null))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME, null,
                null, null, null, null, null), Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME, null)), "ENABLED"),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationTrimParameters()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification using input parameters with leading and trailing empty spaces.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(createBusinessObjectDataNotificationRegistrationCreateRequest(new NotificationRegistrationKey(
                addWhitespace(NAMESPACE_CD), addWhitespace(NOTIFICATION_NAME)), addWhitespace(NOTIFICATION_EVENT_TYPE), addWhitespace(BOD_NAMESPACE),
                addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(STORAGE_NAME),
                addWhitespace(BDATA_STATUS), addWhitespace(BDATA_STATUS_2), Arrays.asList(new JobAction(addWhitespace(JOB_NAMESPACE), addWhitespace(JOB_NAME),
                    addWhitespace(CORRELATION_DATA))), BLANK_TEXT + "ENABLED" + BLANK_TEXT));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(), new NotificationRegistrationKey(
            NAMESPACE_CD, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), Arrays.asList(new JobAction(JOB_NAMESPACE, JOB_NAME,
                    addWhitespace(CORRELATION_DATA))), "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationUpperCaseParameters()
    {
        // Create and persist the relative database entities using lower case values.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD.toLowerCase(), Arrays.asList(NOTIFICATION_EVENT_TYPE
            .toLowerCase()), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), Arrays.asList(FORMAT_FILE_TYPE_CODE.toLowerCase()), Arrays.asList(STORAGE_NAME
                .toLowerCase()), Arrays.asList(BDATA_STATUS.toLowerCase(), BDATA_STATUS_2.toLowerCase()), Arrays.asList(new JobAction(NAMESPACE_CD
                    .toLowerCase(), JOB_NAME.toLowerCase(), BLANK_TEXT)));

        // Create a business object data notification using upper case input parameters.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(createBusinessObjectDataNotificationRegistrationCreateRequest(new NotificationRegistrationKey(
                NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toUpperCase()), NOTIFICATION_EVENT_TYPE.toUpperCase(), NAMESPACE_CD.toUpperCase(), BOD_NAME
                    .toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, STORAGE_NAME.toUpperCase(),
                BDATA_STATUS.toUpperCase(), BDATA_STATUS_2.toUpperCase(), Arrays.asList(new JobAction(NAMESPACE_CD.toUpperCase(), JOB_NAME.toUpperCase(),
                    CORRELATION_DATA.toUpperCase()))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(), new NotificationRegistrationKey(
            NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toUpperCase()), NOTIFICATION_EVENT_TYPE.toLowerCase(), new BusinessObjectDataNotificationFilter(
                NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION,
                STORAGE_NAME.toLowerCase(), BDATA_STATUS.toLowerCase(), BDATA_STATUS_2.toLowerCase()), Arrays.asList(new JobAction(NAMESPACE_CD.toLowerCase(),
                    JOB_NAME.toLowerCase(), CORRELATION_DATA.toUpperCase())), "ENABLED"),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationLowerCaseParameters()
    {
        // Create and persist the relative database entities using upper case values.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD.toUpperCase(), Arrays.asList(NOTIFICATION_EVENT_TYPE
            .toUpperCase()), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), Arrays.asList(FORMAT_FILE_TYPE_CODE.toUpperCase()), Arrays.asList(STORAGE_NAME
                .toUpperCase()), Arrays.asList(BDATA_STATUS.toUpperCase(), BDATA_STATUS_2.toUpperCase()), Arrays.asList(new JobAction(NAMESPACE_CD
                    .toUpperCase(), JOB_NAME.toUpperCase(), BLANK_TEXT)));

        // Create a business object data notification using lower case input parameters.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(createBusinessObjectDataNotificationRegistrationCreateRequest(new NotificationRegistrationKey(
                NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase()), NOTIFICATION_EVENT_TYPE.toLowerCase(), NAMESPACE_CD.toLowerCase(), BOD_NAME
                    .toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, STORAGE_NAME.toLowerCase(),
                BDATA_STATUS.toLowerCase(), BDATA_STATUS_2.toLowerCase(), Arrays.asList(new JobAction(NAMESPACE_CD.toLowerCase(), JOB_NAME.toLowerCase(),
                    CORRELATION_DATA.toLowerCase())), "enabled"));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(), new NotificationRegistrationKey(
            NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toLowerCase()), NOTIFICATION_EVENT_TYPE.toUpperCase(), new BusinessObjectDataNotificationFilter(
                NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION,
                STORAGE_NAME.toUpperCase(), BDATA_STATUS.toUpperCase(), BDATA_STATUS_2.toUpperCase()), Arrays.asList(new JobAction(NAMESPACE_CD.toUpperCase(),
                    JOB_NAME.toUpperCase(), CORRELATION_DATA.toLowerCase())), "ENABLED"),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationInvalidParameters()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        BusinessObjectDataNotificationRegistrationCreateRequest request;

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Try to create a business object data notification using non-existing namespace.
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(new NotificationRegistrationKey("I_DO_NOT_EXIST", NOTIFICATION_NAME),
            NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS,
            BDATA_STATUS_2, getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing namespace.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationRegistrationKey().getNamespace()), e
                .getMessage());
        }

        // Try to create a business object data notification using non-existing notification event type.
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, "I_DO_NOT_EXIST", BOD_NAMESPACE,
            BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing notification event type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Notification event type with code \"%s\" doesn't exist.", request.getBusinessObjectDataEventType()), e.getMessage());
        }

        // Try to create a business object data notification using non-existing business object definition name.
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            BOD_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
            getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object definition name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", request
                .getBusinessObjectDataNotificationFilter().getBusinessObjectDefinitionName(), request.getBusinessObjectDataNotificationFilter().getNamespace()),
                e.getMessage());
        }

        // Try to create a business object data notification using non-existing business object format file type.
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object format file type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("File type with code \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationFilter()
                .getBusinessObjectFormatFileType()), e.getMessage());
        }

        // Try to create a business object data notification using non-existing storage name.
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST", BDATA_STATUS, BDATA_STATUS_2,
            getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationFilter().getStorageName()), e
                .getMessage());
        }

        // Try to create a business object data notification using non-existing new business object data status.
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, "I_DO_NOT_EXIST", BDATA_STATUS_2,
            getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing new business object data status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data status \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationFilter()
                .getNewBusinessObjectDataStatus()), e.getMessage());
        }

        // Try to create a business object data notification using non-existing old business object data status.
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, "I_DO_NOT_EXIST",
            getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing old business object data status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data status \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationFilter()
                .getOldBusinessObjectDataStatus()), e.getMessage());
        }

        // Try to create a business object data notification when using new and old business object data statuses that are the same (case-insensitive).
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS.toUpperCase(), BDATA_STATUS
                .toLowerCase(), getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an IllegalArgumentException when using new and old business object data statuses that are the same");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The new business object data status is the same as the old one.", e.getMessage());
        }

        // Try to create a business object data notification for business object data
        // registration notification event type with an old business object data status specified.
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey,
            NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(), BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an IllegalArgumentException when old business object data status is specified "
                + "for a business object data registration notification event type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The old business object data status cannot be specified with a business object data registration event type.", e.getMessage());
        }

        // Try to create a business object data notification using non-existing job definition.
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
            BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, Arrays.asList(
                new JobAction(NAMESPACE_CD, "I_DO_NOT_EXIST", CORRELATION_DATA)));
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing job definition.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Job definition with namespace \"%s\" and job name \"%s\" doesn't exist.", request.getJobActions().get(0).getNamespace(),
                request.getJobActions().get(0).getJobName()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationDuplicateJobActions()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Try to create a business object data notification with duplicate job actions.
        BusinessObjectDataNotificationRegistrationCreateRequest request = createBusinessObjectDataNotificationRegistrationCreateRequest(
            new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, Arrays.asList(new JobAction(NAMESPACE_CD.toLowerCase(), JOB_NAME
                .toLowerCase(), CORRELATION_DATA), new JobAction(NAMESPACE_CD.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA)));
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an IllegalArgumentException when create request contains duplicate job actions.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate job action {namespace: \"%s\", jobName: \"%s\"} found.", request.getJobActions().get(1).getNamespace(),
                request.getJobActions().get(1).getJobName()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationAlreadyExists()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Try to create a business object data notification when it already exists.
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE,
                    NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                    getTestJobActions()));
            fail("Should throw an AlreadyExistsException when business object data notification already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create business object data notification with name \"%s\" because it already exists for namespace \"%s\".",
                businessObjectDataNotificationRegistrationKey.getNotificationName(), businessObjectDataNotificationRegistrationKey.getNamespace()), e
                    .getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD, Arrays.asList(NOTIFICATION_EVENT_TYPE,
            NOTIFICATION_EVENT_TYPE_2), BOD_NAMESPACE_2, BOD_NAME_2, Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME,
                STORAGE_NAME_2), Arrays.asList(BDATA_STATUS, BDATA_STATUS_2, BDATA_STATUS_3, BDATA_STATUS_4), getTestJobActions2());

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Update the business object data notification registration.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .updateBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME),
                createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2, BOD_NAMESPACE_2, BOD_NAME_2, FORMAT_USAGE_CODE_2,
                    FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS_3, BDATA_STATUS_4, getTestJobActions2(),
                    "ENABLED"));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE_2, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE_2, BOD_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS_3, BDATA_STATUS_4), getTestJobActions2(),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistrationMissingRequiredParameters()
    {
        // Try to update a business object data notification registration when namespace is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(BLANK_TEXT,
                NOTIFICATION_NAME), createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2, BOD_NAMESPACE_2, BOD_NAME_2,
                    FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions2(),
                    "ENABLED"));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to update a business object data notification registration when notification name is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD,
                BLANK_TEXT), createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2, BOD_NAMESPACE_2, BOD_NAME_2,
                    FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions2(),
                    "ENABLED"));
            fail("Should throw an IllegalArgumentException when notification name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A notification name must be specified.", e.getMessage());
        }

        // Try to update a business object data notification registration when notification status is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD,
                NOTIFICATION_NAME), createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2, BOD_NAMESPACE_2, BOD_NAME_2,
                    FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions2(), null));
            fail("Should throw an IllegalArgumentException when notification name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A notification registration status must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistrationMissingOptionalParametersPassedAsWhitespace()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create other database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD, Arrays.asList(NOTIFICATION_EVENT_TYPE,
            NOTIFICATION_EVENT_TYPE_2), BOD_NAMESPACE_2, BOD_NAME_2, null, null, Arrays.asList(BDATA_STATUS, BDATA_STATUS_2), getTestJobActions());

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Update the business object data notification without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2, BOD_NAMESPACE_2, BOD_NAME_2, BLANK_TEXT, BLANK_TEXT,
                    null, BLANK_TEXT, BLANK_TEXT, BLANK_TEXT, Arrays.asList(new JobAction(JOB_NAMESPACE_2, JOB_NAME_2, BLANK_TEXT)),
                    "ENABLED"));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE_2, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE_2, BOD_NAME_2,
                null, null, null, null, null, null), Arrays.asList(new JobAction(JOB_NAMESPACE_2, JOB_NAME_2, BLANK_TEXT)),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistrationMissingOptionalParametersPassedAsNulls()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create other database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD, Arrays.asList(NOTIFICATION_EVENT_TYPE,
            NOTIFICATION_EVENT_TYPE_2), BOD_NAMESPACE_2, BOD_NAME_2, null, null, Arrays.asList(BDATA_STATUS, BDATA_STATUS_2), getTestJobActions());

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Update the business object data notification without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2, BOD_NAMESPACE_2, BOD_NAME_2, null, null, null, null,
                    null, null, Arrays.asList(new JobAction(JOB_NAMESPACE_2, JOB_NAME_2, null)), "ENABLED"));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE_2, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE_2, BOD_NAME_2,
                null, null, null, null, null, null), Arrays.asList(new JobAction(JOB_NAMESPACE_2, JOB_NAME_2, null)),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistrationTrimParameters()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD, Arrays.asList(NOTIFICATION_EVENT_TYPE,
            NOTIFICATION_EVENT_TYPE_2), BOD_NAMESPACE_2, BOD_NAME_2, Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME,
                STORAGE_NAME_2), Arrays.asList(BDATA_STATUS, BDATA_STATUS_2, BDATA_STATUS_3, BDATA_STATUS_4), getTestJobActions2());

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Update the business object data notification using input parameters with leading and trailing empty spaces.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .updateBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(addWhitespace(NAMESPACE_CD), addWhitespace(NOTIFICATION_NAME)),
                createBusinessObjectDataNotificationRegistrationUpdateRequest(addWhitespace(NOTIFICATION_EVENT_TYPE_2), addWhitespace(BOD_NAMESPACE_2),
                    addWhitespace(BOD_NAME_2), addWhitespace(FORMAT_USAGE_CODE_2), addWhitespace(FORMAT_FILE_TYPE_CODE_2), FORMAT_VERSION_2, addWhitespace(
                        STORAGE_NAME_2), addWhitespace(BDATA_STATUS_3), addWhitespace(BDATA_STATUS_4), Arrays.asList(new JobAction(addWhitespace(
                            JOB_NAMESPACE_2), addWhitespace(JOB_NAME_2), addWhitespace(CORRELATION_DATA_2))), BLANK_TEXT + "ENABLED" + BLANK_TEXT));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE_2, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE_2, BOD_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS_3, BDATA_STATUS_4), Arrays.asList(new JobAction(
                    JOB_NAMESPACE_2, JOB_NAME_2, addWhitespace(CORRELATION_DATA_2))), "ENABLED"),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistrationUpperCaseParameters()
    {
        // Create database entities required for testing using lower case alternate key values.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD.toLowerCase(), Arrays.asList(NOTIFICATION_EVENT_TYPE
            .toLowerCase(), NOTIFICATION_EVENT_TYPE_2.toLowerCase()), BOD_NAMESPACE_2.toLowerCase(), BOD_NAME_2.toLowerCase(), Arrays.asList(
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE_2.toLowerCase()), Arrays.asList(STORAGE_NAME.toLowerCase(), STORAGE_NAME_2
                    .toLowerCase()), Arrays.asList(BDATA_STATUS.toLowerCase(), BDATA_STATUS_2.toLowerCase(), BDATA_STATUS_3.toLowerCase(), BDATA_STATUS_4
                        .toLowerCase()), Arrays.asList(new JobAction(JOB_NAMESPACE_2.toLowerCase(), JOB_NAME_2.toLowerCase(), null)));

        // Create and persist a business object data notification registration entity using lower case alternate key values.
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD.toLowerCase(),
            NOTIFICATION_NAME.toLowerCase());
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE.toLowerCase(),
            BOD_NAMESPACE.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION,
            STORAGE_NAME.toLowerCase(), BDATA_STATUS.toLowerCase(), BDATA_STATUS_2.toLowerCase(), Arrays.asList(new JobAction(JOB_NAMESPACE.toLowerCase(),
                JOB_NAME.toLowerCase(), CORRELATION_DATA)));

        // Update the business object data notification using upper case input parameters.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .updateBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toUpperCase()),
                createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2.toUpperCase(), BOD_NAMESPACE_2.toUpperCase(), BOD_NAME_2
                    .toUpperCase(), FORMAT_USAGE_CODE_2.toUpperCase(), FORMAT_FILE_TYPE_CODE_2.toUpperCase(), FORMAT_VERSION_2, STORAGE_NAME_2.toUpperCase(),
                    BDATA_STATUS_3.toUpperCase(), BDATA_STATUS_4.toUpperCase(), Arrays.asList(new JobAction(JOB_NAMESPACE_2.toUpperCase(), JOB_NAME_2
                        .toUpperCase(), CORRELATION_DATA_2.toUpperCase())), "ENABLED"));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(), new NotificationRegistrationKey(
            NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase()), NOTIFICATION_EVENT_TYPE_2.toLowerCase(), new BusinessObjectDataNotificationFilter(
                BOD_NAMESPACE_2.toLowerCase(), BOD_NAME_2.toLowerCase(), FORMAT_USAGE_CODE_2.toUpperCase(), FORMAT_FILE_TYPE_CODE_2.toLowerCase(),
                FORMAT_VERSION_2, STORAGE_NAME_2.toLowerCase(), BDATA_STATUS_3.toLowerCase(), BDATA_STATUS_4.toLowerCase()), Arrays.asList(new JobAction(
                    JOB_NAMESPACE_2.toLowerCase(), JOB_NAME_2.toLowerCase(), CORRELATION_DATA_2.toUpperCase())), "ENABLED"),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistrationLowerCaseParameters()
    {
        // Create database entities required for testing using upper case alternate key values.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD.toUpperCase(), Arrays.asList(NOTIFICATION_EVENT_TYPE
            .toUpperCase(), NOTIFICATION_EVENT_TYPE_2.toUpperCase()), BOD_NAMESPACE_2.toUpperCase(), BOD_NAME_2.toUpperCase(), Arrays.asList(
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE_2.toUpperCase()), Arrays.asList(STORAGE_NAME.toUpperCase(), STORAGE_NAME_2
                    .toUpperCase()), Arrays.asList(BDATA_STATUS.toUpperCase(), BDATA_STATUS_2.toUpperCase(), BDATA_STATUS_3.toUpperCase(), BDATA_STATUS_4
                        .toUpperCase()), Arrays.asList(new JobAction(JOB_NAMESPACE_2.toUpperCase(), JOB_NAME_2.toUpperCase(), null)));

        // Create and persist a business object data notification registration entity using upper case alternate key values.
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD.toUpperCase(),
            NOTIFICATION_NAME.toUpperCase());
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE.toUpperCase(),
            BOD_NAMESPACE.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION,
            STORAGE_NAME.toUpperCase(), BDATA_STATUS.toUpperCase(), BDATA_STATUS_2.toUpperCase(), Arrays.asList(new JobAction(JOB_NAMESPACE.toUpperCase(),
                JOB_NAME.toUpperCase(), CORRELATION_DATA)));

        // Update the business object data notification using lower case input parameters.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .updateBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase()),
                createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2.toLowerCase(), BOD_NAMESPACE_2.toLowerCase(), BOD_NAME_2
                    .toLowerCase(), FORMAT_USAGE_CODE_2.toLowerCase(), FORMAT_FILE_TYPE_CODE_2.toLowerCase(), FORMAT_VERSION_2, STORAGE_NAME_2.toLowerCase(),
                    BDATA_STATUS_3.toLowerCase(), BDATA_STATUS_4.toLowerCase(), Arrays.asList(new JobAction(JOB_NAMESPACE_2.toLowerCase(), JOB_NAME_2
                        .toLowerCase(), CORRELATION_DATA_2.toLowerCase())), "enabled"));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE_2.toUpperCase(), new BusinessObjectDataNotificationFilter(BOD_NAMESPACE_2
                .toUpperCase(), BOD_NAME_2.toUpperCase(), FORMAT_USAGE_CODE_2.toLowerCase(), FORMAT_FILE_TYPE_CODE_2.toUpperCase(), FORMAT_VERSION_2,
                STORAGE_NAME_2.toUpperCase(), BDATA_STATUS_3.toUpperCase(), BDATA_STATUS_4.toUpperCase()), Arrays.asList(new JobAction(JOB_NAMESPACE_2
                    .toUpperCase(), JOB_NAME_2.toUpperCase(), CORRELATION_DATA_2.toLowerCase())), "ENABLED"),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistrationInvalidParameters()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        BusinessObjectDataNotificationRegistrationUpdateRequest request;

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD, Arrays.asList(NOTIFICATION_EVENT_TYPE,
            NOTIFICATION_EVENT_TYPE_2), BOD_NAMESPACE_2, BOD_NAME_2, Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME,
                STORAGE_NAME_2), Arrays.asList(BDATA_STATUS, BDATA_STATUS_2, BDATA_STATUS_3, BDATA_STATUS_4), getTestJobActions2());

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Try to update a business object data notification using non-existing notification event type.
        request = createBusinessObjectDataNotificationRegistrationUpdateRequest("I_DO_NOT_EXIST", NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions(), "ENABLED");
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                request);
            fail("Should throw an ObjectNotFoundException when using non-existing notification event type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Notification event type with code \"%s\" doesn't exist.", request.getBusinessObjectDataEventType()), e.getMessage());
        }

        // Try to update a business object data notification using non-existing business object definition name.
        request = createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions(), "ENABLED");
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object definition name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", request
                .getBusinessObjectDataNotificationFilter().getBusinessObjectDefinitionName(), request.getBusinessObjectDataNotificationFilter().getNamespace()),
                e.getMessage());
        }

        // Try to update a business object data notification using non-existing business object format file type.
        request = createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            "I_DO_NOT_EXIST", FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions(), "ENABLED");
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object format file type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("File type with code \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationFilter()
                .getBusinessObjectFormatFileType()), e.getMessage());
        }

        // Try to update a business object data notification using non-existing storage name.
        request = createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST", BDATA_STATUS, BDATA_STATUS_2, getTestJobActions(),
            "ENABLED");
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationFilter().getStorageName()), e
                .getMessage());
        }

        // Try to update a business object data notification using non-existing new business object data status.
        request = createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, "I_DO_NOT_EXIST", BDATA_STATUS_2, getTestJobActions(),
            "ENABLED");
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                request);
            fail("Should throw an ObjectNotFoundException when using non-existing new business object data status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data status \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationFilter()
                .getNewBusinessObjectDataStatus()), e.getMessage());
        }

        // Try to update a business object data notification using non-existing old business object data status.
        request = createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, "I_DO_NOT_EXIST", getTestJobActions(),
            "ENABLED");
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                request);
            fail("Should throw an ObjectNotFoundException when using non-existing old business object data status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data status \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationFilter()
                .getOldBusinessObjectDataStatus()), e.getMessage());
        }

        // Try to create a business object data notification when using new and old business object data statuses that are the same (case-insensitive).
        request = createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS.toUpperCase(), BDATA_STATUS.toLowerCase(), getTestJobActions(),
            "ENABLED");
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                request);
            fail("Should throw an IllegalArgumentException when using new and old business object data statuses that are the same");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The new business object data status is the same as the old one.", e.getMessage());
        }

        // Try to create a business object data notification for business object data
        // registration notification event type with an old business object data status specified.
        request = createBusinessObjectDataNotificationRegistrationUpdateRequest(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name(),
            BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions(),
            "ENABLED");
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                request);
            fail("Should throw an IllegalArgumentException when old business object data status is specified "
                + "for a business object data registration notification event type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The old business object data status cannot be specified with a business object data registration event type.", e.getMessage());
        }

        // Try to update a business object data notification registration using non-existing job definition.
        request = createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, Arrays.asList(new JobAction(NAMESPACE_CD, "I_DO_NOT_EXIST",
                CORRELATION_DATA)), "ENABLED");
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey,
                request);
            fail("Should throw an ObjectNotFoundException when using non-existing job definition.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Job definition with namespace \"%s\" and job name \"%s\" doesn't exist.", request.getJobActions().get(0).getNamespace(),
                request.getJobActions().get(0).getJobName()), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistrationNoExists()
    {
        // Try to update a non-existing business object data notification registration.
        try
        {
            businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD,
                NOTIFICATION_NAME), createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2, BOD_NAMESPACE_2, BOD_NAME_2,
                    FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions2(),
                    "ENABLED"));
            fail("Should throw an ObjectNotFoundException when trying to update a non-existing business object data notification.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data notification registration with name \"%s\" does not exist for \"%s\" namespace.",
                NOTIFICATION_NAME, NAMESPACE_CD), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Retrieve the business object data notification registration.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey);

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationMissingRequiredParameters()
    {
        // Try to get a business object data notification registration when namespace is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(BLANK_TEXT,
                NOTIFICATION_NAME));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to get a business object data notification when notification name is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD,
                BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when notification name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A notification name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationTrimParameters()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Retrieve the business object data notification using input parameters with leading and trailing empty spaces.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(addWhitespace(NAMESPACE_CD), addWhitespace(NOTIFICATION_NAME)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationUpperCaseParameters()
    {
        // Create and persist a business object data notification registration entity using lower case alternate key values.
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD.toLowerCase(),
            NOTIFICATION_NAME.toLowerCase());
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Retrieve the business object data notification using upper case input parameters.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationLowerCaseParameters()
    {
        // Create and persist a business object data notification registration entity using upper case alternate key values.
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD.toUpperCase(),
            NOTIFICATION_NAME.toUpperCase());
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Retrieve the business object data notification using lower case input parameters.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationNoExists()
    {
        // Try to retrieve a non-existing business object data notification.
        try
        {
            businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD,
                NOTIFICATION_NAME));
            fail("Should throw an ObjectNotFoundException when trying to retrieve a non-existing business object data notification.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data notification registration with name \"%s\" does not exist for \"%s\" namespace.",
                NOTIFICATION_NAME, NAMESPACE_CD), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrations()
    {
        // Create and persist business object data notification entities.
        for (NotificationRegistrationKey businessObjectDataNotificationRegistrationKey : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, null, null, getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys resultKeys = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDataNotificationRegistrationKeys(), resultKeys.getBusinessObjectDataNotificationRegistrationKeys());
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsMissingRequiredParameters()
    {
        // Try to get business object data notifications when namespace is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistrations(BLANK_TEXT);
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsTrimParameters()
    {
        // Create and persist business object data notification entities.
        for (NotificationRegistrationKey businessObjectDataNotificationRegistrationKey : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys using input parameters with leading and trailing empty spaces.
        BusinessObjectDataNotificationRegistrationKeys resultKeys = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistrations(addWhitespace(NAMESPACE_CD));

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDataNotificationRegistrationKeys(), resultKeys.getBusinessObjectDataNotificationRegistrationKeys());
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsUpperCaseParameters()
    {
        // Create and persist business object data notification entities using lower case alternate key values.
        for (NotificationRegistrationKey businessObjectDataNotificationRegistrationKey : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            NotificationRegistrationKey businessObjectDataNotificationRegistrationKeyLowerCase = new NotificationRegistrationKey(
                businessObjectDataNotificationRegistrationKey.getNamespace().toLowerCase(), businessObjectDataNotificationRegistrationKey.getNotificationName()
                    .toLowerCase());
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKeyLowerCase, NOTIFICATION_EVENT_TYPE,
                BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys using upper case namespace code value.
        BusinessObjectDataNotificationRegistrationKeys resultKeys = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD.toUpperCase());

        // Validate the returned object.
        List<NotificationRegistrationKey> expectedKeys = new ArrayList<>();
        for (NotificationRegistrationKey origKey : getExpectedBusinessObjectDataNotificationRegistrationKeys())
        {
            NotificationRegistrationKey expectedKey = new NotificationRegistrationKey();
            expectedKeys.add(expectedKey);
            expectedKey.setNamespace(origKey.getNamespace().toLowerCase());
            expectedKey.setNotificationName(origKey.getNotificationName().toLowerCase());
        }
        assertEquals(expectedKeys, resultKeys.getBusinessObjectDataNotificationRegistrationKeys());
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsLowerCaseParameters()
    {
        // Create and persist business object data notification entities using upper case alternate key values.
        for (NotificationRegistrationKey businessObjectDataNotificationRegistrationKey : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            NotificationRegistrationKey businessObjectDataNotificationRegistrationKeyUpperCase = new NotificationRegistrationKey(
                businessObjectDataNotificationRegistrationKey.getNamespace().toUpperCase(), businessObjectDataNotificationRegistrationKey.getNotificationName()
                    .toUpperCase());
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKeyUpperCase, NOTIFICATION_EVENT_TYPE,
                BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2,
                getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys using lower case namespace code value.
        BusinessObjectDataNotificationRegistrationKeys resultKeys = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD.toLowerCase());

        // Validate the returned object.
        List<NotificationRegistrationKey> expectedKeys = new ArrayList<>();
        for (NotificationRegistrationKey origKey : getExpectedBusinessObjectDataNotificationRegistrationKeys())
        {
            NotificationRegistrationKey expectedKey = new NotificationRegistrationKey();
            expectedKeys.add(expectedKey);
            expectedKey.setNamespace(origKey.getNamespace().toUpperCase());
            expectedKey.setNotificationName(origKey.getNotificationName().toUpperCase());
        }
        assertEquals(expectedKeys, resultKeys.getBusinessObjectDataNotificationRegistrationKeys());
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsNamespaceNoExists()
    {
        // Try to retrieve business object data notifications for a non-existing namespace.
        try
        {
            businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD);
            fail("Should throw an ObjectNotFoundException when using non-existing namespace.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", NAMESPACE_CD), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsBusinessObjectDataNotificationsNoExist()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Retrieve an empty list of business object data notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys resultKeys = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD);

        // Validate the returned object.
        assertEquals(new ArrayList<NotificationRegistrationKey>(), resultKeys.getBusinessObjectDataNotificationRegistrationKeys());
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistration()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Validate that this business object data notification exists.
        assertNotNull(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(
            businessObjectDataNotificationRegistrationKey));

        // Delete this business object data notification.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .deleteBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationKey);

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), new NotificationRegistrationKey(
            NAMESPACE_CD, NOTIFICATION_NAME), NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(
            businessObjectDataNotificationRegistrationKey));
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistrationMissingRequiredParameters()
    {
        // Try to delete a business object data notification when namespace is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.deleteBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(BLANK_TEXT,
                NOTIFICATION_NAME));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to delete a business object data notification when notification name is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.deleteBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD,
                BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when notification name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A notification name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistrationTrimParameters()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Validate that this business object data notification exists.
        assertNotNull(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(
            businessObjectDataNotificationRegistrationKey));

        // Delete this business object data notification using input parameters with leading and trailing empty spaces.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .deleteBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(addWhitespace(NAMESPACE_CD), addWhitespace(NOTIFICATION_NAME)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(
            businessObjectDataNotificationRegistrationKey));
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistrationUpperCaseParameters()
    {
        // Create and persist a business object data notification registration entity using lower case alternate key values.
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD.toLowerCase(),
            NOTIFICATION_NAME.toLowerCase());
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Validate that this business object data notification exists.
        assertNotNull(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(
            businessObjectDataNotificationRegistrationKey));

        // Delete this business object data notification using upper case input parameters.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .deleteBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(
            businessObjectDataNotificationRegistrationKey));
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistrationLowerCaseParameters()
    {
        // Create and persist a business object data notification registration entity using upper case alternate key values.
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD.toUpperCase(),
            NOTIFICATION_NAME.toUpperCase());
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Validate that this business object data notification exists.
        assertNotNull(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(
            businessObjectDataNotificationRegistrationKey));

        // Delete this business object data notification using lower case input parameters.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .deleteBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "ENABLED"), deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(businessObjectDataNotificationRegistrationDao.getBusinessObjectDataNotificationRegistrationByAltKey(
            businessObjectDataNotificationRegistrationKey));
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistrationBusinessObjectDataNotificationNoExists()
    {
        // Try to delete a non-existing business object data notification.
        try
        {
            businessObjectDataNotificationRegistrationService.deleteBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD,
                NOTIFICATION_NAME));
            fail("Should throw an ObjectNotFoundException when trying to delete a non-existing business object data notification.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data notification registration with name \"%s\" does not exist for \"%s\" namespace.",
                NOTIFICATION_NAME, NAMESPACE_CD), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationWithStatus()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(createBusinessObjectDataNotificationRegistrationCreateRequest(
                businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions(), "DISABLED"));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2), getTestJobActions(),
            "DISABLED"), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testUpdateBusinessObjectDataNotificationRegistrationWithStatus()
    {
        NotificationRegistrationKey businessObjectDataNotificationRegistrationKey = new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);

        // Create database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD, Arrays.asList(NOTIFICATION_EVENT_TYPE,
            NOTIFICATION_EVENT_TYPE_2), BOD_NAMESPACE_2, BOD_NAME_2, Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2), Arrays.asList(STORAGE_NAME,
                STORAGE_NAME_2), Arrays.asList(BDATA_STATUS, BDATA_STATUS_2, BDATA_STATUS_3, BDATA_STATUS_4), getTestJobActions2());

        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE, BOD_NAMESPACE, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, BDATA_STATUS, BDATA_STATUS_2, getTestJobActions());

        // Update the business object data notification registration.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .updateBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME),
                createBusinessObjectDataNotificationRegistrationUpdateRequest(NOTIFICATION_EVENT_TYPE_2, BOD_NAMESPACE_2, BOD_NAME_2, FORMAT_USAGE_CODE_2,
                    FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS_3, BDATA_STATUS_4, getTestJobActions2(),
                    "DISABLED"));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataNotificationRegistration(resultBusinessObjectDataNotificationRegistration.getId(),
            businessObjectDataNotificationRegistrationKey, NOTIFICATION_EVENT_TYPE_2, new BusinessObjectDataNotificationFilter(BOD_NAMESPACE_2, BOD_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, STORAGE_NAME_2, BDATA_STATUS_3, BDATA_STATUS_4), getTestJobActions2(),
            "DISABLED"), resultBusinessObjectDataNotificationRegistration);
    }

}
