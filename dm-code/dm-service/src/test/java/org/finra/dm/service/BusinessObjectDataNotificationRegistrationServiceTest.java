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
package org.finra.dm.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.dm.model.AlreadyExistsException;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistration;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKey;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKeys;
import org.finra.dm.model.api.xml.JobAction;

/**
 * This class tests various functionality within the business object data notification registration REST controller.
 */
public class BusinessObjectDataNotificationRegistrationServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateBusinessObjectDataNotificationRegistration()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions()));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(null, NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions(), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationLegacy()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, null, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions()));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(null, NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions(), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationMissingRequiredParameters()
    {
        // Try to create a business object data notification instance when namespace is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(BLANK_TEXT, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions()));
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
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, BLANK_TEXT, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions()));
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
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, BLANK_TEXT, NAMESPACE_CD, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions()));
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
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD,
                    BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions()));
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
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, null));
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
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                    Arrays.asList(new JobAction(BLANK_TEXT, JOB_NAME, CORRELATION_DATA))));
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
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                    Arrays.asList(new JobAction(NAMESPACE_CD, BLANK_TEXT, CORRELATION_DATA))));
            fail("Should throw an IllegalArgumentException when job action job name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A job action job name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationMissingOptionalParameters()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BLANK_TEXT, BOD_NAME,
                    BLANK_TEXT, BLANK_TEXT, null, BLANK_TEXT, Arrays.asList(new JobAction(NAMESPACE_CD, JOB_NAME, BLANK_TEXT))));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(null, NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, null, null,
            null, null, Arrays.asList(new JobAction(NAMESPACE_CD, JOB_NAME, BLANK_TEXT)), resultBusinessObjectDataNotificationRegistration);

        // Create a business object data notification without specifying any of the optional parameters (passing null values).
        resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
            createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME_2, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                null, null, null, null, Arrays.asList(new JobAction(NAMESPACE_CD, JOB_NAME, null))));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(null, NAMESPACE_CD, NOTIFICATION_NAME_2, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, null, null,
            null, null, Arrays.asList(new JobAction(NAMESPACE_CD, JOB_NAME, null)), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationTrimParameters()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Create a business object data notification using input parameters with leading and trailing empty spaces.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(addWhitespace(NAMESPACE_CD), addWhitespace(NOTIFICATION_NAME),
                    addWhitespace(NOTIFICATION_EVENT_TYPE), addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                    addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(STORAGE_NAME),
                    Arrays.asList(new JobAction(addWhitespace(NAMESPACE_CD), addWhitespace(JOB_NAME), addWhitespace(CORRELATION_DATA)))));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(null, NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
            Arrays.asList(new JobAction(NAMESPACE_CD, JOB_NAME, addWhitespace(CORRELATION_DATA))), resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationUpperCaseParameters()
    {
        // Create and persist the relative database entities using lower case values.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD.toLowerCase(), NOTIFICATION_EVENT_TYPE.toLowerCase(),
            NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(), STORAGE_NAME.toLowerCase(),
            Arrays.asList(new JobAction(NAMESPACE_CD.toLowerCase(), JOB_NAME.toLowerCase(), BLANK_TEXT)));

        // Create a business object data notification using upper case input parameters.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toUpperCase(),
                    NOTIFICATION_EVENT_TYPE.toUpperCase(), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                    FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, STORAGE_NAME.toUpperCase(),
                    Arrays.asList(new JobAction(NAMESPACE_CD.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA.toUpperCase()))));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(null, NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toUpperCase(),
            NOTIFICATION_EVENT_TYPE.toLowerCase(), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, STORAGE_NAME.toLowerCase(),
            Arrays.asList(new JobAction(NAMESPACE_CD.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA.toUpperCase())),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationLowerCaseParameters()
    {
        // Create and persist the relative database entities using upper case values.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting(NAMESPACE_CD.toUpperCase(), NOTIFICATION_EVENT_TYPE.toUpperCase(),
            NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(), STORAGE_NAME.toUpperCase(),
            Arrays.asList(new JobAction(NAMESPACE_CD.toUpperCase(), JOB_NAME.toUpperCase(), BLANK_TEXT)));

        // Create a business object data notification using lower case input parameters.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase(),
                    NOTIFICATION_EVENT_TYPE.toLowerCase(), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                    FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, STORAGE_NAME.toLowerCase(),
                    Arrays.asList(new JobAction(NAMESPACE_CD.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA.toLowerCase()))));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(null, NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toLowerCase(),
            NOTIFICATION_EVENT_TYPE.toUpperCase(), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, STORAGE_NAME.toUpperCase(),
            Arrays.asList(new JobAction(NAMESPACE_CD.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA.toLowerCase())),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testCreateBusinessObjectDataNotificationRegistrationInvalidParameters()
    {
        BusinessObjectDataNotificationRegistrationCreateRequest request;

        // Create and persist the relative database entities.
        createDatabaseEntitiesForBusinessObjectDataNotificationRegistrationTesting();

        // Try to create a business object data notification using non-existing namespace.
        request =
            createBusinessObjectDataNotificationRegistrationCreateRequest("I_DO_NOT_EXIST", NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing namespace.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationRegistrationKey().getNamespace()),
                e.getMessage());
        }

        // Try to create a business object data notification using non-existing notification event type.
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, "I_DO_NOT_EXIST", NAMESPACE_CD, BOD_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());
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
        request = createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD,
            "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object definition name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                request.getBusinessObjectDataNotificationFilter().getBusinessObjectDefinitionName(),
                request.getBusinessObjectDataNotificationFilter().getNamespace()), e.getMessage());
        }

        // Try to create a business object data notification using non-existing business object format file type.
        request =
            createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, STORAGE_NAME, getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object format file type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("File type with code \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationFilter().getBusinessObjectFormatFileType()),
                e.getMessage());
        }

        // Try to create a business object data notification using non-existing storage name.
        request =
            createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST", getTestJobActions());
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getBusinessObjectDataNotificationFilter().getStorageName()),
                e.getMessage());
        }

        // Try to create a business object data notification using non-existing job definition.
        request =
            createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                Arrays.asList(new JobAction(NAMESPACE_CD, "I_DO_NOT_EXIST", CORRELATION_DATA)));
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
        BusinessObjectDataNotificationRegistrationCreateRequest request =
            createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, Arrays
                .asList(new JobAction(NAMESPACE_CD.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA),
                    new JobAction(NAMESPACE_CD.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA)));
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
        // Create and persist a business object data notification registration entity.
        createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());

        // Try to create a business object data notification when it already exists.
        try
        {
            businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(
                createBusinessObjectDataNotificationRegistrationCreateRequest(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions()));
            fail("Should throw an AlreadyExistsException when business object data notification already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create business object data notification with name \"%s\" because it already exists for namespace \"%s\".",
                NOTIFICATION_NAME, NAMESPACE_CD), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistration()
    {
        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());

        // Retrieve the business object data notification.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistration(new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), NAMESPACE_CD, NOTIFICATION_NAME,
            NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions(),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationMissingRequiredParameters()
    {
        // Try to get a business object data notification when namespace is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService
                .getBusinessObjectDataNotificationRegistration(new BusinessObjectDataNotificationRegistrationKey(BLANK_TEXT, NOTIFICATION_NAME));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to get a business object data notification when notification name is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService
                .getBusinessObjectDataNotificationRegistration(new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD, BLANK_TEXT));
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
        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());

        // Retrieve the business object data notification using input parameters with leading and trailing empty spaces.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistration(
                new BusinessObjectDataNotificationRegistrationKey(addWhitespace(NAMESPACE_CD), addWhitespace(NOTIFICATION_NAME)));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), NAMESPACE_CD, NOTIFICATION_NAME,
            NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions(),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationUpperCaseParameters()
    {
        // Create and persist a business object data notification registration entity using lower case alternate key values.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase(), NOTIFICATION_EVENT_TYPE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                Arrays.asList(new JobAction(NAMESPACE_CD.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA)));

        // Retrieve the business object data notification using upper case input parameters.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistration(
                new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toUpperCase()));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), NAMESPACE_CD.toLowerCase(),
            NOTIFICATION_NAME.toLowerCase(), NOTIFICATION_EVENT_TYPE, NAMESPACE_CD.toLowerCase(), BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, Arrays.asList(new JobAction(NAMESPACE_CD.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA)),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationLowerCaseParameters()
    {
        // Create and persist a business object data notification registration entity using upper case alternate key values.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toUpperCase(), NOTIFICATION_EVENT_TYPE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                Arrays.asList(new JobAction(NAMESPACE_CD.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA)));

        // Retrieve the business object data notification using lower case input parameters.
        BusinessObjectDataNotificationRegistration resultBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistration(
                new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase()));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), NAMESPACE_CD.toUpperCase(),
            NOTIFICATION_NAME.toUpperCase(), NOTIFICATION_EVENT_TYPE, NAMESPACE_CD.toUpperCase(), BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, Arrays.asList(new JobAction(NAMESPACE_CD.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA)),
            resultBusinessObjectDataNotificationRegistration);
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationNoExists()
    {
        // Try to retrieve a non-existing business object data notification.
        try
        {
            businessObjectDataNotificationRegistrationService
                .getBusinessObjectDataNotificationRegistration(new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME));
            fail("Should throw an ObjectNotFoundException when trying to retrieve a non-existing business object data notification.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                .format("Business object data notification registration with name \"%s\" does not exist for \"%s\" namespace.", NOTIFICATION_NAME,
                    NAMESPACE_CD), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrations()
    {
        // Create and persist business object data notification entities.
        for (BusinessObjectDataNotificationRegistrationKey key : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            createBusinessObjectDataNotificationRegistrationEntity(key.getNamespace(), key.getNotificationName(), NOTIFICATION_EVENT_TYPE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys.
        BusinessObjectDataNotificationRegistrationKeys resultKeys =
            businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD);

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
        for (BusinessObjectDataNotificationRegistrationKey key : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            createBusinessObjectDataNotificationRegistrationEntity(key.getNamespace(), key.getNotificationName(), NOTIFICATION_EVENT_TYPE, BOD_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys using input parameters with leading and trailing empty spaces.
        BusinessObjectDataNotificationRegistrationKeys resultKeys =
            businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistrations(addWhitespace(NAMESPACE_CD));

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDataNotificationRegistrationKeys(), resultKeys.getBusinessObjectDataNotificationRegistrationKeys());
    }

    @Test
    public void testGetBusinessObjectDataNotificationRegistrationsUpperCaseParameters()
    {
        // Create and persist business object data notification entities using lower case alternate key values.
        for (BusinessObjectDataNotificationRegistrationKey key : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            createBusinessObjectDataNotificationRegistrationEntity(key.getNamespace().toLowerCase(), key.getNotificationName().toLowerCase(),
                NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys using upper case namespace code value.
        BusinessObjectDataNotificationRegistrationKeys resultKeys =
            businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD.toUpperCase());

        // Validate the returned object.
        List<BusinessObjectDataNotificationRegistrationKey> expectedKeys = new ArrayList<>();
        for (BusinessObjectDataNotificationRegistrationKey origKey : getExpectedBusinessObjectDataNotificationRegistrationKeys())
        {
            BusinessObjectDataNotificationRegistrationKey expectedKey = new BusinessObjectDataNotificationRegistrationKey();
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
        for (BusinessObjectDataNotificationRegistrationKey key : getTestBusinessObjectDataNotificationRegistrationKeys())
        {
            createBusinessObjectDataNotificationRegistrationEntity(key.getNamespace().toUpperCase(), key.getNotificationName().toUpperCase(),
                NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());
        }

        // Retrieve a list of business object data notification registration keys using lower case namespace code value.
        BusinessObjectDataNotificationRegistrationKeys resultKeys =
            businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD.toLowerCase());

        // Validate the returned object.
        List<BusinessObjectDataNotificationRegistrationKey> expectedKeys = new ArrayList<>();
        for (BusinessObjectDataNotificationRegistrationKey origKey : getExpectedBusinessObjectDataNotificationRegistrationKeys())
        {
            BusinessObjectDataNotificationRegistrationKey expectedKey = new BusinessObjectDataNotificationRegistrationKey();
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
        BusinessObjectDataNotificationRegistrationKeys resultKeys =
            businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistrations(NAMESPACE_CD);

        // Validate the returned object.
        assertEquals(new ArrayList<BusinessObjectDataNotificationRegistrationKey>(), resultKeys.getBusinessObjectDataNotificationRegistrationKeys());
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistration()
    {
        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());

        // Validate that this business object data notification exists.
        BusinessObjectDataNotificationRegistrationKey businessObjectDataNotificationKey =
            new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);
        assertNotNull(dmDao.getBusinessObjectDataNotificationByAltKey(businessObjectDataNotificationKey));

        // Delete this business object data notification.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .deleteBusinessObjectDataNotificationRegistration(new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), NAMESPACE_CD, NOTIFICATION_NAME,
            NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions(),
            deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(dmDao.getBusinessObjectDataNotificationByAltKey(businessObjectDataNotificationKey));
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistrationMissingRequiredParameters()
    {
        // Try to delete a business object data notification when namespace is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService
                .deleteBusinessObjectDataNotificationRegistration(new BusinessObjectDataNotificationRegistrationKey(BLANK_TEXT, NOTIFICATION_NAME));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to delete a business object data notification when notification name is not specified.
        try
        {
            businessObjectDataNotificationRegistrationService
                .deleteBusinessObjectDataNotificationRegistration(new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD, BLANK_TEXT));
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
        // Create and persist a business object data notification registration entity.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD, NOTIFICATION_NAME, NOTIFICATION_EVENT_TYPE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions());

        // Validate that this business object data notification exists.
        BusinessObjectDataNotificationRegistrationKey businessObjectDataNotificationKey =
            new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME);
        assertNotNull(dmDao.getBusinessObjectDataNotificationByAltKey(businessObjectDataNotificationKey));

        // Delete this business object data notification using input parameters with leading and trailing empty spaces.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .deleteBusinessObjectDataNotificationRegistration(
                new BusinessObjectDataNotificationRegistrationKey(addWhitespace(NAMESPACE_CD), addWhitespace(NOTIFICATION_NAME)));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), NAMESPACE_CD, NOTIFICATION_NAME,
            NOTIFICATION_EVENT_TYPE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME, getTestJobActions(),
            deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(dmDao.getBusinessObjectDataNotificationByAltKey(businessObjectDataNotificationKey));
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistrationUpperCaseParameters()
    {
        // Create and persist a business object data notification registration entity using lower case alternate key values.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase(), NOTIFICATION_EVENT_TYPE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                Arrays.asList(new JobAction(NAMESPACE_CD.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA)));

        // Validate that this business object data notification exists.
        BusinessObjectDataNotificationRegistrationKey businessObjectDataNotificationKey =
            new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase());
        assertNotNull(dmDao.getBusinessObjectDataNotificationByAltKey(businessObjectDataNotificationKey));

        // Delete this business object data notification using upper case input parameters.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .deleteBusinessObjectDataNotificationRegistration(
                new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toUpperCase()));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), NAMESPACE_CD.toLowerCase(),
            NOTIFICATION_NAME.toLowerCase(), NOTIFICATION_EVENT_TYPE, NAMESPACE_CD.toLowerCase(), BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, Arrays.asList(new JobAction(NAMESPACE_CD.toLowerCase(), JOB_NAME.toLowerCase(), CORRELATION_DATA)),
            deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(dmDao.getBusinessObjectDataNotificationByAltKey(businessObjectDataNotificationKey));
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistrationLowerCaseParameters()
    {
        // Create and persist a business object data notification registration entity using upper case alternate key values.
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            createBusinessObjectDataNotificationRegistrationEntity(NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toUpperCase(), NOTIFICATION_EVENT_TYPE,
                BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, STORAGE_NAME,
                Arrays.asList(new JobAction(NAMESPACE_CD.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA)));

        // Validate that this business object data notification exists.
        BusinessObjectDataNotificationRegistrationKey businessObjectDataNotificationKey =
            new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD.toUpperCase(), NOTIFICATION_NAME.toUpperCase());
        assertNotNull(dmDao.getBusinessObjectDataNotificationByAltKey(businessObjectDataNotificationKey));

        // Delete this business object data notification using lower case input parameters.
        BusinessObjectDataNotificationRegistration deletedBusinessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationService
            .deleteBusinessObjectDataNotificationRegistration(
                new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD.toLowerCase(), NOTIFICATION_NAME.toLowerCase()));

        // Validate the returned object.
        validateBusinessObjectDataNotificationRegistration(businessObjectDataNotificationRegistrationEntity.getId(), NAMESPACE_CD.toUpperCase(),
            NOTIFICATION_NAME.toUpperCase(), NOTIFICATION_EVENT_TYPE, NAMESPACE_CD.toUpperCase(), BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, STORAGE_NAME, Arrays.asList(new JobAction(NAMESPACE_CD.toUpperCase(), JOB_NAME.toUpperCase(), CORRELATION_DATA)),
            deletedBusinessObjectDataNotificationRegistration);

        // Ensure that this business object data notification is no longer there.
        assertNull(dmDao.getBusinessObjectDataNotificationByAltKey(businessObjectDataNotificationKey));
    }

    @Test
    public void testDeleteBusinessObjectDataNotificationRegistrationBusinessObjectDataNotificationNoExists()
    {
        // Try to delete a non-existing business object data notification.
        try
        {
            businessObjectDataNotificationRegistrationService
                .deleteBusinessObjectDataNotificationRegistration(new BusinessObjectDataNotificationRegistrationKey(NAMESPACE_CD, NOTIFICATION_NAME));
            fail("Should throw an ObjectNotFoundException when trying to delete a non-existing business object data notification.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                .format("Business object data notification registration with name \"%s\" does not exist for \"%s\" namespace.", NOTIFICATION_NAME,
                    NAMESPACE_CD), e.getMessage());
        }
    }

}
