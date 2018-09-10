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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdl;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.CustomDdlKeys;
import org.finra.herd.model.jpa.CustomDdlEntity;

/**
 * This class tests various functionality within the custom DDL REST controller.
 */
public class CustomDdlServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateCustomDdl()
    {
        // Create and persist a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Create a custom DDL.
        CustomDdl resultCustomDdl = customDdlService.createCustomDdl(customDdlServiceTestHelper
            .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL));

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL,
                resultCustomDdl);
    }

    @Test
    public void testCreateCustomDdlMissingRequiredParameters()
    {
        // Try to create a custom DDL instance when business object definition name is not specified.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to create a custom DDL instance when business object format usage is not specified.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to create a custom DDL instance when business object format file type is not specified.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to create a custom DDL instance when business object format version is not specified.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, CUSTOM_DDL_NAME, TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to create a custom DDL instance when custom DDL name is not specified.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, TEST_DDL));
            fail("Should throw an IllegalArgumentException when custom DDL name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A custom DDL name must be specified.", e.getMessage());
        }

        // Try to create a custom DDL instance when custom DDL is not specified.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when custom DDL is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("DDL must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateCustomDdlTrimParameters()
    {
        // Create and persist a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Create a custom DDL using input parameters with leading and trailing empty spaces.
        CustomDdl resultCustomDdl = customDdlService.createCustomDdl(customDdlServiceTestHelper
            .createCustomDdlCreateRequest(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(CUSTOM_DDL_NAME), addWhitespace(TEST_DDL)));

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL,
                resultCustomDdl);
    }

    @Test
    public void testCreateCustomDdlUpperCaseParameters()
    {
        // Create and persist a business object format entity using lower case values.
        businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, FORMAT_DESCRIPTION.toLowerCase(), FORMAT_DOCUMENT_SCHEMA.toLowerCase(), true,
            PARTITION_KEY.toLowerCase());

        // Create a custom DDL using upper case input parameters.
        CustomDdl resultCustomDdl = customDdlService.createCustomDdl(customDdlServiceTestHelper
            .createCustomDdlCreateRequest(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase(), TEST_DDL.toUpperCase()));

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(null, NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase(), TEST_DDL.toUpperCase(), resultCustomDdl);
    }

    @Test
    public void testCreateCustomDdlLowerCaseParameters()
    {
        // Create and persist a business object format entity using upper case values.
        businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, FORMAT_DESCRIPTION.toUpperCase(), FORMAT_DOCUMENT_SCHEMA.toUpperCase(), true,
            PARTITION_KEY.toUpperCase());

        // Create a custom DDL using lower case input parameters.
        CustomDdl resultCustomDdl = customDdlService.createCustomDdl(customDdlServiceTestHelper
            .createCustomDdlCreateRequest(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase(), TEST_DDL.toLowerCase()));

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(null, NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase(), TEST_DDL.toLowerCase(), resultCustomDdl);
    }

    @Test
    public void testCreateCustomDdlInvalidParameters()
    {
        // Try to create a custom DDL instance when namespace contains a forward slash character.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(addSlash(NAMESPACE), BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                    TEST_DDL));
            fail("Should throw an IllegalArgumentException when namespace contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a custom DDL instance when business object definition name contains a forward slash character.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, addSlash(BDEF_NAME), FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                    TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object definition name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a custom DDL instance when business object format usage contains a forward slash character.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, addSlash(FORMAT_USAGE_CODE), FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                    TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object format usage contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format usage can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a custom DDL instance when business object format file type contains a forward slash character.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, addSlash(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, CUSTOM_DDL_NAME,
                    TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object format file type contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format file type can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a custom DDL instance when custom DDL name contains a forward slash character.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, addSlash(CUSTOM_DDL_NAME),
                    TEST_DDL));
            fail("Should throw an IllegalArgumentException when custom DDL name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Custom DDL name can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateCustomDdlBusinessObjectFormatNoExists()
    {
        // Try to create a custom DDL instance using non-existing business object format.
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL));
            fail("Should throw an ObjectNotFoundException when business object format does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }
    }

    @Test
    public void testCreateCustomDdlCustomDdlAlreadyExists()
    {
        // Create and persist a custom DDL entity.
        customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase(), TEST_DDL);

        // Try to create a duplicate custom DDL instance (uses the same custom DDL name).
        try
        {
            customDdlService.createCustomDdl(customDdlServiceTestHelper
                .createCustomDdlCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase(),
                    TEST_DDL_2));
            fail("Should throw an AlreadyExistsException when custom DDL already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create custom DDL with name \"%s\" because it already exists for the business object format " +
                    "{namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                    "businessObjectFormatVersion: %d}.", CUSTOM_DDL_NAME.toLowerCase(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testGetCustomDdl()
    {
        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Retrieve the custom DDL.
        CustomDdl resultCustomDdl =
            customDdlService.getCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME));

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(customDdlEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                TEST_DDL, resultCustomDdl);
    }

    @Test
    public void testGetCustomDdlMissingRequiredParameters()
    {
        // Try to get a custom DDL instance when business object definition name is not specified.
        try
        {
            customDdlService.getCustomDdl(new CustomDdlKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to get a custom DDL instance when business object format usage is not specified.
        try
        {
            customDdlService.getCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to get a custom DDL instance when business object format file type is not specified.
        try
        {
            customDdlService.getCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, CUSTOM_DDL_NAME));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to get a custom DDL instance when business object format version is not specified.
        try
        {
            customDdlService.getCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, CUSTOM_DDL_NAME));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to get a custom DDL instance when custom DDL name is not specified.
        try
        {
            customDdlService.getCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when custom DDL name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A custom DDL name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetCustomDdlTrimParameters()
    {
        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Retrieve the custom DDL using input parameters with leading and trailing empty spaces.
        CustomDdl resultCustomDdl = customDdlService.getCustomDdl(
            new CustomDdlKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE),
                FORMAT_VERSION, addWhitespace(CUSTOM_DDL_NAME)));

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(customDdlEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                TEST_DDL, resultCustomDdl);
    }

    @Test
    public void testGetCustomDdlUpperCaseParameters()
    {
        // Create and persist a custom DDL entity using lower case values.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase(), TEST_DDL.toLowerCase());

        // Get the custom DDL using upper case input parameters.
        CustomDdl resultCustomDdl = customDdlService.getCustomDdl(
            new CustomDdlKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase()));

        // Validate the returned object.
        customDdlServiceTestHelper.validateCustomDdl(customDdlEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase(), TEST_DDL.toLowerCase(), resultCustomDdl);
    }

    @Test
    public void testGetCustomDdlLowerCaseParameters()
    {
        // Create and persist a custom DDL entity using upper case values.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase(), TEST_DDL.toUpperCase());

        // Get the custom DDL using lower case input parameters.
        CustomDdl resultCustomDdl = customDdlService.getCustomDdl(
            new CustomDdlKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase()));

        // Validate the returned object.
        customDdlServiceTestHelper.validateCustomDdl(customDdlEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase(), TEST_DDL.toUpperCase(), resultCustomDdl);
    }

    @Test
    public void testGetCustomDdlCustomDdlNoExists()
    {
        // Try to get a non-existing custom DDL.
        try
        {
            customDdlService.getCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME));
            fail("Should throw an ObjectNotFoundException when custom DDL does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Custom DDL with name \"%s\" does not exist for business object format with namespace \"%s\", " +
                    "business object definition name \"%s\", format usage \"%s\", format file type \"%s\", and format version \"%d\".", CUSTOM_DDL_NAME, NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testGetCustomDdls()
    {
        // List of test custom DDL names.
        List<String> testCustomDdlNames = Arrays.asList(CUSTOM_DDL_NAME, CUSTOM_DDL_NAME_2);

        // Create and persist a custom DDL entities.
        for (String customDdlName : testCustomDdlNames)
        {
            customDdlDaoTestHelper
                .createCustomDdlEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, customDdlName, TEST_DDL);
        }

        // Retrieve a list of custom DDL keys.
        CustomDdlKeys resultCustomDdlKeys =
            customDdlService.getCustomDdls(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));

        // Validate the returned object.
        assertNotNull(resultCustomDdlKeys);
        assertEquals(testCustomDdlNames.size(), resultCustomDdlKeys.getCustomDdlKeys().size());
        for (int i = 0; i < testCustomDdlNames.size(); i++)
        {
            assertEquals(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testCustomDdlNames.get(i)),
                resultCustomDdlKeys.getCustomDdlKeys().get(i));
        }
    }

    @Test
    public void testGetCustomDdlsMissingRequiredParameters()
    {
        // Try to get custom DDLs when business object definition name is not specified.
        try
        {
            customDdlService.getCustomDdls(new BusinessObjectFormatKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to get custom DDLs when business object format usage is not specified.
        try
        {
            customDdlService.getCustomDdls(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to get custom DDLs when business object format file type is not specified.
        try
        {
            customDdlService.getCustomDdls(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to get custom DDLs when business object format version is not specified.
        try
        {
            customDdlService.getCustomDdls(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetCustomDdlsTrimParameters()
    {
        // List of test custom DDL names.
        List<String> testCustomDdlNames = Arrays.asList(CUSTOM_DDL_NAME, CUSTOM_DDL_NAME_2);

        // Create and persist a custom DDL entities.
        for (String customDdlName : testCustomDdlNames)
        {
            customDdlDaoTestHelper
                .createCustomDdlEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, customDdlName, TEST_DDL);
        }

        // Retrieve a list of custom DDL keys using input parameters with leading and trailing empty spaces.
        CustomDdlKeys resultCustomDdlKeys = customDdlService.getCustomDdls(
            new BusinessObjectFormatKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION));

        // Validate the returned object.
        assertNotNull(resultCustomDdlKeys);
        assertEquals(testCustomDdlNames.size(), resultCustomDdlKeys.getCustomDdlKeys().size());
        for (int i = 0; i < testCustomDdlNames.size(); i++)
        {
            assertEquals(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testCustomDdlNames.get(i)),
                resultCustomDdlKeys.getCustomDdlKeys().get(i));
        }
    }

    @Test
    public void testGetCustomDdlsUpperCaseParameters()
    {
        // List of test custom DDL names.
        List<String> testCustomDdlNames = Arrays.asList(CUSTOM_DDL_NAME, CUSTOM_DDL_NAME_2);

        // Create and persist a custom DDL entities using lower case values.
        for (String customDdlName : testCustomDdlNames)
        {
            customDdlDaoTestHelper
                .createCustomDdlEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                    FORMAT_VERSION, customDdlName.toLowerCase(), TEST_DDL.toLowerCase());
        }

        // Retrieve a list of custom DDL keys using upper case input parameters.
        CustomDdlKeys resultCustomDdlKeys = customDdlService.getCustomDdls(
            new BusinessObjectFormatKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION));

        // Validate the returned object.
        assertNotNull(resultCustomDdlKeys);
        assertEquals(testCustomDdlNames.size(), resultCustomDdlKeys.getCustomDdlKeys().size());
        for (int i = 0; i < testCustomDdlNames.size(); i++)
        {
            assertEquals(
                new CustomDdlKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                    FORMAT_VERSION, testCustomDdlNames.get(i).toLowerCase()), resultCustomDdlKeys.getCustomDdlKeys().get(i));
        }
    }

    @Test
    public void testGetCustomDdlsLowerCaseParameters()
    {
        // List of test custom DDL names.
        List<String> testCustomDdlNames = Arrays.asList(CUSTOM_DDL_NAME, CUSTOM_DDL_NAME_2);

        // Create and persist a custom DDL entities using upper case values.
        for (String customDdlName : testCustomDdlNames)
        {
            customDdlDaoTestHelper
                .createCustomDdlEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                    FORMAT_VERSION, customDdlName.toUpperCase(), TEST_DDL.toUpperCase());
        }

        // Retrieve a list of custom DDL keys using lower case input parameters.
        CustomDdlKeys resultCustomDdlKeys = customDdlService.getCustomDdls(
            new BusinessObjectFormatKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION));

        // Validate the returned object.
        assertNotNull(resultCustomDdlKeys);
        assertEquals(testCustomDdlNames.size(), resultCustomDdlKeys.getCustomDdlKeys().size());
        for (int i = 0; i < testCustomDdlNames.size(); i++)
        {
            assertEquals(
                new CustomDdlKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                    FORMAT_VERSION, testCustomDdlNames.get(i).toUpperCase()), resultCustomDdlKeys.getCustomDdlKeys().get(i));
        }
    }

    @Test
    public void testGetCustomDdlsBusinessObjectFormatNoExists()
    {
        // Try to retrieve a list of custom DDL keys for a non-existing business object format.
        try
        {
            customDdlService.getCustomDdls(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));
            fail("Should throw an ObjectNotFoundException when business object format does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                e.getMessage());
        }
    }

    @Test
    public void testGetCustomDdlsCustomDdlsNoExist()
    {
        // Create and persist a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

        // Retrieve a list of custom DDL keys, when none of the custom DDLs exist.
        CustomDdlKeys resultCustomDdlKeys =
            customDdlService.getCustomDdls(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));

        // Validate the returned object.
        assertNotNull(resultCustomDdlKeys);
        assertEquals(0, resultCustomDdlKeys.getCustomDdlKeys().size());
    }

    @Test
    public void testUpdateCustomDdl()
    {
        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Update the custom DDL.
        CustomDdl updatedCustomDdl = customDdlService
            .updateCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME),
                customDdlServiceTestHelper.createCustomDdlUpdateRequest(TEST_DDL_2));

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(customDdlEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                TEST_DDL_2, updatedCustomDdl);
    }

    @Test
    public void testUpdateCustomDdlMissingRequiredParameters()
    {
        // Try to update a custom DDL instance when business object definition name is not specified.
        try
        {
            customDdlService.updateCustomDdl(new CustomDdlKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME),
                customDdlServiceTestHelper.createCustomDdlUpdateRequest(TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to update a custom DDL instance when business object format usage is not specified.
        try
        {
            customDdlService.updateCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME),
                customDdlServiceTestHelper.createCustomDdlUpdateRequest(TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to update a custom DDL instance when business object format file type is not specified.
        try
        {
            customDdlService.updateCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, CUSTOM_DDL_NAME),
                customDdlServiceTestHelper.createCustomDdlUpdateRequest(TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to update a custom DDL instance when business object format version is not specified.
        try
        {
            customDdlService.updateCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, CUSTOM_DDL_NAME),
                customDdlServiceTestHelper.createCustomDdlUpdateRequest(TEST_DDL));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to update a custom DDL instance when custom DDL name is not specified.
        try
        {
            customDdlService.updateCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT),
                customDdlServiceTestHelper.createCustomDdlUpdateRequest(TEST_DDL));
            fail("Should throw an IllegalArgumentException when custom DDL name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A custom DDL name must be specified.", e.getMessage());
        }

        // Try to update a custom DDL instance when custom DDL is not specified.
        try
        {
            customDdlService.updateCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME),
                customDdlServiceTestHelper.createCustomDdlUpdateRequest(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when custom DDL is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("DDL must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateCustomDdlTrimParameters()
    {
        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Update the custom DDL using input parameters with leading and trailing empty spaces.
        CustomDdl updatedCustomDdl = customDdlService.updateCustomDdl(
            new CustomDdlKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE),
                FORMAT_VERSION, addWhitespace(CUSTOM_DDL_NAME)), customDdlServiceTestHelper.createCustomDdlUpdateRequest(addWhitespace(TEST_DDL_2)));

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(customDdlEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                TEST_DDL_2, updatedCustomDdl);
    }

    @Test
    public void testUpdateCustomDdlUpperCaseParameters()
    {
        // Create and persist a custom DDL entity using lower case values.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase(), TEST_DDL.toLowerCase());

        // Update the custom DDL using upper case input parameters.
        CustomDdl updatedCustomDdl = customDdlService.updateCustomDdl(
            new CustomDdlKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase()), customDdlServiceTestHelper.createCustomDdlUpdateRequest(TEST_DDL_2.toUpperCase()));

        // Validate the returned object.
        customDdlServiceTestHelper.validateCustomDdl(customDdlEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase(), TEST_DDL_2.toUpperCase(), updatedCustomDdl);
    }

    @Test
    public void testUpdateCustomDdlLowerCaseParameters()
    {
        // Create and persist a custom DDL entity using upper case values.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase(), TEST_DDL.toUpperCase());

        // Update the custom DDL using lower case input parameters.
        CustomDdl updatedCustomDdl = customDdlService.updateCustomDdl(
            new CustomDdlKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase()), customDdlServiceTestHelper.createCustomDdlUpdateRequest(TEST_DDL_2.toLowerCase()));

        // Validate the returned object.
        customDdlServiceTestHelper.validateCustomDdl(customDdlEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase(), TEST_DDL_2.toLowerCase(), updatedCustomDdl);
    }

    @Test
    public void testUpdateCustomDdlCustomDdlNoExists()
    {
        // Try to update a non-existing custom DDL.
        try
        {
            customDdlService.updateCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME),
                customDdlServiceTestHelper.createCustomDdlUpdateRequest(TEST_DDL));
            fail("Should throw an ObjectNotFoundException when custom DDL does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Custom DDL with name \"%s\" does not exist for business object format with namespace \"%s\", " +
                    "business object definition name \"%s\", format usage \"%s\", format file type \"%s\", and format version \"%d\".", CUSTOM_DDL_NAME, NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testDeleteCustomDdl()
    {
        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Validate that this custom DDL exists.
        CustomDdlKey customDdlKey = new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);
        assertNotNull(customDdlDao.getCustomDdlByKey(customDdlKey));

        // Delete this custom DDL.
        CustomDdl deletedCustomDdl =
            customDdlService.deleteCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME));

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(customDdlEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                TEST_DDL, deletedCustomDdl);

        // Ensure that this custom DDL is no longer there.
        assertNull(customDdlDao.getCustomDdlByKey(customDdlKey));
    }

    @Test
    public void testDeleteCustomDdlMissingRequiredParameters()
    {
        // Try to delete a custom DDL instance when business object definition name is not specified.
        try
        {
            customDdlService
                .deleteCustomDdl(new CustomDdlKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to delete a custom DDL instance when business object format usage is not specified.
        try
        {
            customDdlService.deleteCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to delete a custom DDL instance when business object format file type is not specified.
        try
        {
            customDdlService.deleteCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, CUSTOM_DDL_NAME));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to delete a custom DDL instance when business object format version is not specified.
        try
        {
            customDdlService.deleteCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, CUSTOM_DDL_NAME));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to delete a custom DDL instance when custom DDL name is not specified.
        try
        {
            customDdlService.deleteCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when custom DDL name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A custom DDL name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteCustomDdlTrimParameters()
    {
        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Validate that this custom DDL exists.
        CustomDdlKey customDdlKey = new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);
        assertNotNull(customDdlDao.getCustomDdlByKey(customDdlKey));

        // Delete the custom DDL using input parameters with leading and trailing empty spaces.
        CustomDdl deletedCustomDdl = customDdlService.deleteCustomDdl(
            new CustomDdlKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE),
                FORMAT_VERSION, addWhitespace(CUSTOM_DDL_NAME)));

        // Validate the returned object.
        customDdlServiceTestHelper
            .validateCustomDdl(customDdlEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                TEST_DDL, deletedCustomDdl);

        // Ensure that this custom DDL is no longer there.
        assertNull(customDdlDao.getCustomDdlByKey(customDdlKey));
    }

    @Test
    public void testDeleteCustomDdlUpperCaseParameters()
    {
        // Create and persist a custom DDL entity using lower case values.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase(), TEST_DDL.toLowerCase());

        // Validate that this custom DDL exists.
        CustomDdlKey customDdlKey =
            new CustomDdlKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase());
        assertNotNull(customDdlDao.getCustomDdlByKey(customDdlKey));

        // Delete the custom DDL using upper case input parameters.
        CustomDdl deletedCustomDdl = customDdlService.deleteCustomDdl(
            new CustomDdlKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase()));

        // Validate the returned object.
        customDdlServiceTestHelper.validateCustomDdl(customDdlEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase(), TEST_DDL.toLowerCase(), deletedCustomDdl);

        // Ensure that this custom DDL is no longer there.
        assertNull(customDdlDao.getCustomDdlByKey(customDdlKey));
    }

    @Test
    public void testDeleteCustomDdlLowerCaseParameters()
    {
        // Create and persist a custom DDL entity using upper case values.
        CustomDdlEntity customDdlEntity = customDdlDaoTestHelper
            .createCustomDdlEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase(), TEST_DDL.toUpperCase());

        // Validate that this custom DDL exists.
        CustomDdlKey customDdlKey =
            new CustomDdlKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase());
        assertNotNull(customDdlDao.getCustomDdlByKey(customDdlKey));

        // Delete the custom DDL using lower case input parameters.
        CustomDdl deletedCustomDdl = customDdlService.deleteCustomDdl(
            new CustomDdlKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, CUSTOM_DDL_NAME.toLowerCase()));

        // Validate the returned object.
        customDdlServiceTestHelper.validateCustomDdl(customDdlEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, CUSTOM_DDL_NAME.toUpperCase(), TEST_DDL.toUpperCase(), deletedCustomDdl);

        // Ensure that this custom DDL is no longer there.
        assertNull(customDdlDao.getCustomDdlByKey(customDdlKey));
    }

    @Test
    public void testDeleteCustomDdlCustomDdlNoExists()
    {
        // Try to delete a non-existing custom DDL.
        try
        {
            customDdlService.deleteCustomDdl(new CustomDdlKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME));
            fail("Should throw an ObjectNotFoundException when custom DDL does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Custom DDL with name \"%s\" does not exist for business object format with namespace \"%s\", " +
                    "business object definition name \"%s\", format usage \"%s\", format file type \"%s\", and format version \"%d\".", CUSTOM_DDL_NAME, NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), e.getMessage());
        }
    }
}
