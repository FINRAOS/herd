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

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKeys;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

/**
 * This class tests various functionality within the business object definition REST controller.
 */
public class BusinessObjectDefinitionServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectDefinitionServiceImpl")
    private BusinessObjectDefinitionService businessObjectDefinitionServiceImpl;

    @Test
    public void testCreateBusinessObjectDefinition() throws Exception
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition.
        BusinessObjectDefinitionCreateRequest request =
            createBusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes());
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(request);

        // Validate the returned object.
        validateBusinessObjectDefinition(null, NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes(), resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionMissingRequiredParameters()
    {
        // Try to create a business object definition instance when namespace is not specified.
        try
        {
            businessObjectDefinitionService
                .createBusinessObjectDefinition(createBusinessObjectDefinitionCreateRequest(BLANK_TEXT, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to create a business object definition instance when object definition name is not specified.
        try
        {
            businessObjectDefinitionService
                .createBusinessObjectDefinition(createBusinessObjectDefinitionCreateRequest(NAMESPACE, BLANK_TEXT, DATA_PROVIDER_NAME, BDEF_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to create a business object definition instance when data provider name is not specified.
        try
        {
            businessObjectDefinitionService
                .createBusinessObjectDefinition(createBusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, BLANK_TEXT, BDEF_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when data provider name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A data provider name must be specified.", e.getMessage());
        }

        // Try to create a business object definition instance when attribute name is not specified.
        try
        {
            businessObjectDefinitionService.createBusinessObjectDefinition(
                createBusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                    Arrays.asList(new Attribute(BLANK_TEXT, ATTRIBUTE_VALUE_1))));
            fail("Should throw an IllegalArgumentException when attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An attribute name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionMissingOptionalParametersPassedAsWhitespace()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            createBusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BLANK_TEXT,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT))));

        // Validate the returned object.
        validateBusinessObjectDefinition(null, NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BLANK_TEXT,
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT)), resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionMissingOptionalParametersPassedAsNulls()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            createBusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, null,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null))));

        // Validate the returned object.
        validateBusinessObjectDefinition(null, NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, null, Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null)),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionNoAttributes()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition without specifying any of the attributes.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService
            .createBusinessObjectDefinition(createBusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, null, null));

        // Validate the returned object.
        validateBusinessObjectDefinition(null, NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, null, NO_ATTRIBUTES, resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionTrimParameters()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            createBusinessObjectDefinitionCreateRequest(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(DATA_PROVIDER_NAME),
                addWhitespace(BDEF_DESCRIPTION), Arrays.asList(new Attribute(addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), addWhitespace(ATTRIBUTE_VALUE_1)))));

        // Validate the returned object.
        validateBusinessObjectDefinition(null, NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, addWhitespace(BDEF_DESCRIPTION),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1))), resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionUpperCaseParameters()
    {
        // Create and persist database entities required for testing using lower case values.
        createDatabaseEntitiesForBusinessObjectDefinitionTesting(NAMESPACE.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase());

        // Create a business object definition using upper case input parameters.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            createBusinessObjectDefinitionCreateRequest(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(),
                BDEF_DESCRIPTION.toUpperCase(), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase()))));

        // Validate the returned object.
        validateBusinessObjectDefinition(null, NAMESPACE.toLowerCase(), BDEF_NAME.toUpperCase(), DATA_PROVIDER_NAME.toLowerCase(),
            BDEF_DESCRIPTION.toUpperCase(), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionLowerCaseParameters()
    {
        // Create and persist database entities required for testing using upper case values.
        createDatabaseEntitiesForBusinessObjectDefinitionTesting(NAMESPACE.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase());

        // Create a business object definition using upper case input parameters.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            createBusinessObjectDefinitionCreateRequest(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(),
                BDEF_DESCRIPTION.toLowerCase(), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase()))));

        // Validate the returned object.
        validateBusinessObjectDefinition(null, NAMESPACE.toUpperCase(), BDEF_NAME.toLowerCase(), DATA_PROVIDER_NAME.toUpperCase(),
            BDEF_DESCRIPTION.toLowerCase(), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionInvalidParameters()
    {
        BusinessObjectDefinitionCreateRequest request;

        // Create and persist database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Try to create a business object definition using non-existing namespace.
        request = createBusinessObjectDefinitionCreateRequest("I_DO_NOT_EXIST", BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);
        try
        {
            businessObjectDefinitionService.createBusinessObjectDefinition(request);
            fail("Should throw an ObjectNotFoundException when using non-existing namespace.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", request.getNamespace()), e.getMessage());
        }

        // Try to create a business object definition when namespace contains a forward slash character.
        request = createBusinessObjectDefinitionCreateRequest(addSlash(BDEF_NAMESPACE), BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);
        try
        {
            businessObjectDefinitionService.createBusinessObjectDefinition(request);
            fail("Should throw an IllegalArgumentException when namespace contains a forward slash character");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object definition when business object definition name contains a forward slash character.
        request = createBusinessObjectDefinitionCreateRequest(BDEF_NAMESPACE, addSlash(BDEF_NAME), DATA_PROVIDER_NAME, BDEF_DESCRIPTION);
        try
        {
            businessObjectDefinitionService.createBusinessObjectDefinition(request);
            fail("Should throw an IllegalArgumentException when business object definition name contains a forward slash character");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object definition using non-existing data provider.
        request = createBusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", BDEF_DESCRIPTION);
        try
        {
            businessObjectDefinitionService.createBusinessObjectDefinition(request);
            fail("Should throw an ObjectNotFoundException when using non-existing data provider name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Data provider with name \"%s\" doesn't exist.", request.getDataProviderName()), e.getMessage());
        }

        // Try to create a business object definition when data provider name contains a forward slash character.
        request = createBusinessObjectDefinitionCreateRequest(BDEF_NAMESPACE, BDEF_NAME, addSlash(DATA_PROVIDER_NAME), BDEF_DESCRIPTION);
        try
        {
            businessObjectDefinitionService.createBusinessObjectDefinition(request);
            fail("Should throw an IllegalArgumentException when data provider name contains a forward slash character");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Data provider name can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionDuplicateAttributes()
    {
        // Try to create a business object definition instance when duplicate attributes are specified.
        try
        {
            businessObjectDefinitionService.createBusinessObjectDefinition(
                createBusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, Arrays
                    .asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_3),
                        new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_3))));
            fail("Should throw an IllegalArgumentException when duplicate attributes are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate attribute name found: %s", ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase()), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionAlreadyExists() throws Exception
    {
        // Create and persist a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Try to create a business object definition when it already exists.
        try
        {
            businessObjectDefinitionService
                .createBusinessObjectDefinition(createBusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION));
            fail("Should throw an AlreadyExistsException when business object definition already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String
                    .format("Unable to create business object definition with name \"%s\" because it already exists for namespace \"%s\".", BDEF_NAME, NAMESPACE),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionDuplicateNamesWithDifferentNamespaces() throws Exception
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDefinitionTesting(NAMESPACE_2, DATA_PROVIDER_NAME);

        // Create and persist a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create a business object definition that has the same name, but belongs to a different namespace.
        BusinessObjectDefinitionCreateRequest request =
            createBusinessObjectDefinitionCreateRequest(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes());
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(request);

        // Validate the returned object.
        validateBusinessObjectDefinition(null, NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes(),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes());

        // Perform an update by changing the description and updating the attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                createBusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, getNewAttributes2()));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
            getNewAttributes2(), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionMissingRequiredParameters()
    {
        // Try to update a business object definition instance when object definition name is not specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BLANK_TEXT),
                createBusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, getNewAttributes2()));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to update a business object definition instance when attribute name is not specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                createBusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, Arrays.asList(new Attribute(BLANK_TEXT, ATTRIBUTE_VALUE_1))));
            fail("Should throw an IllegalArgumentException when attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("An attribute name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionTrimParameters()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes());

        // Perform an update using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME)),
                createBusinessObjectDefinitionUpdateRequest(addWhitespace(BDEF_DESCRIPTION_2),
                    Arrays.asList(new Attribute(addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), addWhitespace(ATTRIBUTE_VALUE_1)))));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, addWhitespace(BDEF_DESCRIPTION_2),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1))), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionUpperCaseParameters()
    {
        // Create and persist a business object definition entity using lower case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(),
                BDEF_DESCRIPTION.toLowerCase(), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())));

        // Perform an update using upper case input parameters.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()),
                createBusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2.toUpperCase(),
                    Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase()))));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(),
            DATA_PROVIDER_NAME.toLowerCase(), BDEF_DESCRIPTION_2.toUpperCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toUpperCase())), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionLowerCaseParameters()
    {
        // Create and persist a business object definition entity using upper case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(),
                BDEF_DESCRIPTION.toUpperCase(), Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())));

        // Perform an update using lower case input parameters.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()),
                createBusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2.toLowerCase(),
                    Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase()))));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(),
            DATA_PROVIDER_NAME.toUpperCase(), BDEF_DESCRIPTION_2.toLowerCase(),
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toLowerCase())), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDuplicateAttributes()
    {
        // Try to update a business object definition instance when duplicate attributes are specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                createBusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, Arrays
                    .asList(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_3),
                        new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_3))));
            fail("Should throw an IllegalArgumentException when duplicate attributes are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate attribute name found: %s", ATTRIBUTE_NAME_3_MIXED_CASE.toUpperCase()), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionNoExists() throws Exception
    {
        // Try to update a non-existing business object definition.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                createBusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, getNewAttributes2()));
            fail("Should throw an ObjectNotFoundException when business object definition doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BDEF_NAME, NAMESPACE),
                e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionNoOriginalAttributes() throws Exception
    {
        // Create and persist a business object definition entity without any attributes.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, NO_ATTRIBUTES);

        // Perform an update by changing the description and adding the new attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                createBusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, getNewAttributes2()));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
            getNewAttributes2(), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDuplicateOriginalAttributes() throws Exception
    {
        // Create and persist a business object definition entity with duplicate attributes.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, Arrays
            .asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1),
                new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1_UPDATED)));

        // Try to update a business object definition that has duplicate attributes.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                createBusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, getNewAttributes2()));
            fail("Should throw an IllegalStateException when business object definition contains duplicate attributes.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("Found duplicate attribute with name \"%s\" for business object definition {namespace: \"%s\", businessObjectDefinitionName: \"%s\"}.",
                    ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), NAMESPACE, BDEF_NAME), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionAlt() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntityAlt(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_ALIAS);

        // Perform an update by changing the description and updating the attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionAlt(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                createBusinessObjectDefinitionAltUpdateRequest(BDEF_DESCRIPTION_2, BDEF_ALIAS_2));

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2, BDEF_ALIAS_2,
                NO_ATTRIBUTES), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionAltMissingRequiredParameters()
    {
        // Try to update a business object definition instance when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinitionAlt(new BusinessObjectDefinitionKey(BLANK_TEXT, BDEF_NAME),
                createBusinessObjectDefinitionAltUpdateRequest(BDEF_DESCRIPTION_2, BDEF_ALIAS_2));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to update a business object definition instance when business object definition name is not specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinitionAlt(new BusinessObjectDefinitionKey(NAMESPACE, BLANK_TEXT),
                createBusinessObjectDefinitionAltUpdateRequest(BDEF_DESCRIPTION_2, BDEF_ALIAS_2));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionAltOptionalParametersPassedAsWhitespace()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntityAlt(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_ALIAS);

        // Perform an update using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionAlt(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                createBusinessObjectDefinitionAltUpdateRequest(BLANK_TEXT, BLANK_TEXT));

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BLANK_TEXT, BLANK_TEXT.trim(),
                NO_ATTRIBUTES), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionAltOptionalParametersPassedAsNulls()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntityAlt(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_ALIAS);

        // Perform an update using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionAlt(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                createBusinessObjectDefinitionAltUpdateRequest(null, null));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, null, null, NO_ATTRIBUTES),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionAltTrimParameters()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntityAlt(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_ALIAS);

        // Perform an update using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionAlt(new BusinessObjectDefinitionKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME)),
                createBusinessObjectDefinitionAltUpdateRequest(addWhitespace(BDEF_DESCRIPTION_2), addWhitespace(BDEF_ALIAS_2)));

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, addWhitespace(BDEF_DESCRIPTION_2),
                BDEF_ALIAS_2, NO_ATTRIBUTES), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionAltUpperCaseParameters()
    {
        // Create and persist a business object definition entity using lower case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntityAlt(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_ALIAS);

        // Perform an update using upper case input parameters.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionAlt(new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()),
                createBusinessObjectDefinitionAltUpdateRequest(BDEF_DESCRIPTION_2, BDEF_ALIAS_2));

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2, BDEF_ALIAS_2,
                NO_ATTRIBUTES), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionAltLowerCaseParameters()
    {
        // Create and persist a business object definition entity using lower case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntityAlt(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_ALIAS);

        // Perform an update using lower case input parameters.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionAlt(new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()),
                createBusinessObjectDefinitionAltUpdateRequest(BDEF_DESCRIPTION_2, BDEF_ALIAS_2));

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2, BDEF_ALIAS_2,
                NO_ATTRIBUTES), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionAltBusinessObjectDefinitionNoExists() throws Exception
    {
        // Try to update a non-existing business object definition.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinitionAlt(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                createBusinessObjectDefinitionAltUpdateRequest(BDEF_DESCRIPTION_2, BDEF_ALIAS_2));
            fail("Should throw an ObjectNotFoundException when business object definition doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BDEF_NAME, NAMESPACE),
                e.getMessage());
        }
    }


    @Test
    public void testGetBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes());

        // Retrieve the business object definition.
        BusinessObjectDefinition resultBusinessObjectDefinition =
            businessObjectDefinitionService.getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes(),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionMissingRequiredParameters()
    {
        // Try to get a business object definition instance when object definition name is not specified.
        try
        {
            businessObjectDefinitionService.getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionTrimParameters()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes());

        // Retrieve the business object definition using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition resultBusinessObjectDefinition =
            businessObjectDefinitionService.getBusinessObjectDefinition(new BusinessObjectDefinitionKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME)));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes(),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionUpperCaseParameters()
    {
        // Create and persist a business object definition entity using lower case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(),
                BDEF_DESCRIPTION.toLowerCase(), getNewAttributes());

        // Retrieve the business object definition using upper case input parameters.
        BusinessObjectDefinition resultBusinessObjectDefinition =
            businessObjectDefinitionService.getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(),
            DATA_PROVIDER_NAME.toLowerCase(), BDEF_DESCRIPTION.toLowerCase(), getNewAttributes(), resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionLowerCaseParameters()
    {
        // Create and persist a business object definition entity using upper case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(),
                BDEF_DESCRIPTION.toUpperCase(), getNewAttributes());

        // Retrieve the business object definition using lower case input parameters.
        BusinessObjectDefinition resultBusinessObjectDefinition =
            businessObjectDefinitionService.getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(),
            DATA_PROVIDER_NAME.toUpperCase(), BDEF_DESCRIPTION.toUpperCase(), getNewAttributes(), resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionNoExists() throws Exception
    {
        // Try to get a non-existing business object definition.
        try
        {
            businessObjectDefinitionService.getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
            fail("Should throw an ObjectNotFoundException when business object definition doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BDEF_NAME, NAMESPACE),
                e.getMessage());
        }
    }

    /**
     * This method is to get coverage for the business object definition service method that starts a new transaction.
     */
    @Test
    public void testGetBusinessObjectDefinitionNewTransaction() throws Exception
    {
        try
        {
            businessObjectDefinitionServiceImpl.getBusinessObjectDefinition(new BusinessObjectDefinitionKey());
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitions() throws Exception
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : businessObjectDefinitionDaoTestHelper.getTestBusinessObjectDefinitionKeys())
        {
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        }

        // Retrieve a list of business object definition keys for the specified namespace.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionService.getBusinessObjectDefinitions(NAMESPACE);

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDaoTestHelper.getExpectedBusinessObjectDefinitionKeys(), resultKeys.getBusinessObjectDefinitionKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionsTrimParameters()
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : businessObjectDefinitionDaoTestHelper.getTestBusinessObjectDefinitionKeys())
        {
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        }

        // Retrieve a list of business object definition keys for the specified namespace using namespace value with leading and trailing empty spaces.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionService.getBusinessObjectDefinitions(addWhitespace(NAMESPACE));

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDaoTestHelper.getExpectedBusinessObjectDefinitionKeys(), resultKeys.getBusinessObjectDefinitionKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionsUpperCaseParameters()
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : businessObjectDefinitionDaoTestHelper.getTestBusinessObjectDefinitionKeys())
        {
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        }

        // Retrieve a list of business object definition keys for the specified namespace using upper case namespace value.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionService.getBusinessObjectDefinitions(NAMESPACE.toUpperCase());

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDaoTestHelper.getExpectedBusinessObjectDefinitionKeys(), resultKeys.getBusinessObjectDefinitionKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionsLowerCaseParameters()
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : businessObjectDefinitionDaoTestHelper.getTestBusinessObjectDefinitionKeys())
        {
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        }

        // Retrieve a list of business object definition keys for the specified namespace using lower case namespace value.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionService.getBusinessObjectDefinitions(NAMESPACE.toLowerCase());

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDaoTestHelper.getExpectedBusinessObjectDefinitionKeys(), resultKeys.getBusinessObjectDefinitionKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionsEmptyList() throws Exception
    {
        // Retrieve an empty list of business object definition keys.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionService.getBusinessObjectDefinitions("I_DO_NOT_EXIST");

        // Validate the returned object.
        assertNotNull(resultKeys);
        assertEquals(0, resultKeys.getBusinessObjectDefinitionKeys().size());
    }

    @Test
    public void testDeleteBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes());

        // Validate that this business object definition exists.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        assertNotNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));

        // Delete this business object definition.
        BusinessObjectDefinition deletedBusinessObjectDefinition =
            businessObjectDefinitionService.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes(),
            deletedBusinessObjectDefinition);

        // Ensure that this business object definition is no longer there.
        assertNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionMissingRequiredParameters()
    {
        // Try to delete a business object definition instance when object definition name is not specified.
        try
        {
            businessObjectDefinitionService.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDefinitionTrimParameters()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes());

        // Validate that this business object definition exists.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        assertNotNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));

        // Delete this business object definition using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition deletedBusinessObjectDefinition =
            businessObjectDefinitionService.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME)));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, getNewAttributes(),
            deletedBusinessObjectDefinition);

        // Ensure that this business object definition is no longer there.
        assertNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionUpperCaseParameters()
    {
        // Create and persist a business object definition entity using lower case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(),
                BDEF_DESCRIPTION.toLowerCase(), getNewAttributes());

        // Validate that this business object definition exists.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase());
        assertNotNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));

        // Delete this business object definition using upper case input parameters.
        BusinessObjectDefinition deletedBusinessObjectDefinition =
            businessObjectDefinitionService.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(),
            DATA_PROVIDER_NAME.toLowerCase(), BDEF_DESCRIPTION.toLowerCase(), getNewAttributes(), deletedBusinessObjectDefinition);

        // Ensure that this business object definition is no longer there.
        assertNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionLowerCaseParameters()
    {
        // Create and persist a business object definition entity using upper case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(),
                BDEF_DESCRIPTION.toUpperCase(), getNewAttributes());

        // Validate that this business object definition exists.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toLowerCase());
        assertNotNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));

        // Retrieve the business object definition using lower case input parameters.
        BusinessObjectDefinition deletedBusinessObjectDefinition =
            businessObjectDefinitionService.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(),
            DATA_PROVIDER_NAME.toUpperCase(), BDEF_DESCRIPTION.toUpperCase(), getNewAttributes(), deletedBusinessObjectDefinition);

        // Ensure that this business object definition is no longer there.
        assertNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionNoExists() throws Exception
    {
        // Try to get a non-existing business object definition.
        try
        {
            businessObjectDefinitionService.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
            fail("Should throw an ObjectNotFoundException when business object definition doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BDEF_NAME, NAMESPACE),
                e.getMessage());
        }
    }
}
