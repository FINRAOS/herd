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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionChangeEvent;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptiveInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionUpdateRequest;
import org.finra.herd.model.api.xml.DescriptiveBusinessObjectFormat;
import org.finra.herd.model.api.xml.DescriptiveBusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.SampleDataFile;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.BusinessObjectDefinitionSampleFileUpdateDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.SearchIndexUpdateDto;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;

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
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);

        // Retrieve the business object definition with include update history flag set to true.
        resultBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Create business object definition change event.
        List<BusinessObjectDefinitionChangeEvent> businessObjectDefinitionChangeEvents = new ArrayList<>();
        businessObjectDefinitionChangeEvents.add(
            new BusinessObjectDefinitionChangeEvent(BDEF_DISPLAY_NAME, BDEF_DESCRIPTION, NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_UPDATE_REQUEST,
                resultBusinessObjectDefinition.getBusinessObjectDefinitionChangeEvents().get(0).getEventTime(),
                resultBusinessObjectDefinition.getBusinessObjectDefinitionChangeEvents().get(0).getUserId()));

        // Validate the result.
        assertEquals(new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), businessObjectDefinitionChangeEvents),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionMissingRequiredParameters()
    {
        // Try to create a business object definition instance when namespace is not specified.
        try
        {
            businessObjectDefinitionService.createBusinessObjectDefinition(
                new BusinessObjectDefinitionCreateRequest(BLANK_TEXT, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to create a business object definition instance when object definition name is not specified.
        try
        {
            businessObjectDefinitionService.createBusinessObjectDefinition(
                new BusinessObjectDefinitionCreateRequest(NAMESPACE, BLANK_TEXT, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to create a business object definition instance when data provider name is not specified.
        try
        {
            businessObjectDefinitionService.createBusinessObjectDefinition(
                new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, BLANK_TEXT, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES));
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
                new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
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
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BLANK_TEXT, BLANK_TEXT,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT))));

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BLANK_TEXT,
            NO_BDEF_SHORT_DESCRIPTION, EMPTY_STRING, Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT)),
            NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()),
            NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS), resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionMissingOptionalParametersPassedAsNulls()
    {
        // Create and persist database entities required for testing.
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, null, null,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null))));

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, null, null, null,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null)), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);

        // Retrieve the business object definition with include update history flag set to true.
        resultBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Validate the result.
        assertEquals(new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, null, null, null,
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null)), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionNoAttributes()
    {
        // Create and persist database entities required for testing.
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition without specifying any of the attributes.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, NO_BDEF_DESCRIPTION, NO_BDEF_DISPLAY_NAME, NO_ATTRIBUTES));

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, NO_BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, NO_BDEF_DISPLAY_NAME, NO_ATTRIBUTES, NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionTrimParameters()
    {
        // Create and persist database entities required for testing.
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            new BusinessObjectDefinitionCreateRequest(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(DATA_PROVIDER_NAME),
                addWhitespace(BDEF_DESCRIPTION), addWhitespace(BDEF_DISPLAY_NAME),
                Arrays.asList(new Attribute(addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), addWhitespace(ATTRIBUTE_VALUE_1)))));

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, addWhitespace(BDEF_DESCRIPTION),
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1))),
                NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(),
                businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()),
                NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS), resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionUpperCaseParameters()
    {
        // Create and persist database entities required for testing using lower case values.
        businessObjectDefinitionServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDefinitionTesting(NAMESPACE.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase());

        // Create a business object definition using upper case input parameters.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            new BusinessObjectDefinitionCreateRequest(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(),
                BDEF_DESCRIPTION.toUpperCase(), BDEF_DISPLAY_NAME.toUpperCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase()))));

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toUpperCase(),
                DATA_PROVIDER_NAME.toLowerCase(), BDEF_DESCRIPTION.toUpperCase(), NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME.toUpperCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionLowerCaseParameters()
    {
        // Create and persist database entities required for testing using upper case values.
        businessObjectDefinitionServiceTestHelper
            .createDatabaseEntitiesForBusinessObjectDefinitionTesting(NAMESPACE.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase());

        // Create a business object definition using upper case input parameters.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(
            new BusinessObjectDefinitionCreateRequest(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(),
                BDEF_DESCRIPTION.toLowerCase(), BDEF_DISPLAY_NAME.toLowerCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase()))));

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toLowerCase(),
                DATA_PROVIDER_NAME.toUpperCase(), BDEF_DESCRIPTION.toLowerCase(), NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME.toLowerCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateBusinessObjectDefinitionInvalidParameters()
    {
        BusinessObjectDefinitionCreateRequest request;

        // Create and persist database entities required for testing.
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Try to create a business object definition using non-existing namespace.
        request =
            new BusinessObjectDefinitionCreateRequest("I_DO_NOT_EXIST", BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);
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
        request = new BusinessObjectDefinitionCreateRequest(addSlash(BDEF_NAMESPACE), BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
            NO_ATTRIBUTES);
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
        request = new BusinessObjectDefinitionCreateRequest(BDEF_NAMESPACE, addSlash(BDEF_NAME), DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
            NO_ATTRIBUTES);
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
        request = new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);
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
        request = new BusinessObjectDefinitionCreateRequest(BDEF_NAMESPACE, BDEF_NAME, addSlash(DATA_PROVIDER_NAME), BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
            NO_ATTRIBUTES);
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
                new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, Arrays
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
            businessObjectDefinitionService.createBusinessObjectDefinition(
                new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES));
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
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting(NAMESPACE_2, DATA_PROVIDER_NAME);

        // Create and persist a business object definition.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create a business object definition that has the same name, but belongs to a different namespace.
        BusinessObjectDefinitionCreateRequest request =
            new BusinessObjectDefinitionCreateRequest(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(request);

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE_2, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Perform an update by changing the description and updating the attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    businessObjectDefinitionServiceTestHelper.getNewAttributes2()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2, businessObjectDefinitionServiceTestHelper.getNewAttributes2(),
            NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()),
            NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS), updatedBusinessObjectDefinition);

        // Retrieve the business object definition with include update history flag set to true.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Create business object definition change event.
        List<BusinessObjectDefinitionChangeEvent> businessObjectDefinitionChangeEvents = new ArrayList<>();
        businessObjectDefinitionChangeEvents.add(
            new BusinessObjectDefinitionChangeEvent(BDEF_DISPLAY_NAME_2, BDEF_DESCRIPTION_2, NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_UPDATE_REQUEST,
                resultBusinessObjectDefinition.getBusinessObjectDefinitionChangeEvents().get(0).getEventTime(),
                resultBusinessObjectDefinition.getBusinessObjectDefinitionChangeEvents().get(0).getUserId()));

        // Validate the result.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2, businessObjectDefinitionServiceTestHelper.getNewAttributes2(),
            NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()),
            businessObjectDefinitionChangeEvents), resultBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionMissingRequiredParameters()
    {
        // Try to update a business object definition instance when object definition name is not specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BLANK_TEXT),
                new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    businessObjectDefinitionServiceTestHelper.getNewAttributes2()));
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
                new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    Arrays.asList(new Attribute(BLANK_TEXT, ATTRIBUTE_VALUE_1))));
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
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Perform an update using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME)),
                new BusinessObjectDefinitionUpdateRequest(addWhitespace(BDEF_DESCRIPTION_2), addWhitespace(BDEF_DISPLAY_NAME_2),
                    Arrays.asList(new Attribute(addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), addWhitespace(ATTRIBUTE_VALUE_1)))));

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, addWhitespace(BDEF_DESCRIPTION_2),
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2, Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1))),
                NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(),
                businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()),
                NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionUpperCaseParameters()
    {
        // Create and persist a business object definition entity using lower case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(),
                BDEF_DESCRIPTION.toLowerCase(), BDEF_DISPLAY_NAME.toLowerCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase())));

        // Perform an update using upper case input parameters.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()),
                new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2.toUpperCase(), BDEF_DISPLAY_NAME_2.toUpperCase(),
                    Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase()))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(),
                DATA_PROVIDER_NAME.toLowerCase(), BDEF_DESCRIPTION_2.toUpperCase(), NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2.toUpperCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toUpperCase())), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionLowerCaseParameters()
    {
        // Create and persist a business object definition entity using upper case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(),
                BDEF_DESCRIPTION.toUpperCase(), BDEF_DISPLAY_NAME.toUpperCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase())));

        // Perform an update using lower case input parameters.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()),
                new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2.toLowerCase(), BDEF_DISPLAY_NAME_2.toLowerCase(),
                    Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase()))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(),
                DATA_PROVIDER_NAME.toUpperCase(), BDEF_DESCRIPTION_2.toLowerCase(), NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2.toLowerCase(),
                Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toLowerCase())), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDuplicateAttributes()
    {
        // Try to update a business object definition instance when duplicate attributes are specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2, Arrays
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
                new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    businessObjectDefinitionServiceTestHelper.getNewAttributes2()));
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
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Perform an update by changing the description and adding the new attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    businessObjectDefinitionServiceTestHelper.getNewAttributes2()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2, businessObjectDefinitionServiceTestHelper.getNewAttributes2(),
            NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()),
            NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDuplicateOriginalAttributes() throws Exception
    {
        // Create and persist a business object definition entity with duplicate attributes.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, Arrays
                .asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1),
                    new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1_UPDATED)));

        // Try to update a business object definition that has duplicate attributes.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    businessObjectDefinitionServiceTestHelper.getNewAttributes2()));
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
    public void testUpdateBusinessObjectDefinitionDescriptiveInformation() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Create a business object format for the business object definition entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Perform an update by changing the description and updating the descriptive format.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    new DescriptiveBusinessObjectFormatUpdateRequest(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2, NO_ATTRIBUTES,
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);

        // Retrieve the business object definition with include update history flag set to true.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Create business object definition change event
        List<BusinessObjectDefinitionChangeEvent> businessObjectDefinitionChangeEvents = new ArrayList<>();
        businessObjectDefinitionChangeEvents.add(new BusinessObjectDefinitionChangeEvent(BDEF_DISPLAY_NAME_2, BDEF_DESCRIPTION_2,
            new DescriptiveBusinessObjectFormatUpdateRequest(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE),
            resultBusinessObjectDefinition.getBusinessObjectDefinitionChangeEvents().get(0).getEventTime(),
            resultBusinessObjectDefinition.getBusinessObjectDefinitionChangeEvents().get(0).getUserId()));

        // Validate the result.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2, NO_ATTRIBUTES,
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), businessObjectDefinitionChangeEvents),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformationMissingRequiredParameters()
    {
        // Try to update a business object definition instance when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BLANK_TEXT, BDEF_NAME),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_UPDATE_REQUEST));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to update a business object definition instance when business object definition name is not specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BLANK_TEXT),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_UPDATE_REQUEST));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to update a business object definition instance when descriptive business object format usage is not specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    new DescriptiveBusinessObjectFormatUpdateRequest(BLANK_TEXT, FORMAT_FILE_TYPE_CODE)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to update a business object definition instance when descriptive business object format file type is not specified.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    new DescriptiveBusinessObjectFormatUpdateRequest(FORMAT_USAGE_CODE, BLANK_TEXT)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformationOptionalParametersPassedAsWhitespace()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Perform an update using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BLANK_TEXT, BLANK_TEXT, NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_UPDATE_REQUEST));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BLANK_TEXT,
                NO_BDEF_SHORT_DESCRIPTION, BLANK_TEXT.trim(), NO_ATTRIBUTES, NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformationOptionalParametersPassedAsNulls()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Perform an update using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(NO_BDEF_DESCRIPTION, NO_BDEF_DISPLAY_NAME,
                    NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_UPDATE_REQUEST));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, NO_BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, NO_BDEF_DISPLAY_NAME, NO_ATTRIBUTES, NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformationTrimParameters()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Create a business object format for the business object definition entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Perform an update using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME)),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(addWhitespace(BDEF_DESCRIPTION_2), addWhitespace(BDEF_DISPLAY_NAME_2),
                    new DescriptiveBusinessObjectFormatUpdateRequest(addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME,
                addWhitespace(BDEF_DESCRIPTION_2), null, BDEF_DISPLAY_NAME_2, NO_ATTRIBUTES,
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformationUpperCaseParameters()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Create a business object format for the business object definition entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Perform an update using upper case input parameters.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    new DescriptiveBusinessObjectFormatUpdateRequest(FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase())));

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2, null,
                BDEF_DISPLAY_NAME_2, NO_ATTRIBUTES, new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformationLowerCaseParameters()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Create a business object format for the business object definition entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Perform an update using lower case input parameters.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    new DescriptiveBusinessObjectFormatUpdateRequest(FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase())));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2, NO_ATTRIBUTES,
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformationBusinessObjectDefinitionNoExists() throws Exception
    {
        // Try to update business object definition descriptive information for a non-existing business object definition.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DESCRIPTION_2,
                    NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_UPDATE_REQUEST));
            fail("Should throw an ObjectNotFoundException when business object definition doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BDEF_NAME, BDEF_NAMESPACE),
                e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformationDescriptiveBusinessObjectFormatNoExists() throws Exception
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Try to update the business object definition descriptive information by specifying a non-existing descriptive business object format.
        try
        {
            businessObjectDefinitionService.updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DESCRIPTION_2,
                    new DescriptiveBusinessObjectFormatUpdateRequest(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION),
                e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformationWithDescriptiveFormatHavingMultipleVersions() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Create and persist two versions of the business object format.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY);
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Perform an update.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionService
            .updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    new DescriptiveBusinessObjectFormatUpdateRequest(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2, NO_ATTRIBUTES,
                new DescriptiveBusinessObjectFormat(FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION), NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles());

        // Retrieve the business object definition.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
            businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles(), businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()),
            NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS), resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionMissingRequiredParameters()
    {
        // Try to get a business object definition instance when object definition name is not specified.
        try
        {
            businessObjectDefinitionService
                .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BLANK_TEXT), NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);
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
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Retrieve the business object definition using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME)),
                NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionUpperCaseParameters()
    {
        // Create and persist a business object definition entity using lower case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), DATA_PROVIDER_NAME.toLowerCase(),
                BDEF_DESCRIPTION.toLowerCase(), BDEF_DISPLAY_NAME.toLowerCase(), businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Retrieve the business object definition using upper case input parameters.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()),
                NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(),
                DATA_PROVIDER_NAME.toLowerCase(), BDEF_DESCRIPTION.toLowerCase(), NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME.toLowerCase(),
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionLowerCaseParameters()
    {
        // Create and persist a business object definition entity using upper case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(),
                BDEF_DESCRIPTION.toUpperCase(), BDEF_DISPLAY_NAME.toUpperCase(), businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Retrieve the business object definition using lower case input parameters.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()),
                NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(),
                DATA_PROVIDER_NAME.toUpperCase(), BDEF_DESCRIPTION.toUpperCase(), NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME.toUpperCase(),
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testBusinessObjectDefinitionNoHtmlInShortDescription()
    {
        // Set up test data.
        setUpTestEntitiesForSearchTesting();

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE_4), INCLUDE_TAG_HIERARCHY))))),
            Sets.newHashSet(FIELD_SHORT_DESCRIPTION));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(1, actualBusinessObjectDefinitions.size());

        for (BusinessObjectDefinition actualBdef : actualBusinessObjectDefinitions)
        {
            assertEquals("Test Description. Value should be <30> value should be <40>", actualBdef.getShortDescription());
            break;
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionNoExists() throws Exception
    {
        // Try to get a non-existing business object definition.
        try
        {
            businessObjectDefinitionService
                .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);
            fail("Should throw an ObjectNotFoundException when business object definition doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BDEF_NAME, NAMESPACE),
                e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionNoSampleDataFiles() throws Exception
    {
        // Create and persist a business object definition entity without sample data files.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_SAMPLE_DATA_FILES);

        // Retrieve the business object definition.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionValidateSampleDataFilesOrder() throws Exception
    {
        // Create and persist a business object definition entity with sample data files created out of order.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), Arrays
                    .asList(new SampleDataFile(DIRECTORY_PATH_2, FILE_NAME_2), new SampleDataFile(DIRECTORY_PATH, FILE_NAME_2),
                        new SampleDataFile(DIRECTORY_PATH, FILE_NAME)));

        // Retrieve the business object definition.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionService
            .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME), NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
            Arrays.asList(new SampleDataFile(DIRECTORY_PATH, FILE_NAME), new SampleDataFile(DIRECTORY_PATH, FILE_NAME_2),
                new SampleDataFile(DIRECTORY_PATH_2, FILE_NAME_2)), businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()),
            NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS), resultBusinessObjectDefinition);
    }

    /**
     * This method is to get coverage for the business object definition service method that starts a new transaction.
     */
    @Test
    public void testGetBusinessObjectDefinitionNewTransaction() throws Exception
    {
        try
        {
            businessObjectDefinitionServiceImpl.createBusinessObjectDefinition(new BusinessObjectDefinitionCreateRequest());
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDefinitionServiceImpl.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey());
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDefinitionServiceImpl
                .getBusinessObjectDefinition(new BusinessObjectDefinitionKey(), NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDefinitionServiceImpl.updateBusinessObjectDefinition(new BusinessObjectDefinitionKey(), new BusinessObjectDefinitionUpdateRequest());
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDefinitionServiceImpl.updateBusinessObjectDefinitionDescriptiveInformation(new BusinessObjectDefinitionKey(),
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest());
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        businessObjectDefinitionServiceImpl.updateSearchIndexDocumentBusinessObjectDefinition(
            new SearchIndexUpdateDto(SearchIndexUpdateDto.MESSAGE_TYPE_BUSINESS_OBJECT_DEFINITION_UPDATE, new ArrayList<>(),
                SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_CREATE));
    }

    @Test
    public void testGetBusinessObjectDefinitions() throws Exception
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : businessObjectDefinitionDaoTestHelper.getTestBusinessObjectDefinitionKeys())
        {
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                    NO_ATTRIBUTES);
        }

        // Retrieve a list of business object definition keys for the specified namespace.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionService.getBusinessObjectDefinitions(NAMESPACE);

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDaoTestHelper.getExpectedBusinessObjectDefinitionKeysForNamespace(), resultKeys.getBusinessObjectDefinitionKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionsNoParameters() throws Exception
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : businessObjectDefinitionDaoTestHelper.getTestBusinessObjectDefinitionKeys())
        {
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                    NO_ATTRIBUTES);
        }

        // Retrieve a list of business object definition keys for the specified namespace.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionService.getBusinessObjectDefinitions();

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
                .createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                    NO_ATTRIBUTES);
        }

        // Retrieve a list of business object definition keys for the specified namespace using namespace value with leading and trailing empty spaces.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionService.getBusinessObjectDefinitions(addWhitespace(NAMESPACE));

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDaoTestHelper.getExpectedBusinessObjectDefinitionKeysForNamespace(), resultKeys.getBusinessObjectDefinitionKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionsUpperCaseParameters()
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : businessObjectDefinitionDaoTestHelper.getTestBusinessObjectDefinitionKeys())
        {
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                    NO_ATTRIBUTES);
        }

        // Retrieve a list of business object definition keys for the specified namespace using upper case namespace value.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionService.getBusinessObjectDefinitions(NAMESPACE.toUpperCase());

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDaoTestHelper.getExpectedBusinessObjectDefinitionKeysForNamespace(), resultKeys.getBusinessObjectDefinitionKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionsLowerCaseParameters()
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : businessObjectDefinitionDaoTestHelper.getTestBusinessObjectDefinitionKeys())
        {
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                    NO_ATTRIBUTES);
        }

        // Retrieve a list of business object definition keys for the specified namespace using lower case namespace value.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionService.getBusinessObjectDefinitions(NAMESPACE.toLowerCase());

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDaoTestHelper.getExpectedBusinessObjectDefinitionKeysForNamespace(), resultKeys.getBusinessObjectDefinitionKeys());
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
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Validate that this business object definition exists.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        assertNotNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));

        // Delete this business object definition.
        BusinessObjectDefinition deletedBusinessObjectDefinition =
            businessObjectDefinitionService.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
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
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Validate that this business object definition exists.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        assertNotNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));

        // Delete this business object definition using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinition deletedBusinessObjectDefinition =
            businessObjectDefinitionService.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
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
                BDEF_DESCRIPTION.toLowerCase(), BDEF_DISPLAY_NAME.toLowerCase(), businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Validate that this business object definition exists.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase());
        assertNotNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));

        // Delete this business object definition using upper case input parameters.
        BusinessObjectDefinition deletedBusinessObjectDefinition =
            businessObjectDefinitionService.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(),
                DATA_PROVIDER_NAME.toLowerCase(), BDEF_DESCRIPTION.toLowerCase(), NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME.toLowerCase(),
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            deletedBusinessObjectDefinition);

        // Ensure that this business object definition is no longer there.
        assertNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionLowerCaseParameters()
    {
        // Create and persist a business object definition entity using upper case values.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), DATA_PROVIDER_NAME.toUpperCase(),
                BDEF_DESCRIPTION.toUpperCase(), BDEF_DISPLAY_NAME.toUpperCase(), businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Validate that this business object definition exists.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE.toUpperCase(), BDEF_NAME.toLowerCase());
        assertNotNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));

        // Retrieve the business object definition using lower case input parameters.
        BusinessObjectDefinition deletedBusinessObjectDefinition =
            businessObjectDefinitionService.deleteBusinessObjectDefinition(new BusinessObjectDefinitionKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(),
                DATA_PROVIDER_NAME.toUpperCase(), BDEF_DESCRIPTION.toUpperCase(), NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME.toUpperCase(),
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
                businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            deletedBusinessObjectDefinition);

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

    private Set<BusinessObjectDefinition> setUpTestEntitiesForSearchTesting()
    {
        // Create and retrieve a list of business object definition entities.
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities =
            businessObjectDefinitionDaoTestHelper.createExpectedBusinessObjectDefinitionEntities();

        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create a root tag entity for the tag type.
        TagEntity rootTagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create three children for the root tag.
        TagEntity childTagEntity1 =
            tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, rootTagEntity);
        TagEntity childTagEntity2 =
            tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_3, TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, rootTagEntity);
        TagEntity childTagEntity3 =
            tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_4, TAG_DISPLAY_NAME_4, TAG_SEARCH_SCORE_MULTIPLIER_4, TAG_DESCRIPTION_4, rootTagEntity);

        // Create association between business object definition and tag.
        businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntities.get(0), childTagEntity1);
        businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntities.get(1), childTagEntity2);
        businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntities.get(2), childTagEntity3);

        // Convert the entities into business object definition objects for easy comparison later.
        return businessObjectDefinitionEntities.stream()
            .map(businessObjectDefinitionServiceTestHelper::createBusinessObjectDefinitionFromEntityForSearchTesting).collect(Collectors.toSet());
    }

    @Test
    public void testSearchBusinessObjectDefinitionValidParams()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = setUpTestEntitiesForSearchTesting();

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
            Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(expectedBusinessObjectDefinitions, actualBusinessObjectDefinitions);
    }

    @Test
    public void testSearchBusinessObjectDefinitionEmptyFilter()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = setUpTestEntitiesForSearchTesting();

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService
            .searchBusinessObjectDefinitions(new BusinessObjectDefinitionSearchRequest(),
                Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(expectedBusinessObjectDefinitions, actualBusinessObjectDefinitions);
    }

    @Test
    public void testSearchBusinessObjectDefinitionInvalidParams()
    {
        // Try to search business object definition when empty search filter is specified.
        try
        {
            businessObjectDefinitionService
                .searchBusinessObjectDefinitions(new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter())),
                    NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exactly one business object definition search key must be specified.", e.getMessage());
        }


        // Try to search business object definition when there are more than one business object definition search filter is specified.
        try
        {
            businessObjectDefinitionService.searchBusinessObjectDefinitions(new BusinessObjectDefinitionSearchRequest(
                Arrays.asList(new BusinessObjectDefinitionSearchFilter(), new BusinessObjectDefinitionSearchFilter())), NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exactly one business object definition search filter must be specified.", e.getMessage());
        }

        // Try to search business object definition when there are more than one business object definition search key is specified.
        try
        {
            businessObjectDefinitionService.searchBusinessObjectDefinitions(new BusinessObjectDefinitionSearchRequest(Arrays.asList(
                new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                    Arrays.asList(new BusinessObjectDefinitionSearchKey(), new BusinessObjectDefinitionSearchKey())))), NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exactly one business object definition search key must be specified.", e.getMessage());
        }

        // Try to search business object definitions for a non-existing tag type.
        try
        {
            businessObjectDefinitionService.searchBusinessObjectDefinitions(new BusinessObjectDefinitionSearchRequest(Arrays.asList(
                new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                    Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey("I_DO_NOT_EXIST", "I_DO_NO_EXIST"), NOT_INCLUDE_TAG_HIERARCHY))))),
                NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals("Tag with code \"I_DO_NO_EXIST\" doesn't exist for tag type \"I_DO_NOT_EXIST\".", e.getMessage());
        }

        // Try to retrieve the actual business object definition objects using an un-supported search field.
        try
        {
            businessObjectDefinitionService.searchBusinessObjectDefinitions(new BusinessObjectDefinitionSearchRequest(Arrays.asList(
                new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                    Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
                Sets.newHashSet("I_DO_NOT_EXIST"));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Search response field \"i_do_not_exist\" is not supported.", e.getMessage());
        }

        // Try to retrieve the actual business object definition object using an un-supported search field in the mix of supported search fields.
        try
        {
            businessObjectDefinitionService.searchBusinessObjectDefinitions(new BusinessObjectDefinitionSearchRequest(Arrays.asList(
                new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                    Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
                Sets.newHashSet(FIELD_DISPLAY_NAME, "I_DO_NOT_EXIST", FIELD_DATA_PROVIDER_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Search response field \"i_do_not_exist\" is not supported.", e.getMessage());
        }
    }

    @Test
    public void testSearchBusinessObjectDefinitionLowerCaseParams()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = setUpTestEntitiesForSearchTesting();

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
            Sets.newHashSet(FIELD_DATA_PROVIDER_NAME.toLowerCase(), FIELD_DISPLAY_NAME.toLowerCase(), FIELD_SHORT_DESCRIPTION.toLowerCase()));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(expectedBusinessObjectDefinitions, actualBusinessObjectDefinitions);
    }

    @Test
    public void testSearchBusinessObjectDefinitionsUpperCaseParams()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = setUpTestEntitiesForSearchTesting();

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
            Sets.newHashSet(FIELD_DATA_PROVIDER_NAME.toUpperCase(), FIELD_DISPLAY_NAME.toUpperCase(), FIELD_SHORT_DESCRIPTION.toUpperCase()));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(expectedBusinessObjectDefinitions, actualBusinessObjectDefinitions);
    }

    @Test
    public void testSearchBusinessObjectDefinitionsMissingOptionalParams()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = setUpTestEntitiesForSearchTesting();

        // Remove fields which are not expected from the expected business object definition objects.
        for (BusinessObjectDefinition businessObjectDefinition : expectedBusinessObjectDefinitions)
        {
            businessObjectDefinition.setDisplayName(null);
            businessObjectDefinition.setShortDescription(null);
            businessObjectDefinition.setDataProviderName(null);
        }

        // Retrieve the actual business object definition objects from the search response.
        // Fields are required to have a blank text value because that is set by default in the controller.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))), Sets.newHashSet(BLANK_TEXT));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(actualBusinessObjectDefinitions, expectedBusinessObjectDefinitions);
    }

    @Test
    public void testSearchBusinessObjectDefinitionsOnlyDisplayName()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = setUpTestEntitiesForSearchTesting();

        // Remove fields which are not expected from the expected business object definition objects.
        for (BusinessObjectDefinition businessObjectDefinition : expectedBusinessObjectDefinitions)
        {
            businessObjectDefinition.setShortDescription(null);
            businessObjectDefinition.setDataProviderName(null);
        }

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
            Sets.newHashSet(FIELD_DISPLAY_NAME));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(actualBusinessObjectDefinitions, expectedBusinessObjectDefinitions);
    }

    @Test
    public void testSearchBusinessObjectDefinitionsOnlyShortDescription()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = new HashSet<>(setUpTestEntitiesForSearchTesting());

        // Remove fields which are not expected from the expected business object definition objects.
        for (BusinessObjectDefinition businessObjectDefinition : expectedBusinessObjectDefinitions)
        {
            businessObjectDefinition.setDisplayName(null);
            businessObjectDefinition.setDataProviderName(null);
        }

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
            Sets.newHashSet(FIELD_SHORT_DESCRIPTION));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertTrue(CollectionUtils.isEqualCollection(expectedBusinessObjectDefinitions, actualBusinessObjectDefinitions));
    }

    @Test
    public void testSearchBusinessObjectDefinitionsOnlyDataProviderName()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = setUpTestEntitiesForSearchTesting();

        // Remove fields which are not expected from the expected business object definition objects.
        for (BusinessObjectDefinition businessObjectDefinition : expectedBusinessObjectDefinitions)
        {
            businessObjectDefinition.setDisplayName(null);
            businessObjectDefinition.setShortDescription(null);
        }

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
            Sets.newHashSet(FIELD_DATA_PROVIDER_NAME));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(actualBusinessObjectDefinitions, expectedBusinessObjectDefinitions);
    }

    @Test
    public void testSearchBusinessObjectDefinitionsOnlyDataProviderNameAndDisplayName()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = setUpTestEntitiesForSearchTesting();

        // Remove fields which are not expected from the expected business object definition objects.
        for (BusinessObjectDefinition businessObjectDefinition : expectedBusinessObjectDefinitions)
        {
            businessObjectDefinition.setShortDescription(null);
        }

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
            Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(actualBusinessObjectDefinitions, expectedBusinessObjectDefinitions);
    }

    @Test
    public void testSearchBusinessObjectDefinitionsOnlyShortDescriptionAndDataProviderName()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = setUpTestEntitiesForSearchTesting();

        // Remove fields which are not expected from the expected business object definition objects.
        for (BusinessObjectDefinition businessObjectDefinition : expectedBusinessObjectDefinitions)
        {
            businessObjectDefinition.setDisplayName(null);
        }

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
            Sets.newHashSet(FIELD_SHORT_DESCRIPTION, FIELD_DATA_PROVIDER_NAME));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(actualBusinessObjectDefinitions, expectedBusinessObjectDefinitions);
    }

    @Test
    public void testSearchBusinessObjectDefinitionsOnlyShortDescriptionAndDisplayName()
    {
        // Set up test data.
        Set<BusinessObjectDefinition> expectedBusinessObjectDefinitions = setUpTestEntitiesForSearchTesting();

        // Remove fields which are not expected from the expected business object definition objects.
        for (BusinessObjectDefinition businessObjectDefinition : expectedBusinessObjectDefinitions)
        {
            businessObjectDefinition.setDataProviderName(null);
        }

        // Retrieve the actual business object definition objects from the search response.
        BusinessObjectDefinitionSearchResponse searchResponse = businessObjectDefinitionService.searchBusinessObjectDefinitions(
            new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(NO_EXCLUSION_SEARCH_FILTER,
                Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))),
            Sets.newHashSet(FIELD_SHORT_DESCRIPTION, FIELD_DISPLAY_NAME));
        Set<BusinessObjectDefinition> actualBusinessObjectDefinitions = new HashSet<>(searchResponse.getBusinessObjectDefinitions());

        assertEquals(actualBusinessObjectDefinitions, expectedBusinessObjectDefinitions);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionSampleFiles()
    {
        // Create and persist database entities required for testing.
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        storageDaoTestHelper.createStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE,
            Arrays.asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME)));

        // Create a business object definition.
        BusinessObjectDefinitionCreateRequest request =
            new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        businessObjectDefinitionService.createBusinessObjectDefinition(request);

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        String fileName = "testfile.csv";
        long fileSize = 1820L;
        String path = NAMESPACE + "/" + BDEF_NAME + "/";
        BusinessObjectDefinitionSampleFileUpdateDto sampleFileUpdateDto = new BusinessObjectDefinitionSampleFileUpdateDto(path, fileName, fileSize);
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        businessObjectDefinitionServiceImpl.updateBusinessObjectDefinitionEntitySampleFile(businessObjectDefinitionKey, sampleFileUpdateDto);

        BusinessObjectDefinition updatedBusinessObjectDefinition =
            businessObjectDefinitionService.getBusinessObjectDefinition(businessObjectDefinitionKey, NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        List<SampleDataFile> sampleDataFiles = Arrays.asList(new SampleDataFile(NAMESPACE + "/" + BDEF_NAME + "/", fileName));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(updatedBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                sampleDataFiles, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionSampleFilesAddFile()
    {
        // Create and persist database entities required for testing.
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        storageDaoTestHelper.createStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE,
            Arrays.asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME)));

        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles());

        String fileName = "testfile.csv";
        long fileSize = 1820L;
        String path = NAMESPACE + "/" + BDEF_NAME + "/";
        BusinessObjectDefinitionSampleFileUpdateDto sampleFileUpdateDto = new BusinessObjectDefinitionSampleFileUpdateDto(path, fileName, fileSize);

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        businessObjectDefinitionServiceImpl.updateBusinessObjectDefinitionEntitySampleFile(businessObjectDefinitionKey, sampleFileUpdateDto);

        BusinessObjectDefinition updatedBusinessObjectDefinition =
            businessObjectDefinitionService.getBusinessObjectDefinition(businessObjectDefinitionKey, NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);
        List<SampleDataFile> updatedSampleDataFileList = businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles();
        updatedSampleDataFileList.add(new SampleDataFile(NAMESPACE + "/" + BDEF_NAME + "/", fileName));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(updatedBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                updatedSampleDataFileList, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionSampleFilesDifferentFileSize()
    {
        // Create and persist database entities required for testing.
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        storageDaoTestHelper.createStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE,
            Arrays.asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME)));

        // Create a business object definition.
        BusinessObjectDefinitionCreateRequest request =
            new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        businessObjectDefinitionService.createBusinessObjectDefinition(request);

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        String fileName = "testfile.csv";
        long fileSize = 1820L;
        String path = NAMESPACE + "/" + BDEF_NAME + "/";
        BusinessObjectDefinitionSampleFileUpdateDto sampleFileUpdateDto = new BusinessObjectDefinitionSampleFileUpdateDto(path, fileName, fileSize);

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        businessObjectDefinitionServiceImpl.updateBusinessObjectDefinitionEntitySampleFile(businessObjectDefinitionKey, sampleFileUpdateDto);

        fileSize = 1024L;
        sampleFileUpdateDto.setFileSize(fileSize);
        businessObjectDefinitionServiceImpl.updateBusinessObjectDefinitionEntitySampleFile(businessObjectDefinitionKey, sampleFileUpdateDto);

        BusinessObjectDefinition updatedBusinessObjectDefinition =
            businessObjectDefinitionService.getBusinessObjectDefinition(businessObjectDefinitionKey, NOT_INCLUDE_BUSINESS_OBJECT_DEFINITION_UPDATE_HISTORY);

        List<SampleDataFile> sampleDataFiles = Arrays.asList(new SampleDataFile(NAMESPACE + "/" + BDEF_NAME + "/", fileName));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(updatedBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
                sampleDataFiles, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()), NO_BUSINESS_OBJECT_DEFINITION_CHANGE_EVENTS),
            updatedBusinessObjectDefinition);

        // Validate the file size is updated.
        assertEquals(businessObjectDefinitionEntity.getSampleDataFiles().iterator().next().getFileSizeBytes().longValue(), fileSize);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionSampleFilesMissingFileName()
    {
        // Create and persist database entities required for testing.
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        storageDaoTestHelper.createStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE,
            Arrays.asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME)));

        // Create a business object definition.
        BusinessObjectDefinitionCreateRequest request =
            new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        businessObjectDefinitionService.createBusinessObjectDefinition(request);
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        String fileName = "";
        long fileSize = 1820L;
        String path = NAMESPACE + "/" + BDEF_NAME + "/";
        BusinessObjectDefinitionSampleFileUpdateDto sampleFileUpdateDto = new BusinessObjectDefinitionSampleFileUpdateDto(path, fileName, fileSize);

        try
        {
            businessObjectDefinitionServiceImpl.updateBusinessObjectDefinitionEntitySampleFile(businessObjectDefinitionKey, sampleFileUpdateDto);
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(ex.getMessage(), "A file name must be specified.");
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionSampleFilesBusinessObjectDefinitionNoExists()
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        String fileName = "testfile.csv";
        long fileSize = 1820L;
        String path = NAMESPACE + "/" + BDEF_NAME + "/";
        BusinessObjectDefinitionSampleFileUpdateDto sampleFileUpdateDto = new BusinessObjectDefinitionSampleFileUpdateDto(path, fileName, fileSize);

        try
        {
            businessObjectDefinitionServiceImpl.updateBusinessObjectDefinitionEntitySampleFile(businessObjectDefinitionKey, sampleFileUpdateDto);
        }
        catch (ObjectNotFoundException ex)
        {
            assertEquals(ex.getMessage(),
                String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BDEF_NAME, NAMESPACE));
        }
    }
}
