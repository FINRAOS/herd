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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the helper for business object definition related operations.
 */
public class BusinessObjectDefinitionHelperTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @InjectMocks
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testBusinessObjectDefinitionKeyToString()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Get a business object definition key as a string.
        String response = businessObjectDefinitionHelper.businessObjectDefinitionKeyToString(businessObjectDefinitionKey);

        // Validate the returned object.
        assertEquals(String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\"", BDEF_NAMESPACE, BDEF_NAME), response);
    }

    @Test
    public void testExecuteFunctionForBusinessObjectDefinitionEntities()
    {
        // Create a list of business object definition entities.
        final List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = Collections.unmodifiableList(Arrays.asList(
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()), businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                    businessObjectDefinitionServiceTestHelper.getNewAttributes2())));

        businessObjectDefinitionEntities.forEach(entity ->
        {
            entity.setDescriptiveBusinessObjectFormat(new BusinessObjectFormatEntity());
            entity.getDescriptiveBusinessObjectFormat().setSchemaColumns(new ArrayList<>());

            entity.setSubjectMatterExperts(new ArrayList<>());
        });

        // Mock the external calls.
        when(jsonHelper.objectToJson(any())).thenReturn(JSON_STRING);

        // Execute a function for all business object definition entities.
        businessObjectDefinitionHelper
            .executeFunctionForBusinessObjectDefinitionEntities(SEARCH_INDEX_NAME, businessObjectDefinitionEntities,
                (indexName, id, json) ->
                {
                });

        // Verify the external calls.
        verify(jsonHelper, times(businessObjectDefinitionEntities.size())).objectToJson(any());
        verifyNoMoreInteractions(alternateKeyHelper, jsonHelper);
    }

    @Test
    public void testExecuteFunctionForBusinessObjectDefinitionEntitiesJsonParseException()
    {
        // Create a list of business object definition entities.
        final List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = Collections.unmodifiableList(Arrays.asList(
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()), businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                    businessObjectDefinitionServiceTestHelper.getNewAttributes2())));

        businessObjectDefinitionEntities.forEach(entity ->
        {
            entity.setDescriptiveBusinessObjectFormat(new BusinessObjectFormatEntity());
            entity.getDescriptiveBusinessObjectFormat().setSchemaColumns(new ArrayList<>());

            entity.setSubjectMatterExperts(new ArrayList<>());
        });

        // Mock the external calls.
        when(jsonHelper.objectToJson(any()))
            .thenThrow(new IllegalStateException(new JsonParseException("Failed to Parse", new JsonLocation("SRC", 100L, 1, 2))));

        // Execute a function for all business object definition entities.
        businessObjectDefinitionHelper
            .executeFunctionForBusinessObjectDefinitionEntities(SEARCH_INDEX_NAME, businessObjectDefinitionEntities,
                (indexName, id, json) ->
                {
                });

        // Verify the external calls.
        verify(jsonHelper, times(businessObjectDefinitionEntities.size())).objectToJson(any());
        verifyNoMoreInteractions(alternateKeyHelper, jsonHelper);
    }

    @Test
    public void testGetBusinessObjectDefinitionKey()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Get a business object definition key from the column key.
        BusinessObjectDefinitionKey response = businessObjectDefinitionHelper.getBusinessObjectDefinitionKey(businessObjectDefinitionColumnKey);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), response);
    }

    @Test
    public void testValidateBusinessObjectDefinitionKey()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("namespace", BDEF_NAMESPACE)).thenReturn(BDEF_NAMESPACE);
        when(alternateKeyHelper.validateStringParameter("business object definition name", BDEF_NAME)).thenReturn(BDEF_NAME);

        // Validate and trim a business object definition key.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", BDEF_NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("business object definition name", BDEF_NAME);
        verifyNoMoreInteractions(alternateKeyHelper, jsonHelper);

        // Validate the business object definition key.
        assertEquals(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), businessObjectDefinitionKey);
    }

    @Test
    public void testValidateBusinessObjectDefinitionKeyBusinessObjectDefinitionKeyIsNull()
    {
        // Try to validate a null business object definition key.
        try
        {
            businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition key must be specified.", e.getMessage());
        }
    }

    @Test
    public void testProcessTagSearchScoreMultiplier()
    {

        // Create a business object definition entity
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2());

        // Create two tag entities
        TagEntity tagEntity1 = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, null);
        TagEntity tagEntity2 = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER_NULL, TAG_DESCRIPTION, null);

        // Assocaite tag entities with business object definition entity
        List<BusinessObjectDefinitionTagEntity> businessObjectDefinitionTagEntities = Arrays
            .asList(businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntity, tagEntity1),
                businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntity, tagEntity2));
        businessObjectDefinitionEntity.setBusinessObjectDefinitionTags(businessObjectDefinitionTagEntities);

        // Call the method under test
        businessObjectDefinitionHelper.processTagSearchScoreMultiplier(businessObjectDefinitionEntity);

        // Validate the result
        assertEquals(businessObjectDefinitionEntity.getTagSearchScoreMultiplier(), TAG_SEARCH_SCORE_MULTIPLIER.setScale(3, RoundingMode.HALF_UP));
    }

    @Test
    public void testProcessTagSearchScoreMultiplierTagsEmpty()
    {

        // Create a business object definition entity
        final BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes2());

        // Associate business object definition entity with no tag entities
        List<BusinessObjectDefinitionTagEntity> businessObjectDefinitionTagEntities = new ArrayList<>();
        businessObjectDefinitionEntity.setBusinessObjectDefinitionTags(businessObjectDefinitionTagEntities);

        // Call the method under test
        businessObjectDefinitionHelper.processTagSearchScoreMultiplier(businessObjectDefinitionEntity);

        // Validate the result
        assertEquals(businessObjectDefinitionEntity.getTagSearchScoreMultiplier(), BigDecimal.ONE.setScale(3, RoundingMode.HALF_UP));
    }
}
