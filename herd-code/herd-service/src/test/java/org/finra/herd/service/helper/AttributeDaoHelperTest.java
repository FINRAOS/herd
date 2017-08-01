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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IterableUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.service.AbstractServiceTest;

public class AttributeDaoHelperTest extends AbstractServiceTest
{
    @InjectMocks
    private AttributeDaoHelper attributeDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributesAttributeAdded()
    {
        // Create a list of attributes.
        List<Attribute> attributes = Arrays.asList(new Attribute(ATTRIBUTE_NAME, ATTRIBUTE_VALUE));

        // Create a business object data entity without attributes.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setAttributes(new ArrayList<>());

        // Call the method under test.
        attributeDaoHelper.updateBusinessObjectDataAttributes(businessObjectDataEntity, attributes);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(1, CollectionUtils.size(businessObjectDataEntity.getAttributes()));
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = IterableUtils.get(businessObjectDataEntity.getAttributes(), 0);
        assertEquals(businessObjectDataEntity, businessObjectDataAttributeEntity.getBusinessObjectData());
        assertEquals(ATTRIBUTE_NAME, businessObjectDataAttributeEntity.getName());
        assertEquals(ATTRIBUTE_VALUE, businessObjectDataAttributeEntity.getValue());
    }

    @Test
    public void testUpdateBusinessObjectDataAttributesAttributeDeleted()
    {
        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity.setName(ATTRIBUTE_NAME);
        businessObjectDataAttributeEntity.setValue(ATTRIBUTE_VALUE);

        // Create a business object data entity that contains one attribute entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        List<BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntities = new ArrayList<>();
        businessObjectDataEntity.setAttributes(businessObjectDataAttributeEntities);
        businessObjectDataAttributeEntities.add(businessObjectDataAttributeEntity);

        // Call the method under test.
        attributeDaoHelper.updateBusinessObjectDataAttributes(businessObjectDataEntity, NO_ATTRIBUTES);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(0, CollectionUtils.size(businessObjectDataEntity.getAttributes()));
    }

    @Test
    public void testUpdateBusinessObjectDataAttributesAttributeValueNotUpdated()
    {
        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity.setName(ATTRIBUTE_NAME);
        businessObjectDataAttributeEntity.setValue(ATTRIBUTE_VALUE);

        // Create a business object data entity that contains one attribute entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        List<BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntities = new ArrayList<>();
        businessObjectDataEntity.setAttributes(businessObjectDataAttributeEntities);
        businessObjectDataAttributeEntities.add(businessObjectDataAttributeEntity);

        // Call the method under test.
        attributeDaoHelper.updateBusinessObjectDataAttributes(businessObjectDataEntity, Arrays.asList(new Attribute(ATTRIBUTE_NAME, ATTRIBUTE_VALUE)));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(1, CollectionUtils.size(businessObjectDataEntity.getAttributes()));
        BusinessObjectDataAttributeEntity result = IterableUtils.get(businessObjectDataEntity.getAttributes(), 0);
        assertEquals(ATTRIBUTE_NAME, result.getName());
        assertEquals(ATTRIBUTE_VALUE, result.getValue());
    }

    @Test
    public void testUpdateBusinessObjectDataAttributesAttributeValueUpdated()
    {
        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity.setName(ATTRIBUTE_NAME);
        businessObjectDataAttributeEntity.setValue(ATTRIBUTE_VALUE);

        // Create a business object data entity that contains one attribute entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        List<BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntities = new ArrayList<>();
        businessObjectDataEntity.setAttributes(businessObjectDataAttributeEntities);
        businessObjectDataAttributeEntities.add(businessObjectDataAttributeEntity);

        // Call the method under test.
        attributeDaoHelper.updateBusinessObjectDataAttributes(businessObjectDataEntity, Arrays.asList(new Attribute(ATTRIBUTE_NAME, ATTRIBUTE_VALUE_2)));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(1, CollectionUtils.size(businessObjectDataEntity.getAttributes()));
        BusinessObjectDataAttributeEntity result = IterableUtils.get(businessObjectDataEntity.getAttributes(), 0);
        assertEquals(ATTRIBUTE_NAME, result.getName());
        assertEquals(ATTRIBUTE_VALUE_2, result.getValue());
    }

    @Test
    public void testUpdateBusinessObjectDataAttributesAttributeValueUpdatedAttributeNamesEqualIgnoreCase()
    {
        // Create a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity.setName(ATTRIBUTE_NAME.toUpperCase());
        businessObjectDataAttributeEntity.setValue(ATTRIBUTE_VALUE);

        // Create a business object data entity that contains one attribute entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        List<BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntities = new ArrayList<>();
        businessObjectDataEntity.setAttributes(businessObjectDataAttributeEntities);
        businessObjectDataAttributeEntities.add(businessObjectDataAttributeEntity);

        // Call the method under test.
        attributeDaoHelper
            .updateBusinessObjectDataAttributes(businessObjectDataEntity, Arrays.asList(new Attribute(ATTRIBUTE_NAME.toLowerCase(), ATTRIBUTE_VALUE_2)));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(1, CollectionUtils.size(businessObjectDataEntity.getAttributes()));
        BusinessObjectDataAttributeEntity result = IterableUtils.get(businessObjectDataEntity.getAttributes(), 0);
        assertEquals(ATTRIBUTE_NAME.toUpperCase(), result.getName());
        assertEquals(ATTRIBUTE_VALUE_2, result.getValue());
    }

    @Test
    public void testUpdateBusinessObjectDataAttributesDuplicateAttributes()
    {
        // Create two business object data attribute entities that use the same attribute name (case-insensitive).
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntityA = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntityA.setName(ATTRIBUTE_NAME.toUpperCase());
        businessObjectDataAttributeEntityA.setValue(ATTRIBUTE_VALUE);
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntityB = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntityB.setName(ATTRIBUTE_NAME.toLowerCase());
        businessObjectDataAttributeEntityB.setValue(ATTRIBUTE_VALUE_2);

        // Create a business object data entity that contains duplicate attributes (case-insensitive).
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        List<BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntities = new ArrayList<>();
        businessObjectDataEntity.setAttributes(businessObjectDataAttributeEntities);
        businessObjectDataAttributeEntities.add(businessObjectDataAttributeEntityA);
        businessObjectDataAttributeEntities.add(businessObjectDataAttributeEntityB);

        // Mock the external calls.
        when(businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);

        // Try to call the method under test.
        try
        {
            attributeDaoHelper.updateBusinessObjectDataAttributes(businessObjectDataEntity, NO_ATTRIBUTES);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("Found duplicate attribute with name \"%s\" for business object data. Business object data: {%s}", ATTRIBUTE_NAME.toLowerCase(),
                    BUSINESS_OBJECT_DATA_KEY_AS_STRING), e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectDataHelper).businessObjectDataEntityAltKeyToString(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAttributesAgainstBusinessObjectDataAttributeDefinitionsAttributeNamesEqualIgnoreCase()
    {
        // Create a business object data attribute definition for a required attribute with attribute name in upper case.
        BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
        businessObjectDataAttributeDefinitionEntity.setName(ATTRIBUTE_NAME.toUpperCase());

        // Call the method under test with the attribute name specified in lowercase.
        attributeDaoHelper
            .validateAttributesAgainstBusinessObjectDataAttributeDefinitions(Arrays.asList(new Attribute(ATTRIBUTE_NAME.toLowerCase(), ATTRIBUTE_VALUE)),
                Arrays.asList(businessObjectDataAttributeDefinitionEntity));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAttributesAgainstBusinessObjectDataAttributeDefinitionsRequiredAttributeHasBlankValue()
    {
        // Create a business object data attribute definition for a required attribute.
        BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
        businessObjectDataAttributeDefinitionEntity.setName(ATTRIBUTE_NAME);

        // Try to call the method under test.
        try
        {
            attributeDaoHelper.validateAttributesAgainstBusinessObjectDataAttributeDefinitions(Arrays.asList(new Attribute(ATTRIBUTE_NAME, BLANK_TEXT)),
                Arrays.asList(businessObjectDataAttributeDefinitionEntity));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("The business object format has a required attribute \"%s\" which was not specified or has a value which is blank.", ATTRIBUTE_NAME),
                e.getMessage());
        }

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateAttributesAgainstBusinessObjectDataAttributeDefinitionsRequiredAttributeIsMissing()
    {
        // Create a business object data attribute definition for a required attribute.
        BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
        businessObjectDataAttributeDefinitionEntity.setName(ATTRIBUTE_NAME);

        // Try to call the method under test.
        try
        {
            attributeDaoHelper
                .validateAttributesAgainstBusinessObjectDataAttributeDefinitions(NO_ATTRIBUTES, Arrays.asList(businessObjectDataAttributeDefinitionEntity));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("The business object format has a required attribute \"%s\" which was not specified or has a value which is blank.", ATTRIBUTE_NAME),
                e.getMessage());
        }

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataHelper);
    }
}
