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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptiveInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionUpdateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;

/**
 * This class tests various functionality within the business object definition REST controller.
 */
public class BusinessObjectDefinitionRestControllerTest extends AbstractRestTest
{
    // Constant to hold the data provider name option for the business object definition search
    public static final String FIELD_DATA_PROVIDER_NAME = "dataProviderName";

    // Constant to hold the short description option for the business object definition search
    public static final String FIELD_SHORT_DESCRIPTION = "shortDescription";

    // Constant to hold the display name option for the business object definition search
    public static final String FIELD_DISPLAY_NAME = "displayName";

    @Test
    public void testCreateBusinessObjectDefinition() throws Exception
    {
        // Create and persist database entities required for testing.
        businessObjectDefinitionServiceTestHelper.createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition.
        BusinessObjectDefinitionCreateRequest request =
            new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionRestController.createBusinessObjectDefinition(request);

        // Get the business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));
        assertNotNull(businessObjectDefinitionEntity);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(resultBusinessObjectDefinition.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
            NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
            HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn())), resultBusinessObjectDefinition);

        // Validate that the newly created entity uses system username for the relative auditable fields.
        assertEquals(resultBusinessObjectDefinition.getId(), businessObjectDefinitionEntity.getId());
        assertEquals(HerdDaoSecurityHelper.SYSTEM_USER, businessObjectDefinitionEntity.getCreatedBy());
        assertEquals(HerdDaoSecurityHelper.SYSTEM_USER, businessObjectDefinitionEntity.getUpdatedBy());
    }

    @Test
    public void testUpdateBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Perform an update by changing the description and updating the attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionRestController.updateBusinessObjectDefinition(NAMESPACE, BDEF_NAME,
            new BusinessObjectDefinitionUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2, businessObjectDefinitionServiceTestHelper.getNewAttributes2()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2, businessObjectDefinitionServiceTestHelper.getNewAttributes2(),
            NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn())),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionMissingOptionalParametersPassedAsWhitespace()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Perform an update without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionRestController.updateBusinessObjectDefinition(NAMESPACE, BDEF_NAME,
            new BusinessObjectDefinitionUpdateRequest(BLANK_TEXT, BLANK_TEXT,
                Collections.singletonList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BLANK_TEXT,
            NO_BDEF_SHORT_DESCRIPTION, EMPTY_STRING, Collections.singletonList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT)),
            NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn())),
            updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionMissingOptionalParametersPassedAsNulls()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Perform an update without specifying any of the optional parameters (passing null values).
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionRestController.updateBusinessObjectDefinition(NAMESPACE, BDEF_NAME,
            new BusinessObjectDefinitionUpdateRequest(null, null, Collections.singletonList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, null, null, null,
            Collections.singletonList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null)), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
            businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
            HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn())), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptiveInformation() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);

        // Perform an update by changing the description and updating the attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionRestController
            .updateBusinessObjectDefinitionDescriptiveInformation(NAMESPACE, BDEF_NAME,
                new BusinessObjectDefinitionDescriptiveInformationUpdateRequest(BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME_2,
                    NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_UPDATE_REQUEST));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME_2, NO_ATTRIBUTES, NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT, NO_SAMPLE_DATA_FILES,
            businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
            HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn())), updatedBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles());

        // Retrieve the business object definition.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionRestController.getBusinessObjectDefinition(NAMESPACE, BDEF_NAME);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
            businessObjectDefinitionServiceTestHelper.getTestSampleDataFiles(), businessObjectDefinitionEntity.getCreatedBy(),
            businessObjectDefinitionEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn())),
            resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitions() throws Exception
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : businessObjectDefinitionDaoTestHelper.getTestBusinessObjectDefinitionKeys())
        {
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                    NO_BDEF_SHORT_DESCRIPTION, NO_ATTRIBUTES);
        }

        // Retrieve a list of business object definition keys for the specified namespace.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionRestController.getBusinessObjectDefinitions(NAMESPACE);

        // Validate the returned object.
        assertEquals(businessObjectDefinitionDaoTestHelper.getExpectedBusinessObjectDefinitionKeysForNamespace(), resultKeys.getBusinessObjectDefinitionKeys());
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
        BusinessObjectDefinition deletedBusinessObjectDefinition = businessObjectDefinitionRestController.deleteBusinessObjectDefinition(NAMESPACE, BDEF_NAME);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
            NO_BDEF_SHORT_DESCRIPTION, BDEF_DISPLAY_NAME, businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_DESCRIPTIVE_BUSINESS_OBJECT_FORMAT,
            NO_SAMPLE_DATA_FILES, businessObjectDefinitionEntity.getCreatedBy(), businessObjectDefinitionEntity.getUpdatedBy(),
            HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn())), deletedBusinessObjectDefinition);

        // Ensure that this business object definition is no longer there.
        assertNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));
    }

    @Test
    public void testSearchBusinessObjectDefinition()
    {
        // Create and retrieve a list of business object definition entities.
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities =
            businessObjectDefinitionDaoTestHelper.createExpectedBusinessObjectDefinitionEntities();

        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER);

        // Create a root tag entity for the tag type.
        TagEntity rootTagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Create two children for the root tag.
        TagEntity childTagEntity1 = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2, rootTagEntity);
        TagEntity childTagEntity2 = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_3, TAG_DISPLAY_NAME_3, TAG_DESCRIPTION_3, rootTagEntity);

        // Create association between business object definition and tag.
        businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntities.get(0), childTagEntity1);
        businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntities.get(1), childTagEntity2);

        List<BusinessObjectDefinition> actualBusinessObjectDefinitions = new ArrayList<>();
        for (BusinessObjectDefinitionEntity businessObjectDefinitionEntity : businessObjectDefinitionEntities)
        {
            actualBusinessObjectDefinitions.add(
                new BusinessObjectDefinition(null, businessObjectDefinitionEntity.getNamespace().getCode(), businessObjectDefinitionEntity.getName(),
                    businessObjectDefinitionEntity.getDataProvider().getName(), NO_BDEF_DESCRIPTION, businessObjectDefinitionEntity.getDescription(), null,
                    null, null, null, null, null, null));
        }

        // Tests with tag filter.
        BusinessObjectDefinitionSearchResponse businessObjectDefinitionSearchResponse = businessObjectDefinitionRestController
            .searchBusinessObjectDefinitions(Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION),
                new BusinessObjectDefinitionSearchRequest(Arrays.asList(new BusinessObjectDefinitionSearchFilter(
                    Arrays.asList(new BusinessObjectDefinitionSearchKey(new TagKey(TAG_TYPE, TAG_CODE), INCLUDE_TAG_HIERARCHY))))));
        assertEquals(actualBusinessObjectDefinitions, businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions());

        // Tests without tag filter.
        businessObjectDefinitionSearchResponse = businessObjectDefinitionRestController
            .searchBusinessObjectDefinitions(Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION),
                new BusinessObjectDefinitionSearchRequest());
        assertEquals(actualBusinessObjectDefinitions, businessObjectDefinitionSearchResponse.getBusinessObjectDefinitions());
    }
}
