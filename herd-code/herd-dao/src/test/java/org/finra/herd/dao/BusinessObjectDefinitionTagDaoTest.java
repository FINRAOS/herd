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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKey;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity;
import org.finra.herd.model.jpa.TagEntity;

public class BusinessObjectDefinitionTagDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectDefinitionTagByKey()
    {
        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Create and persist a business object definition tag entity.
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity = businessObjectDefinitionTagDaoTestHelper
            .createBusinessObjectDefinitionTagEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Get the business object definition tag.
        assertEquals(businessObjectDefinitionTagEntity, businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));

        // Get business object definition tag by passing all case-insensitive parameters in uppercase.
        assertEquals(businessObjectDefinitionTagEntity, businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()),
                new TagKey(TAG_TYPE.toUpperCase(), TAG_CODE.toUpperCase()))));

        // Get business object definition tag by passing all case-insensitive parameters in lowercase.
        assertEquals(businessObjectDefinitionTagEntity, businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()),
                new TagKey(TAG_TYPE.toLowerCase(), TAG_CODE.toLowerCase()))));

        // Try invalid values for all input parameters.
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey("I_DO_NOT_EXIST", BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE))));
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, "I_DO_NOT_EXIST"), new TagKey(TAG_TYPE, TAG_CODE))));
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey("I_DO_NOT_EXIST", TAG_CODE))));
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, "I_DO_NOT_EXIST"))));
    }

    @Test
    public void testGetBusinessObjectDefinitionTagByKeyDuplicateTags() throws Exception
    {
        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create duplicate business object definition tags.
        businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntity,
            tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE.toUpperCase(), TAG_DISPLAY_NAME, DESCRIPTION));
        businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntity,
            tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE.toLowerCase(), TAG_DISPLAY_NAME, DESCRIPTION));

        // Try to get business object definition tag when business object definition tags are duplicate.
        try
        {
            businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one business object definition tag instance with parameters {namespace=\"%s\", " +
                "businessObjectDefinitionName=\"%s\", tagType=\"%s\", tagCode=\"%s\"}.", BDEF_NAMESPACE, BDEF_NAME, TAG_TYPE, TAG_CODE), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionTagByParentEntities()
    {
        // Create and persist two business object definition entities.
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = Arrays.asList(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), DATA_PROVIDER_NAME, DESCRIPTION),
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE_2, BDEF_NAME_2), DATA_PROVIDER_NAME, DESCRIPTION));

        // Create and persist two tag entities.
        List<TagEntity> tagEntities = Arrays.asList(tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION),
            tagDaoTestHelper.createTagEntity(TAG_TYPE_2, TAG_CODE_2, TAG_DISPLAY_NAME, TAG_DESCRIPTION));

        // Create and persist a business object definition tag entity.
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity =
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntities.get(0), tagEntities.get(0));

        // Get the business object definition tag by it's parent entities.
        assertEquals(businessObjectDefinitionTagEntity,
            businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByParentEntities(businessObjectDefinitionEntities.get(0), tagEntities.get(0)));

        // Try invalid values for all input parameters.
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByParentEntities(businessObjectDefinitionEntities.get(1), tagEntities.get(0)));
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByParentEntities(businessObjectDefinitionEntities.get(0), tagEntities.get(1)));
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByBusinessObjectDefinitionEntity()
    {
        // Create and persist two business object definition entities.
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = Arrays.asList(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), DATA_PROVIDER_NAME, DESCRIPTION),
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE_2, BDEF_NAME_2), DATA_PROVIDER_NAME, DESCRIPTION));

        // Create and persist two tag entities with display names in reverse order.
        List<TagEntity> tagEntities = Arrays.asList(tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION),
            tagDaoTestHelper.createTagEntity(TAG_TYPE_2, TAG_CODE_2, TAG_DISPLAY_NAME, TAG_DESCRIPTION));

        // Create and persist two business object definition tag entities for the first business object definition entity.
        for (TagEntity tagEntity : tagEntities)
        {
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntities.get(0), tagEntity);
        }

        // Get business object definition tags by business object definition entity.
        assertEquals(Arrays
            .asList(new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE_2, TAG_CODE_2)),
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE))),
            businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagsByBusinessObjectDefinitionEntity(businessObjectDefinitionEntities.get(0)));

        // Try invalid values for all input parameters.
        assertTrue(
            businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagsByBusinessObjectDefinitionEntity(businessObjectDefinitionEntities.get(1)).isEmpty());
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByTagEntity()
    {
        // Create and persist two business object definition entities with display names in reverse order.
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = Arrays.asList(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME_2, NO_ATTRIBUTES),
            businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION_2, BDEF_DISPLAY_NAME, NO_ATTRIBUTES));

        // Create and persist two tag entities.
        List<TagEntity> tagEntities = Arrays.asList(tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION),
            tagDaoTestHelper.createTagEntity(TAG_TYPE_2, TAG_CODE_2, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION));

        // Create and persist two business object definition tag entities for the first business object definition entity.
        for (BusinessObjectDefinitionEntity businessObjectDefinitionEntity : businessObjectDefinitionEntities)
        {
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntity, tagEntities.get(0));
        }

        // Get business object definition tags by business object definition entity.
        assertEquals(Arrays
            .asList(new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE_2, BDEF_NAME_2), new TagKey(TAG_TYPE, TAG_CODE)),
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE))),
            businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagsByTagEntity(tagEntities.get(0)));

        // Try invalid values for all input parameters.
        assertTrue(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagsByTagEntity(tagEntities.get(1)).isEmpty());
    }
}
