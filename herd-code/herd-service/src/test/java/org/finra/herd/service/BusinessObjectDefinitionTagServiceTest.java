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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTag;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKeys;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity;

/**
 * This class tests functionality within the business object definition tag service.
 */
public class BusinessObjectDefinitionTagServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateBusinessObjectDefinitionTag()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, new TagKey(TAG_TYPE, TAG_CODE));

        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(businessObjectDefinitionKey, DATA_PROVIDER_NAME, DESCRIPTION);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Create a business object definition tag.
        BusinessObjectDefinitionTag result =
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(businessObjectDefinitionTagKey));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionTag(result.getId(), businessObjectDefinitionTagKey), result);
    }

    @Test
    public void testCreateBusinessObjectDefinitionTagBusinessObjectDefinitionNoExists()
    {
        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Try to create a business object definition tag for a non-existing business object definition.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE))));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionTagBusinessObjectDefinitionTagAlreadyExists()
    {
        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Create and persist a business object definition tag entity.
        businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionTagKey);

        // Try to add a duplicate business object definition tag.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(businessObjectDefinitionTagKey));
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Tag with tag type \"%s\" and code \"%s\" already exists for business object definition {%s}.", TAG_TYPE, TAG_CODE,
                businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionTagInvalidParameters()
    {
        // Try to create a business object definition tag when business object definition namespace contains a forward slash character.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(addSlash(BDEF_NAMESPACE), BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object definition tag when business object definition name contains a forward slash character.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, addSlash(BDEF_NAME)), new TagKey(TAG_TYPE, TAG_CODE))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object definition tag when business object definition namespace contains a forward slash character.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(addSlash(TAG_TYPE), TAG_CODE))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag type code can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object definition tag when business object definition name contains a forward slash character.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, addSlash(TAG_CODE)))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag code can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionTagLowerCaseParameters()
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), DATA_PROVIDER_NAME, DESCRIPTION);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Create a business object definition tag using lower case parameter values.
        BusinessObjectDefinitionTag result = businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()),
                new TagKey(TAG_TYPE.toLowerCase(), TAG_CODE.toLowerCase()))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionTag(result.getId(),
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE))), result);
    }

    @Test
    public void testCreateBusinessObjectDefinitionTagMissingRequiredParameters()
    {
        // Try to create a business object definition tag when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BLANK_TEXT, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to create a business object definition tag when business object definition name is not specified.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BLANK_TEXT), new TagKey(TAG_TYPE, TAG_CODE))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to create a business object definition tag when tag type is not specified.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(BLANK_TEXT, TAG_CODE))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to create a business object definition tag when tag code is not specified.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, BLANK_TEXT))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag code must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionTagTagNoExists()
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), DATA_PROVIDER_NAME, DESCRIPTION);

        // Try to create a business object definition tag for a non-existing tag.
        try
        {
            businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE))));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag with code \"%s\" doesn't exist for tag type \"%s\".", TAG_CODE, TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionTagTrimParameters()
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), DATA_PROVIDER_NAME, DESCRIPTION);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Create a business object definition tag using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionTag result = businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME)),
                new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG_CODE)))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionTag(result.getId(),
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE))), result);
    }

    @Test
    public void testCreateBusinessObjectDefinitionTagUpperCaseParameters()
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), DATA_PROVIDER_NAME, DESCRIPTION);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Create a business object definition tag using upper case parameter values.
        BusinessObjectDefinitionTag result = businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(new BusinessObjectDefinitionTagCreateRequest(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()),
                new TagKey(TAG_TYPE.toUpperCase(), TAG_CODE.toUpperCase()))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionTag(result.getId(),
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE))), result);
    }

    @Test
    public void testDeleteBusinessObjectDefinitionTag()
    {
        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Create and persist a business object definition tag entity.
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity = businessObjectDefinitionTagDaoTestHelper
            .createBusinessObjectDefinitionTagEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Validate that this business object definition tag exists.
        assertNotNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));

        // Delete this business object definition tag.
        BusinessObjectDefinitionTag result = businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(businessObjectDefinitionTagKey);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionTag(businessObjectDefinitionTagEntity.getId(), businessObjectDefinitionTagKey), result);

        // Ensure that this business object definition tag is no longer there.
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionTagLowerCaseParameters()
    {
        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Create and persist a business object definition tag entity.
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity = businessObjectDefinitionTagDaoTestHelper
            .createBusinessObjectDefinitionTagEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Validate that this business object definition tag exists.
        assertNotNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));

        // Delete this business object definition tag using lower case parameter values.
        BusinessObjectDefinitionTag result = businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()),
                new TagKey(TAG_TYPE.toLowerCase(), TAG_CODE.toLowerCase())));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionTag(businessObjectDefinitionTagEntity.getId(), businessObjectDefinitionTagKey), result);

        // Ensure that this business object definition tag is no longer there.
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionTagMissingRequiredParameters()
    {
        // Try to delete a business object definition tag when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BLANK_TEXT, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to delete a business object definition tag when business object definition name is not specified.
        try
        {
            businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BLANK_TEXT), new TagKey(TAG_TYPE, TAG_CODE)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to delete a business object definition tag when tag type is not specified.
        try
        {
            businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(BLANK_TEXT, TAG_CODE)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to delete a business object definition tag when tag code is not specified.
        try
        {
            businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, BLANK_TEXT)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag code must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDefinitionTagTagNoExists()
    {
        // Try to create a non-existing business object definition tag.
        try
        {
            businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(
                new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag with tag type \"%s\" and code \"%s\" does not exist for business object definition {%s}.", TAG_TYPE, TAG_CODE,
                businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDefinitionTagTrimParameters()
    {
        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Create and persist a business object definition tag entity.
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity = businessObjectDefinitionTagDaoTestHelper
            .createBusinessObjectDefinitionTagEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Validate that this business object definition tag exists.
        assertNotNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));

        // Delete this business object definition tag using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionTag result = businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME)),
                new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG_CODE))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionTag(businessObjectDefinitionTagEntity.getId(), businessObjectDefinitionTagKey), result);

        // Ensure that this business object definition tag is no longer there.
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionTagUpperCaseParameters()
    {
        // Create a business object definition tag key.
        BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey =
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Create and persist a business object definition tag entity.
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity = businessObjectDefinitionTagDaoTestHelper
            .createBusinessObjectDefinitionTagEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new TagKey(TAG_TYPE, TAG_CODE));

        // Validate that this business object definition tag exists.
        assertNotNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));

        // Delete this business object definition tag using upper case parameter values.
        BusinessObjectDefinitionTag result = businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()),
                new TagKey(TAG_TYPE.toUpperCase(), TAG_CODE.toUpperCase())));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionTag(businessObjectDefinitionTagEntity.getId(), businessObjectDefinitionTagKey), result);

        // Ensure that this business object definition tag is no longer there.
        assertNull(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey));
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByBusinessObjectDefinition()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create tag keys.
        List<TagKey> tagKeys = Arrays.asList(new TagKey(TAG_TYPE, TAG_CODE), new TagKey(TAG_TYPE_2, TAG_CODE_2));

        // Create and persist business object definition tag entities.
        for (TagKey tagKey : tagKeys)
        {
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionKey, tagKey);
        }

        // Get business object definition tags by business object definition.
        BusinessObjectDefinitionTagKeys result =
            businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByBusinessObjectDefinition(businessObjectDefinitionKey);

        // Validate the returned object.
        assertNotNull(result);
        assertEquals(Arrays.asList(new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, tagKeys.get(0)),
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, tagKeys.get(1))), result.getBusinessObjectDefinitionTagKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByBusinessObjectDefinitionBusinessObjectDefinitionNoExists()
    {
        // Try to business object definition tags for a non-existing business object definition.
        try
        {
            businessObjectDefinitionTagService
                .getBusinessObjectDefinitionTagsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME),
                e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByBusinessObjectDefinitionLowerCaseParameters()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create tag keys.
        List<TagKey> tagKeys = Arrays.asList(new TagKey(TAG_TYPE, TAG_CODE), new TagKey(TAG_TYPE_2, TAG_CODE_2));

        // Create and persist business object definition tag entities.
        for (TagKey tagKey : tagKeys)
        {
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionKey, tagKey);
        }

        // Get business object definition tags by business object definition using lower case parameter values.
        BusinessObjectDefinitionTagKeys result = businessObjectDefinitionTagService
            .getBusinessObjectDefinitionTagsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()));

        // Validate the returned object.
        assertNotNull(result);
        assertEquals(Arrays.asList(new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, tagKeys.get(0)),
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, tagKeys.get(1))), result.getBusinessObjectDefinitionTagKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByBusinessObjectDefinitionMissingRequiredParameters()
    {
        // Try to get business object definition tags by business object definition when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionTagService
                .getBusinessObjectDefinitionTagsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BLANK_TEXT, BDEF_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to get business object definition tags by business object definition when business object definition name is not specified.
        try
        {
            businessObjectDefinitionTagService
                .getBusinessObjectDefinitionTagsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BLANK_TEXT));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByBusinessObjectDefinitionNoBusinessObjectDefinitionTagsExist()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(businessObjectDefinitionKey, DATA_PROVIDER_NAME, DESCRIPTION);

        // Get business object definition tags by business object definition.
        BusinessObjectDefinitionTagKeys result =
            businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByBusinessObjectDefinition(businessObjectDefinitionKey);

        // Validate the returned object.
        assertNotNull(result);
        assertTrue(result.getBusinessObjectDefinitionTagKeys().isEmpty());
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByBusinessObjectDefinitionTrimParameters()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create tag keys.
        List<TagKey> tagKeys = Arrays.asList(new TagKey(TAG_TYPE, TAG_CODE), new TagKey(TAG_TYPE_2, TAG_CODE_2));

        // Create and persist business object definition tag entities.
        for (TagKey tagKey : tagKeys)
        {
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionKey, tagKey);
        }

        // Get business object definition tags by business object definition using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionTagKeys result = businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByBusinessObjectDefinition(
            new BusinessObjectDefinitionKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME)));

        // Validate the returned object.
        assertNotNull(result);
        assertEquals(Arrays.asList(new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, tagKeys.get(0)),
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, tagKeys.get(1))), result.getBusinessObjectDefinitionTagKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByBusinessObjectDefinitionUpperCaseParameters()
    {
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Create tag keys.
        List<TagKey> tagKeys = Arrays.asList(new TagKey(TAG_TYPE, TAG_CODE), new TagKey(TAG_TYPE_2, TAG_CODE_2));

        // Create and persist business object definition tag entities.
        for (TagKey tagKey : tagKeys)
        {
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionKey, tagKey);
        }

        // Get business object definition tags by business object definition using upper case parameter values.
        BusinessObjectDefinitionTagKeys result = businessObjectDefinitionTagService
            .getBusinessObjectDefinitionTagsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()));

        // Validate the returned object.
        assertNotNull(result);
        assertEquals(Arrays.asList(new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, tagKeys.get(0)),
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKey, tagKeys.get(1))), result.getBusinessObjectDefinitionTagKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByTag()
    {
        // Create business object definition keys.
        List<BusinessObjectDefinitionKey> businessObjectDefinitionKeys =
            Arrays.asList(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new BusinessObjectDefinitionKey(BDEF_NAMESPACE_2, BDEF_NAME_2));

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist business object definition tag entities.
        for (BusinessObjectDefinitionKey businessObjectDefinitionKey : businessObjectDefinitionKeys)
        {
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionKey, tagKey);
        }

        // Get business object definition tags by tag.
        BusinessObjectDefinitionTagKeys result = businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByTag(tagKey);

        // Validate the returned object.
        assertNotNull(result);
        assertEquals(Arrays.asList(new BusinessObjectDefinitionTagKey(businessObjectDefinitionKeys.get(0), tagKey),
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKeys.get(1), tagKey)), result.getBusinessObjectDefinitionTagKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByTagLowerCaseParameters()
    {
        // Create business object definition keys.
        List<BusinessObjectDefinitionKey> businessObjectDefinitionKeys =
            Arrays.asList(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new BusinessObjectDefinitionKey(BDEF_NAMESPACE_2, BDEF_NAME_2));

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist business object definition tag entities.
        for (BusinessObjectDefinitionKey businessObjectDefinitionKey : businessObjectDefinitionKeys)
        {
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionKey, tagKey);
        }

        // Get business object definition tags by tag using lower case parameter values.
        BusinessObjectDefinitionTagKeys result =
            businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByTag(new TagKey(TAG_TYPE.toLowerCase(), TAG_CODE.toLowerCase()));

        // Validate the returned object.
        assertNotNull(result);
        assertEquals(Arrays.asList(new BusinessObjectDefinitionTagKey(businessObjectDefinitionKeys.get(0), tagKey),
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKeys.get(1), tagKey)), result.getBusinessObjectDefinitionTagKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByTagMissingRequiredParameters()
    {
        // Try to get business object definition tags by tag when when tag type is not specified.
        try
        {
            businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByTag(new TagKey(BLANK_TEXT, TAG_CODE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to get business object definition tags by tag when tag code is not specified.
        try
        {
            businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByTag(new TagKey(TAG_TYPE, BLANK_TEXT));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag code must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByTagNoBusinessObjectDefinitionTagsExist()
    {
        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Get business object definition tags by tag.
        BusinessObjectDefinitionTagKeys result = businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByTag(new TagKey(TAG_TYPE, TAG_CODE));

        // Validate the returned object.
        assertNotNull(result);
        assertTrue(result.getBusinessObjectDefinitionTagKeys().isEmpty());
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByTagTagNoExists()
    {
        // Try to get business object definition tags for a non-existing tag.
        try
        {
            businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByTag(new TagKey(TAG_TYPE, TAG_CODE));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag with code \"%s\" doesn't exist for tag type \"%s\".", TAG_CODE, TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByTagTrimParamters()
    {
        // Create business object definition keys.
        List<BusinessObjectDefinitionKey> businessObjectDefinitionKeys =
            Arrays.asList(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new BusinessObjectDefinitionKey(BDEF_NAMESPACE_2, BDEF_NAME_2));

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist business object definition tag entities.
        for (BusinessObjectDefinitionKey businessObjectDefinitionKey : businessObjectDefinitionKeys)
        {
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionKey, tagKey);
        }

        // Get business object definition tags by tag using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionTagKeys result =
            businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByTag(new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG_CODE)));

        // Validate the returned object.
        assertNotNull(result);
        assertEquals(Arrays.asList(new BusinessObjectDefinitionTagKey(businessObjectDefinitionKeys.get(0), tagKey),
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKeys.get(1), tagKey)), result.getBusinessObjectDefinitionTagKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionTagsByTagUpperCaseParameters()
    {
        // Create business object definition keys.
        List<BusinessObjectDefinitionKey> businessObjectDefinitionKeys =
            Arrays.asList(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), new BusinessObjectDefinitionKey(BDEF_NAMESPACE_2, BDEF_NAME_2));

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist business object definition tag entities.
        for (BusinessObjectDefinitionKey businessObjectDefinitionKey : businessObjectDefinitionKeys)
        {
            businessObjectDefinitionTagDaoTestHelper.createBusinessObjectDefinitionTagEntity(businessObjectDefinitionKey, tagKey);
        }

        // Get business object definition tags by tag using upper case parameter values.
        BusinessObjectDefinitionTagKeys result =
            businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByTag(new TagKey(TAG_TYPE.toUpperCase(), TAG_CODE.toUpperCase()));

        // Validate the returned object.
        assertNotNull(result);
        assertEquals(Arrays.asList(new BusinessObjectDefinitionTagKey(businessObjectDefinitionKeys.get(0), tagKey),
            new BusinessObjectDefinitionTagKey(businessObjectDefinitionKeys.get(1), tagKey)), result.getBusinessObjectDefinitionTagKeys());
    }
}
