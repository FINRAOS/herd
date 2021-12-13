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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagChild;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagListResponse;
import org.finra.herd.model.api.xml.TagSearchFilter;
import org.finra.herd.model.api.xml.TagSearchKey;
import org.finra.herd.model.api.xml.TagSearchRequest;
import org.finra.herd.model.api.xml.TagSearchResponse;
import org.finra.herd.model.api.xml.TagUpdateRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.service.impl.TagServiceImpl;

public class TagServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateTag()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a tag.
        Tag tag = tagService.createTag(new TagCreateRequest(tagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));

        // Get the tag entity.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        assertNotNull(tagEntity);

        // Validate the response object.
        assertEquals(
            new Tag(tagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, tagEntity.getCreatedBy(),
                tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            tag);
    }

    @Test
    public void testCreateTagDisplayNameAlreadyExists()
    {
        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME.toUpperCase(), TAG_DESCRIPTION);

        // Try to create a tag with a duplicate tag display name.
        try
        {
            tagService.createTag(
                new TagCreateRequest(new TagKey(TAG_TYPE, TAG_CODE_2), TAG_DISPLAY_NAME.toLowerCase(), TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION,
                    NO_PARENT_TAG_KEY));
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String
                .format("Display name \"%s\" already exists for a tag with tag type \"%s\" and tag code \"%s\".", TAG_DISPLAY_NAME.toLowerCase(), TAG_TYPE,
                    TAG_CODE), e.getMessage());
        }
    }

    @Test
    public void testCreateTagInvalidParameters()
    {
        // Try to create a tag when tag type contains a forward slash character.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(addSlash(TAG_TYPE), TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION,
                NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag type code can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a tag when tag code contains a forward slash character.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, addSlash(TAG_CODE)), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION,
                NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag code can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a tag when tag type contains a backward slash character.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(addBackwardSlash(TAG_TYPE), TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION,
                NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag type code can not contain a backward slash character.", e.getMessage());
        }

        // Try to create a tag when tag code contains a backward slash character.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, addBackwardSlash(TAG_CODE)), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION,
                NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag code can not contain a backward slash character.", e.getMessage());
        }

        // Try to create a tag with parent tag type is not the same as the requested.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION,
                new TagKey(TAG_TYPE_2, TAG_CODE_2)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag type code in parent tag key must match the tag type code in the request.", e.getMessage());
        }

        // Try to create a tag with a negative search score multiplier value.
        try
        {
            tagService.createTag(
                new TagCreateRequest(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER.multiply(BigDecimal.valueOf(-1)),
                    TAG_DESCRIPTION, NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The searchScoreMultiplier can not have a negative value. searchScoreMultiplier=%s",
                TAG_SEARCH_SCORE_MULTIPLIER.multiply(BigDecimal.valueOf(-1))), e.getMessage());
        }
    }

    @Test
    public void testCreateTagLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a tag using lowercase input parameters.
        Tag resultTag = tagService.createTag(
            new TagCreateRequest(new TagKey(TAG_TYPE.toLowerCase(), TAG_CODE.toLowerCase()), TAG_DISPLAY_NAME.toLowerCase(), TAG_SEARCH_SCORE_MULTIPLIER,
                TAG_DESCRIPTION.toLowerCase(), NO_PARENT_TAG_KEY));

        // Get the tag entity.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        assertNotNull(tagEntity);

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE.toLowerCase()), TAG_DISPLAY_NAME.toLowerCase(), TAG_SEARCH_SCORE_MULTIPLIER,
            TAG_DESCRIPTION.toLowerCase(), tagEntity.getCreatedBy(), tagEntity.getUpdatedBy(),
            HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG), resultTag);
    }

    @Test
    public void testCreateTagMissingOptionalParametersPassedAsNulls()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a tag with description passed in as null.
        Tag tag = tagService.createTag(new TagCreateRequest(tagKey, TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_PARENT_TAG_KEY));

        // Get the tag entity.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        assertNotNull(tagEntity);

        // Validate the response object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, tagEntity.getCreatedBy(),
            tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG), tag);
    }

    @Test
    public void testCreateTagMissingOptionalParametersPassedAsWhitespace()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a tag with description passed in as whitespace.
        Tag tag = tagService.createTag(new TagCreateRequest(tagKey, TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, BLANK_TEXT, NO_PARENT_TAG_KEY));

        // Get the tag entity.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        assertNotNull(tagEntity);

        // Validate the response object.
        assertEquals(
            new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, BLANK_TEXT, tagEntity.getCreatedBy(), tagEntity.getUpdatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG), tag);
    }

    @Test
    public void testCreateTagMissingRequiredParams()
    {
        // Missing tag key.
        try
        {
            tagService.createTag(new TagCreateRequest(null, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag key must be specified.", e.getMessage());
        }

        // Missing tag type code in the key.
        try
        {
            tagService
                .createTag(new TagCreateRequest(new TagKey(null, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Missing tag code in the key.
        try
        {
            tagService
                .createTag(new TagCreateRequest(new TagKey(TAG_TYPE, null), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag code must be specified.", e.getMessage());
        }

        // Missing display name in the request.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG_CODE), null, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A display name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateTagParentTagNoExists()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Try to create a tag with a non-existing parent tag.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION,
                new TagKey(TAG_TYPE, TAG_CODE_2)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag with code \"%s\" doesn't exist for tag type \"%s\".", TAG_CODE_2, TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testCreateTagTagAlreadyExists()
    {
        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE.toUpperCase(), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Try to create a duplicate tag (uses the same tag type and tag name).
        try
        {
            tagService.createTag(
                new TagCreateRequest(new TagKey(TAG_TYPE, TAG_CODE.toLowerCase()), TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2,
                    NO_PARENT_TAG_KEY));
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String
                .format("Unable to create tag with tag type code \"%s\" and tag code \"%s\" because it already exists.", TAG_TYPE, TAG_CODE.toLowerCase()),
                e.getMessage());
        }
    }

    @Test
    public void testCreateTagTagTypeNoExists()
    {
        // Try to create a tag using non-existing tag type.
        try
        {
            tagService.createTag(
                new TagCreateRequest(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag type with code \"%s\" doesn't exist.", TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testCreateTagTrimParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a tag with parameters padded with whitespace.
        Tag tag = tagService.createTag(
            new TagCreateRequest(new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG_CODE)), addWhitespace(TAG_DISPLAY_NAME), TAG_SEARCH_SCORE_MULTIPLIER,
                addWhitespace(TAG_DESCRIPTION), NO_PARENT_TAG_KEY));

        // Get the tag entity.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        assertNotNull(tagEntity);

        // Validate the response object.
        assertEquals(new Tag(tagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, addWhitespace(TAG_DESCRIPTION),
            tagEntity.getCreatedBy(), tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY,
            NO_TAG_HAS_CHILDREN_FLAG), tag);
    }

    @Test
    public void testCreateTagUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a tag using uppercase input parameters.
        Tag resultTag = tagService.createTag(
            new TagCreateRequest(new TagKey(TAG_TYPE.toUpperCase(), TAG_CODE.toUpperCase()), TAG_DISPLAY_NAME.toUpperCase(), TAG_SEARCH_SCORE_MULTIPLIER,
                TAG_DESCRIPTION.toUpperCase(), NO_PARENT_TAG_KEY));

        // Get the tag entity.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        assertNotNull(tagEntity);

        // Validate the returned object.
        assertEquals(new Tag(resultTag.getId(), new TagKey(TAG_TYPE, TAG_CODE.toUpperCase()), TAG_DISPLAY_NAME.toUpperCase(), TAG_SEARCH_SCORE_MULTIPLIER,
            TAG_DESCRIPTION.toUpperCase(), tagEntity.getCreatedBy(), tagEntity.getUpdatedBy(),
            HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG), resultTag);
    }

    @Test
    public void testCreateTagWithParent()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create a tag key.
        TagKey parentTagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a parent tag.
        Tag parentTag =
            tagService.createTag(new TagCreateRequest(parentTagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));

        // Get the parent tag entity.
        TagEntity parentTagEntity = tagDao.getTagByKey(parentTagKey);
        assertNotNull(parentTagEntity);

        // Validate the response object.
        assertEquals(new Tag(parentTagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION,
            parentTagEntity.getCreatedBy(), parentTagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(parentTagEntity.getUpdatedOn()),
            NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG), parentTag);

        // Create a tag key.
        TagKey childTagKey = new TagKey(TAG_TYPE, TAG_CODE_2);

        // Create a child tag.
        Tag childTag =
            tagService.createTag(new TagCreateRequest(childTagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, parentTagKey));

        // Get the child tag entity.
        TagEntity childTagEntity = tagDao.getTagByKey(childTagKey);
        assertNotNull(childTagEntity);

        assertEquals(
            new Tag(childTagEntity.getId(), childTagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, childTagEntity.getCreatedBy(),
                childTagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(childTagEntity.getUpdatedOn()), parentTagKey,
                NO_TAG_HAS_CHILDREN_FLAG), childTag);
    }

    @Test
    public void testDeleteTag()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Validate that this tag exists.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        assertNotNull(tagEntity);

        // Delete this tag.
        Tag deletedTag = tagService.deleteTag(new TagKey(TAG_TYPE, TAG_CODE));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, tagEntity.getCreatedBy(),
            tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(tagKey));
    }

    @Test
    public void testDeleteTagLowerCaseParameters()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Validate that this tag exists.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        assertNotNull(tagEntity);

        // Delete this tag using uppercase input parameters.
        Tag deletedTag = tagService.deleteTag(new TagKey(TAG_TYPE.toLowerCase(), TAG_CODE.toLowerCase()));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, tagEntity.getCreatedBy(),
            tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(tagKey));
    }

    @Test
    public void testDeleteTagMissingRequiredParameters()
    {
        // Try to delete a tag when tag type is not specified.
        try
        {
            tagService.deleteTag(new TagKey(BLANK_TEXT, TAG_CODE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to delete a tag when tag code is not specified.
        try
        {
            tagService.deleteTag(new TagKey(TAG_TYPE, BLANK_TEXT));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag code must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteTagTagNoExists()
    {
        // Try to delete a non-existing tag.
        try
        {
            tagService.deleteTag(new TagKey(TAG_TYPE, TAG_CODE));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag with code \"%s\" doesn't exist for tag type \"%s\".", TAG_CODE, TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testDeleteTagTrimParameters()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Validate that this tag exists.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        assertNotNull(tagEntity);

        // Delete this tag using input parameters with leading and trailing empty spaces.
        Tag deletedTag = tagService.deleteTag(new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG_CODE)));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, tagEntity.getCreatedBy(),
            tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(tagKey));
    }

    @Test
    public void testDeleteTagUpperCaseParameters()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Validate that this tag exists.
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);
        assertNotNull(tagEntity);

        // Delete this tag using uppercase input parameters.
        Tag deletedTag = tagService.deleteTag(new TagKey(TAG_TYPE.toUpperCase(), TAG_CODE.toUpperCase()));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, tagEntity.getCreatedBy(),
            tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(tagKey));
    }

    @Test
    public void testDeleteTagWithParent()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create a tag key.
        TagKey parentTagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a parent tag.
        Tag parentTag =
            tagService.createTag(new TagCreateRequest(parentTagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));

        // Get the parent tag entity.
        TagEntity parentTagEntity = tagDao.getTagByKey(parentTagKey);
        assertNotNull(parentTagEntity);

        // Validate the response object.
        assertEquals(new Tag(parentTagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION,
            parentTagEntity.getCreatedBy(), parentTagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(parentTagEntity.getUpdatedOn()),
            NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG), parentTag);

        // Create a tag key.
        TagKey childTagKey = new TagKey(TAG_TYPE, TAG_CODE_2);

        // Create a child tag.
        Tag childTag =
            tagService.createTag(new TagCreateRequest(childTagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, parentTagKey));

        // Get the child tag entity.
        TagEntity childTagEntity = tagDao.getTagByKey(childTagKey);
        assertNotNull(childTagEntity);

        assertEquals(
            new Tag(childTagEntity.getId(), childTagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, childTagEntity.getCreatedBy(),
                childTagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(childTagEntity.getUpdatedOn()), parentTagKey,
                NO_TAG_HAS_CHILDREN_FLAG), childTag);

        // Delete this tag.
        Tag deletedTag = tagService.deleteTag(childTagKey);

        // Validate the returned object.
        assertEquals(
            new Tag(childTagEntity.getId(), childTagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, childTagEntity.getCreatedBy(),
                childTagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(childTagEntity.getUpdatedOn()), parentTagKey,
                NO_TAG_HAS_CHILDREN_FLAG), deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(childTagKey));
    }

    @Test
    public void testGetTag()
    {
        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Retrieve the tag.
        Tag resultTag = tagService.getTag(new TagKey(TAG_TYPE, TAG_CODE));

        // Validate the returned object.
        assertEquals(
            new Tag(tagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, tagEntity.getCreatedBy(),
                tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            resultTag);
    }

    @Test
    public void testGetTagLowerCaseParameters()
    {
        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Get the tag using lower case input parameters.
        Tag resultTag = tagService.getTag(new TagKey(TAG_TYPE.toLowerCase(), TAG_CODE.toLowerCase()));

        // Validate the returned object.
        assertEquals(
            new Tag(tagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, tagEntity.getCreatedBy(),
                tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            resultTag);
    }

    @Test
    public void testGetTagMissingRequiredParameters()
    {
        // Try to get a tag when tag type is not specified.
        try
        {
            tagService.getTag(new TagKey(BLANK_TEXT, TAG_CODE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to get a tag when tag code is not specified.
        try
        {
            tagService.getTag(new TagKey(TAG_TYPE, BLANK_TEXT));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag code must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetTagTagNoExists()
    {
        // Try to get a non-existing tag.
        try
        {
            tagService.getTag(new TagKey(TAG_TYPE, TAG_CODE));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag with code \"%s\" doesn't exist for tag type \"%s\".", TAG_CODE, TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testGetTagTrimParameters()
    {
        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Retrieve the tag using input parameters with leading and trailing empty spaces.
        Tag resultTag = tagService.getTag(new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG_CODE)));

        // Validate the returned object.
        assertEquals(
            new Tag(tagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, tagEntity.getCreatedBy(),
                tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            resultTag);
    }

    @Test
    public void testGetTagUpperCaseParameters()
    {
        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Get the tag using uppercase input parameters.
        Tag resultTag = tagService.getTag(new TagKey(TAG_TYPE.toUpperCase(), TAG_CODE.toUpperCase()));

        // Validate the returned object.
        assertEquals(
            new Tag(tagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, tagEntity.getCreatedBy(),
                tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            resultTag);
    }

    @Test
    public void testGetTags()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create and persist two tag entities for the same tag type.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_TYPE_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2);

        // Retrieve a list of tag keys.
        TagListResponse resultTagKeys = tagService.getTags(TAG_TYPE, NO_PARENT_TAG_CODE);

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(Arrays
            .asList(new TagChild(new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN), new TagChild(new TagKey(TAG_TYPE, TAG_CODE_2), TAG_HAS_NO_CHILDREN)),
            resultTagKeys.getTagChildren());
    }

    @Test
    public void testGetTagsLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create and persist two tag entities for the same tag type.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_TYPE_DISPLAY_NAME_2, TAG_DESCRIPTION_2);

        // Retrieve a list of tag keys using lowercase input parameters.
        TagListResponse resultTagKeys = tagService.getTags(addWhitespace(TAG_TYPE), NO_PARENT_TAG_CODE);

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(Arrays
            .asList(new TagChild(new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN), new TagChild(new TagKey(TAG_TYPE, TAG_CODE_2), TAG_HAS_NO_CHILDREN)),
            resultTagKeys.getTagChildren());
    }

    @Test
    public void testGetTagsMissingRequiredParameters()
    {
        // Try to get a tag when tag type is not specified.
        try
        {
            tagService.getTags(BLANK_TEXT, NO_PARENT_TAG_CODE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetTagsTagTypeNoExists()
    {
        // Try to retrieve a list of tag keys for a non-existing tag type.
        try
        {
            tagService.getTags(TAG_TYPE, NO_PARENT_TAG_CODE);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag type with code \"%s\" doesn't exist.", TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testGetTagsTagsNoExist()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Retrieve a list of tag keys, when none of the tags exist for the tag type.
        TagListResponse resultTagKeys = tagService.getTags(TAG_TYPE, NO_PARENT_TAG_CODE);

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(0, resultTagKeys.getTagChildren().size());
    }

    @Test
    public void testGetTagsTrimParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);
        // Create and persist two tag entities for the same tag type.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_TYPE_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2);

        // Retrieve a list of tag keys using input parameters with leading and trailing empty spaces.
        TagListResponse resultTagKeys = tagService.getTags(addWhitespace(TAG_TYPE), NO_PARENT_TAG_CODE);

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(Arrays
            .asList(new TagChild(new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN), new TagChild(new TagKey(TAG_TYPE, TAG_CODE_2), TAG_HAS_NO_CHILDREN)),
            resultTagKeys.getTagChildren());
    }

    @Test
    public void testGetTagsUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create and persist two tag entities for the same tag type.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_TYPE_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION);

        // Retrieve a list of tag keys using uppercase input parameters.
        TagListResponse resultTagKeys = tagService.getTags(TAG_TYPE.toUpperCase(), NO_PARENT_TAG_CODE);

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(Arrays
            .asList(new TagChild(new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN), new TagChild(new TagKey(TAG_TYPE, TAG_CODE_2), TAG_HAS_NO_CHILDREN)),
            resultTagKeys.getTagChildren());
    }

    @Test
    public void testGetTagsWithParent()
    {
        // Create and persist a tag entity.
        TagEntity root = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);
        TagEntity child =
            tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE_2, TAG_DISPLAY_NAME + "x", TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2 + "x", root);
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE_2 + "y", TAG_DISPLAY_NAME_2 + "y", TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_2 + "y", child);

        //only the root
        TagListResponse resultTagKeys = tagService.getTags(TAG_TYPE, NO_PARENT_TAG_CODE);
        assertNull(resultTagKeys.getParentTagKey());
        assertNull(resultTagKeys.getTagKey());
        assertEquals(resultTagKeys.getTagChildren().size(), 1);

        resultTagKeys = tagService.getTags(TAG_TYPE, TAG_CODE);
        assertNull(resultTagKeys.getParentTagKey());
        assertEquals(resultTagKeys.getTagChildren().size(), 1);
        assertEquals(resultTagKeys.getTagKey(), new TagKey(TAG_TYPE, TAG_CODE));
        //the lower case should be the same
        resultTagKeys = tagService.getTags(TAG_TYPE, TAG_CODE.toLowerCase() + " ");
        assertNull(resultTagKeys.getParentTagKey());
        assertEquals(resultTagKeys.getTagChildren().size(), 1);
        assertEquals(resultTagKeys.getTagKey(), new TagKey(TAG_TYPE, TAG_CODE));

        resultTagKeys = tagService.getTags(TAG_TYPE, TAG_CODE_2.toLowerCase() + " ");
        assertNotNull(resultTagKeys.getParentTagKey());
        assertEquals(resultTagKeys.getParentTagKey(), new TagKey(TAG_TYPE, TAG_CODE));
        assertEquals(resultTagKeys.getTagChildren().size(), 1);
        assertEquals(resultTagKeys.getTagKey(), new TagKey(TAG_TYPE, TAG_CODE_2));
    }

    @Test
    public void testSearchTags()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagSearchTesting();

        // Search the tags.
        TagSearchResponse tagSearchResponse = tagService.searchTags(
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, TAG_CODE, NO_IS_PARENT_TAG_NULL_FLAG))))),
            Sets.newHashSet(TagServiceImpl.DISPLAY_NAME_FIELD, TagServiceImpl.SEARCH_SCORE_MULTIPLIER_FIELD, TagServiceImpl.DESCRIPTION_FIELD,
                TagServiceImpl.PARENT_TAG_KEY_FIELD, TagServiceImpl.HAS_CHILDREN_FIELD));

        // Validate the returned object.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN))), tagSearchResponse);
    }

    @Test
    public void testSearchTagsInvalidParameters()
    {
        // Try to search tags when there are more than one tag search filter is specified.
        try
        {
            tagService.searchTags(new TagSearchRequest(Arrays.asList(new TagSearchFilter(), new TagSearchFilter())), NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At most one tag search filter must be specified.", e.getMessage());
        }

        // Try to search tags when there are more than one tag search key is specified.
        try
        {
            tagService.searchTags(new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(), new TagSearchKey())))),
                NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exactly one tag search key must be specified.", e.getMessage());
        }

        // Try to search tags for a non-existing tag type.
        try
        {
            tagService.searchTags(new TagSearchRequest(
                Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey("I_DO_NOT_EXIST", NO_PARENT_TAG_CODE, NO_IS_PARENT_TAG_NULL_FLAG))))),
                NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals("Tag type with code \"I_DO_NOT_EXIST\" doesn't exist.", e.getMessage());
        }

        // Try to search tags using a un-supported search response field option.
        try
        {
            tagService.searchTags(new TagSearchRequest(
                Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, NO_PARENT_TAG_CODE, NO_IS_PARENT_TAG_NULL_FLAG))))),
                Sets.newHashSet("INVALID_FIELD_OPTION"));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Search response field \"invalid_field_option\" is not supported.", e.getMessage());
        }

        // Try to search tags when parent tag code is specified along with "is parent tag null" flag set to true.
        try
        {
            tagService
                .searchTags(new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, TAG_CODE, PARENT_TAG_IS_NULL))))),
                    NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A parent tag code can not be specified when isParentTagNull flag is set to true.", e.getMessage());
        }
    }

    @Test
    public void testSearchTagsLowerCaseParameters()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagSearchTesting();

        // Search the tags using lower case input parameters.
        TagSearchResponse tagSearchResponse = tagService.searchTags(new TagSearchRequest(
            Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE.toLowerCase(), TAG_CODE.toLowerCase(), NO_IS_PARENT_TAG_NULL_FLAG))))),
            Sets.newHashSet(TagServiceImpl.DISPLAY_NAME_FIELD.toLowerCase(), TagServiceImpl.SEARCH_SCORE_MULTIPLIER_FIELD.toLowerCase(),
                TagServiceImpl.DESCRIPTION_FIELD.toLowerCase(), TagServiceImpl.PARENT_TAG_KEY_FIELD.toLowerCase(),
                TagServiceImpl.HAS_CHILDREN_FIELD.toLowerCase()));

        // Validate the returned object.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN))), tagSearchResponse);
    }

    @Test
    public void testSearchTagsMissingOptionalParameters()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagSearchTesting();

        // Search tags without specifying an optional tag search filter.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG))), tagService.searchTags(new TagSearchRequest(), NO_SEARCH_RESPONSE_FIELDS));

        // Search tags when an optional tag search filter is set to null.
        List<TagSearchFilter> tagSearchFilters = new ArrayList<>();
        tagSearchFilters.add(null);
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG))),
            tagService.searchTags(new TagSearchRequest(tagSearchFilters), NO_SEARCH_RESPONSE_FIELDS));

        // Search tags without specifying optional parameters inside the tag search filter.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG))), tagService.searchTags(
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, BLANK_TEXT, NO_IS_PARENT_TAG_NULL_FLAG))))),
            NO_SEARCH_RESPONSE_FIELDS));

        // Search tags without specifying optional parameters except for the display name field option.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), TAG_DISPLAY_NAME_2, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), TAG_DISPLAY_NAME_3, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG))), tagService.searchTags(
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, BLANK_TEXT, NO_IS_PARENT_TAG_NULL_FLAG))))),
            Sets.newHashSet(TagServiceImpl.DISPLAY_NAME_FIELD)));

        // Search tags without specifying optional parameters except for the search score multiplier option.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE), NO_TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), NO_TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER_3, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), NO_TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER_2, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG))), tagService.searchTags(
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, BLANK_TEXT, NO_IS_PARENT_TAG_NULL_FLAG))))),
            Sets.newHashSet(TagServiceImpl.SEARCH_SCORE_MULTIPLIER_FIELD)));

        // Search tags without specifying optional parameters except for the description field option.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION_3, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION_2, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG))), tagService.searchTags(
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, BLANK_TEXT, NO_IS_PARENT_TAG_NULL_FLAG))))),
            Sets.newHashSet(TagServiceImpl.DESCRIPTION_FIELD)));

        // Search tags without specifying optional parameters except for the parent tag key field option.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, new TagKey(TAG_TYPE, TAG_CODE), NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, new TagKey(TAG_TYPE, TAG_CODE), NO_TAG_HAS_CHILDREN_FLAG))), tagService.searchTags(
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, BLANK_TEXT, NO_IS_PARENT_TAG_NULL_FLAG))))),
            Sets.newHashSet(TagServiceImpl.PARENT_TAG_KEY_FIELD)));

        // Search tags without specifying optional parameters except for the "has children" field option.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, TAG_HAS_CHILDREN),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, TAG_HAS_NO_CHILDREN),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, TAG_HAS_NO_CHILDREN))), tagService.searchTags(
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, BLANK_TEXT, NO_IS_PARENT_TAG_NULL_FLAG))))),
            Sets.newHashSet(TagServiceImpl.HAS_CHILDREN_FIELD)));
    }

    @Test
    public void testSearchTagsMissingRequiredParameters()
    {
        // Try to search tags when tag search request is not specified.
        try
        {
            tagService.searchTags(null, NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag search request must be specified.", e.getMessage());
        }

        // Try to search tags when tag search key is not specified.
        try
        {
            tagService.searchTags(new TagSearchRequest(Arrays.asList(new TagSearchFilter())), NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exactly one tag search key must be specified.", e.getMessage());
        }

        // Try to search tags when tag search key is set to null.
        try
        {
            List<TagSearchKey> tagSearchKeys = new ArrayList<>();
            tagSearchKeys.add(null);
            tagService.searchTags(new TagSearchRequest(Arrays.asList(new TagSearchFilter(tagSearchKeys))), NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exactly one tag search key must be specified.", e.getMessage());
        }

        // Try to search tags when tag type code is not specified.
        try
        {
            tagService.searchTags(new TagSearchRequest(
                Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(BLANK_TEXT, NO_PARENT_TAG_CODE, NO_IS_PARENT_TAG_NULL_FLAG))))),
                NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }
    }

    @Test
    public void testSearchTagsTrimParameters()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagSearchTesting();

        // Search the tags by using input parameters with leading and trailing empty spaces.
        TagSearchResponse tagSearchResponse = tagService.searchTags(new TagSearchRequest(
            Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(addWhitespace(TAG_TYPE), addWhitespace(TAG_CODE), NO_IS_PARENT_TAG_NULL_FLAG))))),
            Sets.newHashSet(addWhitespace(TagServiceImpl.DISPLAY_NAME_FIELD), addWhitespace(TagServiceImpl.SEARCH_SCORE_MULTIPLIER_FIELD),
                addWhitespace(TagServiceImpl.DESCRIPTION_FIELD), addWhitespace(TagServiceImpl.PARENT_TAG_KEY_FIELD),
                addWhitespace(TagServiceImpl.HAS_CHILDREN_FIELD)));

        // Validate the returned object.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN))), tagSearchResponse);
    }

    @Test
    public void testSearchTagsUpperCaseParameters()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagSearchTesting();

        // Search the tags using upper case input parameters.
        TagSearchResponse tagSearchResponse = tagService.searchTags(new TagSearchRequest(
            Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE.toUpperCase(), TAG_CODE.toUpperCase(), NO_IS_PARENT_TAG_NULL_FLAG))))),
            Sets.newHashSet(TagServiceImpl.DISPLAY_NAME_FIELD.toUpperCase(), TagServiceImpl.SEARCH_SCORE_MULTIPLIER_FIELD.toUpperCase(),
                TagServiceImpl.DESCRIPTION_FIELD.toUpperCase(), TagServiceImpl.PARENT_TAG_KEY_FIELD.toUpperCase(),
                TagServiceImpl.HAS_CHILDREN_FIELD.toUpperCase()));

        // Validate the returned object.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN))), tagSearchResponse);
    }

    @Test
    public void testSearchTagsWithIsParentTagNullFlag()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagSearchTesting();

        // Get root tag entities (parent tag must not be set).
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG))), tagService.searchTags(
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, NO_PARENT_TAG_CODE, PARENT_TAG_IS_NULL))))),
            NO_SEARCH_RESPONSE_FIELDS));

        // Get all non-root tag entities (parent tag must be set).
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), NO_TAG_DISPLAY_NAME, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_USER_ID, NO_USER_ID,
                NO_UPDATED_TIME, NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG))), tagService.searchTags(
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, NO_PARENT_TAG_CODE, PARENT_TAG_IS_NOT_NULL))))),
            NO_SEARCH_RESPONSE_FIELDS));
    }

    @Test
    public void testUpdateTag()
    {
        // Create a parent tag key.
        TagKey parentTagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a parent tag entity.
        tagDaoTestHelper.createTagEntity(parentTagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE_2);

        // Create and persist a tag entity without a parent tag.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2);

        // Update the tag.
        Tag updatedTag = tagService.updateTag(tagKey, new TagUpdateRequest(TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, parentTagKey));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, tagEntity.getCreatedBy(),
            tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), parentTagKey, NO_TAG_HAS_CHILDREN_FLAG),
            updatedTag);
    }

    @Test
    public void testUpdateTagDisplayNameAlreadyExistsForOtherTagType()
    {
        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create and persist a second tag entity for the another tag type that would have the display name to be updated to.
        tagDaoTestHelper.createTagEntity(TAG_TYPE_2, TAG_CODE_2, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);

        // Update the tag.
        Tag updatedTag = tagService.updateTag(new TagKey(TAG_TYPE, TAG_CODE),
            new TagUpdateRequest(TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, NO_PARENT_TAG_KEY));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3,
            tagEntity.getCreatedBy(), tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY,
            NO_TAG_HAS_CHILDREN_FLAG), updatedTag);
    }

    @Test
    public void testUpdateTagDisplayNameAlreadyExistsForThisTagType()
    {
        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create and persist a second tag entity for the same tag type that would have the display name to be updated to.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE_2, TAG_DISPLAY_NAME_2.toUpperCase(), TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION);

        // Try to update a tag with an already existing display name.
        try
        {
            tagService.updateTag(new TagKey(TAG_TYPE, TAG_CODE),
                new TagUpdateRequest(TAG_DISPLAY_NAME_2.toLowerCase(), TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, NO_PARENT_TAG_KEY));
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String
                .format("Display name \"%s\" already exists for a tag with tag type \"%s\" and tag code \"%s\".", TAG_DISPLAY_NAME_2.toLowerCase(), TAG_TYPE,
                    TAG_CODE_2), e.getMessage());
        }
    }

    @Test
    public void testUpdateTagInvalidParameters()
    {
        // Try to update a tag using a parent tag with a different tag type code.
        try
        {
            tagService.updateTag(new TagKey(TAG_TYPE, TAG_CODE),
                new TagUpdateRequest(TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, new TagKey(TAG_TYPE_2, TAG_CODE_2)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag type code in parent tag key must match the tag type code in the request.", e.getMessage());
        }

        // Try to update a tag using a negative search score multiplier value.
        try
        {
            tagService.updateTag(new TagKey(TAG_TYPE, TAG_CODE),
                new TagUpdateRequest(TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2.multiply(BigDecimal.valueOf(-1)), TAG_DESCRIPTION_2,
                    new TagKey(TAG_TYPE, TAG_CODE_2)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The searchScoreMultiplier can not have a negative value. searchScoreMultiplier=%s",
                TAG_SEARCH_SCORE_MULTIPLIER_2.multiply(BigDecimal.valueOf(-1))), e.getMessage());
        }
    }

    @Test
    public void testUpdateTagLowerCaseParameters()
    {
        // Create a parent tag key.
        TagKey parentTagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a parent tag entity.
        tagDaoTestHelper.createTagEntity(parentTagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE_2);

        // Create and persist a tag entity without a parent tag.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2);

        // Update the tag using lowercase input parameters.
        Tag updatedTag = tagService.updateTag(new TagKey(TAG_TYPE.toLowerCase(), TAG_CODE_2.toLowerCase()),
            new TagUpdateRequest(TAG_DISPLAY_NAME_3.toLowerCase(), TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3.toLowerCase(),
                new TagKey(TAG_TYPE.toLowerCase(), TAG_CODE.toLowerCase())));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME_3.toLowerCase(), TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3.toLowerCase(),
            tagEntity.getCreatedBy(), tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), parentTagKey,
            NO_TAG_HAS_CHILDREN_FLAG), updatedTag);
    }

    @Test
    public void testUpdateTagMissingOptionalParametersPassedAsNulls()
    {
        // Create a parent tag key.
        TagKey parentTagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a parent tag entity.
        TagEntity parentTagEntity = tagDaoTestHelper.createTagEntity(parentTagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE_2);

        // Create and persist a tag entity with a parent tag.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, parentTagEntity);

        // Update the tag with description and parent tag passed in as nulls.
        Tag updatedTag =
            tagService.updateTag(tagKey, new TagUpdateRequest(TAG_DISPLAY_NAME_3, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, NO_PARENT_TAG_KEY));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME_3, NO_TAG_SEARCH_SCORE_MULTIPLIER, NO_TAG_DESCRIPTION, tagEntity.getCreatedBy(),
            tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            updatedTag);
    }

    @Test
    public void testUpdateTagMissingOptionalParametersPassedAsWhitespace()
    {
        // Create a parent tag key.
        TagKey parentTagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a parent tag entity.
        TagEntity parentTagEntity = tagDaoTestHelper.createTagEntity(parentTagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE_2);

        // Create and persist a tag entity with a parent tag.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, parentTagEntity);

        // Update the tag with description passed as whitespace.
        Tag updatedTag = tagService.updateTag(tagKey, new TagUpdateRequest(TAG_DISPLAY_NAME_3, NO_TAG_SEARCH_SCORE_MULTIPLIER, BLANK_TEXT, NO_PARENT_TAG_KEY));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME_3, NO_TAG_SEARCH_SCORE_MULTIPLIER, BLANK_TEXT, tagEntity.getCreatedBy(),
            tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            updatedTag);
    }

    @Test
    public void testUpdateTagMissingRequiredParameters()
    {
        // Try to update a tag when tag type is not specified.
        try
        {
            tagService.updateTag(new TagKey(BLANK_TEXT, TAG_CODE),
                new TagUpdateRequest(TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to update a tag when tag code is not specified.
        try
        {
            tagService.updateTag(new TagKey(TAG_TYPE, BLANK_TEXT),
                new TagUpdateRequest(TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag code must be specified.", e.getMessage());
        }

        // Try to update a tag when tag display name is not specified.
        try
        {
            tagService
                .updateTag(new TagKey(TAG_TYPE, TAG_CODE), new TagUpdateRequest(BLANK_TEXT, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, NO_PARENT_TAG_KEY));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A display name must be specified.", e.getMessage());
        }

        // Try to update a tag when parent tag type is not specified.
        try
        {
            tagService.updateTag(new TagKey(TAG_TYPE, TAG_CODE_2),
                new TagUpdateRequest(TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, new TagKey(BLANK_TEXT, TAG_CODE)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to update a tag when parent tag code is not specified.
        try
        {
            tagService.updateTag(new TagKey(TAG_TYPE, TAG_CODE_2),
                new TagUpdateRequest(TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, new TagKey(TAG_TYPE, BLANK_TEXT)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag code must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateTagNoChangesToDisplayName()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Update the tag without changing it's display name except for the case.
        Tag updatedTag =
            tagService.updateTag(tagKey, new TagUpdateRequest(TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, NO_PARENT_TAG_KEY));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, tagEntity.getCreatedBy(),
            tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            updatedTag);
    }

    @Test
    public void testUpdateTagNoChangesToDisplayNameExceptForCase()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagKey, TAG_DISPLAY_NAME.toUpperCase(), TAG_DESCRIPTION);

        // Update the tag without changing it's display name except for the case.
        Tag updatedTag = tagService
            .updateTag(tagKey, new TagUpdateRequest(TAG_DISPLAY_NAME.toLowerCase(), TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, NO_PARENT_TAG_KEY));

        // Validate the returned object.
        assertEquals(
            new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME.toLowerCase(), TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, tagEntity.getCreatedBy(),
                tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG),
            updatedTag);
    }

    @Test
    public void testUpdateTagParentTagIsChild()
    {
        // Create and persist a root tag entity.
        TagEntity rootTagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create a child tag entity.
        TagEntity childTagEntity =
            tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE_2, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, rootTagEntity);

        // Create a grandchild tag entity.
        TagEntity grandchildTagEntity =
            tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE_3, TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, childTagEntity);

        // Try to update the root tag using a parent tag set to the tag itself and it's children.
        for (TagEntity tagEntity : Arrays.asList(rootTagEntity, childTagEntity, grandchildTagEntity))
        {
            try
            {
                tagService.updateTag(new TagKey(TAG_TYPE, TAG_CODE), new TagUpdateRequest(TAG_DISPLAY_NAME_4, TAG_SEARCH_SCORE_MULTIPLIER_4, TAG_DESCRIPTION_4,
                    new TagKey(tagEntity.getTagType().getCode(), tagEntity.getTagCode())));
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Parent tag key cannot be the requested tag key or any of its children’s tag keys.", e.getMessage());
            }
        }
    }

    @Test
    public void testUpdateTagParentTagMaxAllowedNestingExceeds() throws Exception
    {
        // Create and persist a root tag entity.
        TagEntity rootTagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create a child tag entity.
        TagEntity childTagEntity =
            tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE_2, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, rootTagEntity);

        // Create a grandchild tag entity.
        TagEntity grandchildTagEntity =
            tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE_3, TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, childTagEntity);

        // Override the configuration to set max allowed tag nesting to 1.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.MAX_ALLOWED_TAG_NESTING.getKey(), 1);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Try to update the tag using it's child as a parent tag.
            try
            {
                tagService.updateTag(new TagKey(TAG_TYPE, TAG_CODE), new TagUpdateRequest(TAG_DISPLAY_NAME_4, TAG_SEARCH_SCORE_MULTIPLIER_4, TAG_DESCRIPTION_4,
                    new TagKey(childTagEntity.getTagType().getCode(), childTagEntity.getTagCode())));
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Parent tag key cannot be the requested tag key or any of its children’s tag keys.", e.getMessage());
            }

            // Try to update the tag using it's grandchild as a parent tag.
            try
            {
                tagService.updateTag(new TagKey(TAG_TYPE, TAG_CODE), new TagUpdateRequest(TAG_DISPLAY_NAME_4, TAG_SEARCH_SCORE_MULTIPLIER_4, TAG_DESCRIPTION_4,
                    new TagKey(grandchildTagEntity.getTagType().getCode(), grandchildTagEntity.getTagCode())));
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Exceeds maximum allowed tag nesting level of 1", e.getMessage());
            }
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testUpdateTagParentTagNoExists()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE_2);

        // Create and persist a tag entity without a parent tag.
        tagDaoTestHelper.createTagEntity(tagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Try to update a tag using a non-existing parent tag.
        try
        {
            tagService.updateTag(tagKey, new TagUpdateRequest(TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, new TagKey(TAG_TYPE, TAG_CODE)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag with code \"%s\" doesn't exist for tag type \"%s\".", TAG_CODE, TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testUpdateTagTagNoExists()
    {
        // Try to update a non-existing tag.
        try
        {
            tagService.updateTag(new TagKey(TAG_TYPE, TAG_CODE_2),
                new TagUpdateRequest(TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION, new TagKey(TAG_TYPE, TAG_CODE)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag with code \"%s\" doesn't exist for tag type \"%s\".", TAG_CODE_2, TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testUpdateTagTrimParameters()
    {
        // Create a parent tag key.
        TagKey parentTagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a parent tag entity.
        tagDaoTestHelper.createTagEntity(parentTagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE_2);

        // Create and persist a tag entity without a parent tag.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2);

        // Update the tag using input parameters with leading and trailing empty spaces.
        Tag updatedTag = tagService.updateTag(new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG_CODE_2)),
            new TagUpdateRequest(addWhitespace(TAG_DISPLAY_NAME_3), TAG_SEARCH_SCORE_MULTIPLIER_3, addWhitespace(TAG_DESCRIPTION_3),
                new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG_CODE.toLowerCase()))));

        // Validate the returned object.
        assertEquals(
            new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_3, addWhitespace(TAG_DESCRIPTION_3), tagEntity.getCreatedBy(),
                tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), parentTagKey, NO_TAG_HAS_CHILDREN_FLAG),
            updatedTag);
    }

    @Test
    public void testUpdateTagUpperCaseParameters()
    {
        // Create a parent tag key.
        TagKey parentTagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create a parent tag entity.
        tagDaoTestHelper.createTagEntity(parentTagKey, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE_2);

        // Create and persist a tag entity without a parent tag.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagKey, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2);

        // Update the tag using uppercase input parameters.
        Tag updatedTag = tagService.updateTag(new TagKey(TAG_TYPE.toUpperCase(), TAG_CODE_2.toUpperCase()),
            new TagUpdateRequest(TAG_DISPLAY_NAME_3.toUpperCase(), TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3.toUpperCase(),
                new TagKey(TAG_TYPE.toUpperCase(), TAG_CODE.toUpperCase())));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME_3.toUpperCase(), TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3.toUpperCase(),
            tagEntity.getCreatedBy(), tagEntity.getUpdatedBy(), HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()), parentTagKey,
            NO_TAG_HAS_CHILDREN_FLAG), updatedTag);
    }

    /**
     * Creates database entities required for the tag search service unit tests.
     */
    private void createDatabaseEntitiesForTagSearchTesting()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Create a root tag entity for the tag type.
        TagEntity rootTagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME, TAG_SEARCH_SCORE_MULTIPLIER, TAG_DESCRIPTION);

        // Create two children for the root tag with tag display name in reverse order.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME_3, TAG_SEARCH_SCORE_MULTIPLIER_2, TAG_DESCRIPTION_2, rootTagEntity);
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_3, TAG_DISPLAY_NAME_2, TAG_SEARCH_SCORE_MULTIPLIER_3, TAG_DESCRIPTION_3, rootTagEntity);
    }
}
