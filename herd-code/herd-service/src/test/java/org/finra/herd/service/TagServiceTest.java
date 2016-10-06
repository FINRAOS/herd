/*
* Copyright 20INTEGER_VALUE5 herd contributors
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

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagKeys;
import org.finra.herd.model.api.xml.TagUpdateRequest;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;

public class TagServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateTag()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create a tag.
        Tag tag = tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION));

        // Validate the tag which was created.
        assertEquals(new Tag(tag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION), tag);
    }

    @Test
    public void testCreateTagOnlyRequiredParams()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create a tag.
        Tag tag = tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, null));

        // Validate the tag which was created.
        assertEquals(new Tag(tag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, null), tag);
    }

    @Test
    public void testCreateTagMissingRequiredParams()
    {
        // Missing tag key.
        try
        {
            tagService.createTag(new TagCreateRequest(null, TAG_DISPLAY_NAME, TAG_DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag key must be specified.", e.getMessage());
        }

        // Missing tag type code in the key.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(null, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Missing tag code in the key.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, null), TAG_DISPLAY_NAME, TAG_DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag code must be specified.", e.getMessage());
        }

        // Missing display name in the request.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG), null, TAG_DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A display name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateTagMissingOptionalParametersPassedAsWhitespace()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create a tag with description passed in as whitespace.
        Tag tag = tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, BLANK_TEXT));

        // Validate the tag which was created.
        assertEquals(new Tag(tag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, BLANK_TEXT), tag);
    }

    @Test
    public void testCreateTagMissingOptionalParametersPassedAsNulls()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create a tag with description passed in as null.
        Tag tag = tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, null));

        // Validate the tag which was created.
        assertEquals(new Tag(tag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, null), tag);
    }

    @Test
    public void testCreateTagTrimParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create a tag with parameters padded with whitespace.
        Tag tag = tagService.createTag(
            new TagCreateRequest(new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG)), addWhitespace(TAG_DISPLAY_NAME), addWhitespace(TAG_DESCRIPTION)));

        // Validate the tag which was created.
        assertEquals(new Tag(tag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, addWhitespace(TAG_DESCRIPTION)), tag);
    }

    @Test
    public void testCreateTagUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create a tag using uppercase input parameters.
        Tag resultTag = tagService
            .createTag(new TagCreateRequest(new TagKey(TAG_TYPE.toUpperCase(), TAG.toUpperCase()), TAG_DISPLAY_NAME.toUpperCase(), DESCRIPTION.toUpperCase()));

        // Validate the returned object.
        assertEquals(new Tag(resultTag.getId(), new TagKey(TAG_TYPE, TAG.toUpperCase()), TAG_DISPLAY_NAME.toUpperCase(), DESCRIPTION.toUpperCase()), resultTag);
    }


    @Test
    public void testCreateTagLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create a tag using lowercase input parameters.
        Tag resultTag = tagService
            .createTag(new TagCreateRequest(new TagKey(TAG_TYPE.toLowerCase(), TAG.toLowerCase()), TAG_DISPLAY_NAME.toLowerCase(), DESCRIPTION.toLowerCase()));

        // Validate the returned object.
        assertEquals(new Tag(resultTag.getId(), new TagKey(TAG_TYPE, TAG.toLowerCase()), TAG_DISPLAY_NAME.toLowerCase(), DESCRIPTION.toLowerCase()), resultTag);
    }

    @Test
    public void testCreateTagInvalidParameters()
    {
        // Try to create a tag when tag type contains a forward slash character.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(addSlash(TAG_TYPE), TAG), TAG_DISPLAY_NAME, DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag type code can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a tag when tag code contains a forward slash character.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, addSlash(TAG)), TAG_DISPLAY_NAME, DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag code can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateTagTagTypeNoExists()
    {
        // Try to create a tag using non-existing tag type.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, DESCRIPTION));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag type with code \"%s\" doesn't exist.", TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testCreateTagTagCodeAlreadyExists()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Try to create a duplicate tag (uses the same tag type and tag name).
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG.toLowerCase()), TAG_DISPLAY_NAME_2, DESCRIPTION));
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(
                String.format("Unable to create tag with tag type code \"%s\" and tag code \"%s\" because it already exists.", TAG_TYPE, TAG.toLowerCase()),
                e.getMessage());
        }
    }

    @Test
    public void testCreateTagDisplayNameAlreadyExists()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Try to create a tag with a duplicate tag display name.
        try
        {
            tagService.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG_2), TAG_DISPLAY_NAME.toLowerCase(), DESCRIPTION));
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String
                .format("Display name \"%s\" already exists for a tag with tag type \"%s\" and tag code \"%s\".", TAG_DISPLAY_NAME.toLowerCase(), TAG_TYPE,
                    TAG), e.getMessage());
        }
    }

    @Test
    public void testGetTag()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Retrieve the tag.
        Tag resultTag = tagService.getTag(new TagKey(TAG_TYPE, TAG));

        // Validate the returned object.
        assertEquals(new Tag(resultTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION), resultTag);
    }

    @Test
    public void testGetTagMissingRequiredParameters()
    {
        // Try to get a tag when tag type is not specified.
        try
        {
            tagService.getTag(new TagKey(BLANK_TEXT, TAG));
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
    public void testGetTagTrimParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Retrieve the tag using input parameters with leading and trailing empty spaces.
        Tag resultTag = tagService.getTag(new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG)));

        // Validate the returned object.
        assertEquals(new Tag(resultTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION), resultTag);
    }

    @Test
    public void testGetTagUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Get the tag using uppercase input parameters.
        Tag resultTag = tagService.getTag(new TagKey(TAG_TYPE.toUpperCase(), TAG.toUpperCase()));

        // Validate the returned object.
        assertEquals(new Tag(resultTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION), resultTag);
    }

    @Test
    public void testGetTagLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Get the tag using lower case input parameters.
        Tag resultTag = tagService.getTag(new TagKey(TAG_TYPE.toLowerCase(), TAG.toLowerCase()));

        // Validate the returned object.
        assertEquals(new Tag(resultTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION), resultTag);
    }

    @Test
    public void testGetTagTagNoExists()
    {
        // Try to get a non-existing tag.
        try
        {
            tagService.getTag(new TagKey(TAG_TYPE, TAG));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag not found with the tag type code \"%s\" and tag code \"%s\".", TAG_TYPE, TAG), e.getMessage());
        }
    }

    @Test
    public void testUpdateTag()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Update the tag.
        Tag updatedTag = tagService.updateTag(new TagKey(TAG_TYPE, TAG), new TagUpdateRequest(TAG_DISPLAY_NAME_2, DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new Tag(updatedTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME_2, DESCRIPTION_2), updatedTag);
    }

    @Test
    public void testUpdateTagMissingRequiredParameters()
    {
        // Try to update a tag when tag type is not specified.
        try
        {
            tagService.updateTag(new TagKey(BLANK_TEXT, TAG), new TagUpdateRequest(TAG_DISPLAY_NAME, DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to update a tag when tag code is not specified.
        try
        {
            tagService.updateTag(new TagKey(TAG_TYPE, BLANK_TEXT), new TagUpdateRequest(TAG_DISPLAY_NAME, DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag code must be specified.", e.getMessage());
        }

        // Try to update a tag when tag display name is not specified.
        try
        {
            tagService.updateTag(new TagKey(TAG_TYPE, TAG), new TagUpdateRequest(BLANK_TEXT, DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A display name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateTagTrimParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        Tag updatedTag = tagService.updateTag(new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG)),
            new TagUpdateRequest(addWhitespace(TAG_DISPLAY_NAME_2), addWhitespace(DESCRIPTION_2)));

        // Validate the returned object.
        assertEquals(new Tag(updatedTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME_2, addWhitespace(DESCRIPTION_2)), updatedTag);
    }

    @Test
    public void testUpdateTagUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Update the tag using input parameters with leading and trailing empty spaces.
        Tag updatedTag = tagService.updateTag(new TagKey(TAG_TYPE.toUpperCase(), TAG.toUpperCase()),
            new TagUpdateRequest(TAG_DISPLAY_NAME_2.toUpperCase(), DESCRIPTION_2.toUpperCase()));

        // Validate the returned object.
        assertEquals(new Tag(updatedTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME_2.toUpperCase(), DESCRIPTION_2.toUpperCase()), updatedTag);
    }

    @Test
    public void testUpdateTagLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Update the tag using input parameters with leading and trailing empty spaces.
        Tag updatedTag = tagService.updateTag(new TagKey(TAG_TYPE.toLowerCase(), TAG.toLowerCase()),
            new TagUpdateRequest(TAG_DISPLAY_NAME_2.toLowerCase(), DESCRIPTION_2.toLowerCase()));

        // Validate the returned object.
        assertEquals(new Tag(updatedTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME_2.toLowerCase(), DESCRIPTION_2.toLowerCase()), updatedTag);
    }

    @Test
    public void testUpdateTagTagNoExists()
    {
        // Try to update a non-existing tag.
        try
        {
            tagService.updateTag(new TagKey(TAG_TYPE, TAG), new TagUpdateRequest(TAG_DISPLAY_NAME, DESCRIPTION));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag not found with the tag type code \"%s\" and tag code \"%s\".", TAG_TYPE, TAG), e.getMessage());
        }
    }

    @Test
    public void testUpdateTagDisplayNameAlreadyExistsForThisTagType()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity for the same tag type that would have the display name to be updated to.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_2, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2);

        // Update the tag by only changing case of the display name.
        Tag updatedTag = tagService.updateTag(new TagKey(TAG_TYPE, TAG_2), new TagUpdateRequest(TAG_DISPLAY_NAME_2.toLowerCase(), TAG_DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new Tag(updatedTag.getId(), new TagKey(TAG_TYPE, TAG_2), TAG_DISPLAY_NAME_2.toLowerCase(), TAG_DESCRIPTION_2), updatedTag);
    }

    @Test
    public void testUpdateTagDisplayNameAlreadyExistsForOtherTagType()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist another tag type entity.
        TagTypeEntity tagTypeEntityTwo = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE_2, TAG_TYPE_DISPLAY_NAME_2, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Create and persist a second tag entity for the another tag type that would have the display name to be updated to.
        tagDaoTestHelper.createTagEntity(tagTypeEntityTwo, TAG_2, TAG_DISPLAY_NAME_2, DESCRIPTION);

        // Update the tag.
        Tag updatedTag = tagService.updateTag(new TagKey(TAG_TYPE, TAG), new TagUpdateRequest(TAG_DISPLAY_NAME_2, DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new Tag(updatedTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME_2, DESCRIPTION_2), updatedTag);
    }

    @Test
    public void testUpdateTagNoChangesToDisplayName()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Update the tag with out changing the display name.
        Tag updatedTag = tagService.updateTag(new TagKey(TAG_TYPE, TAG), new TagUpdateRequest(TAG_DISPLAY_NAME, TAG_DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new Tag(updatedTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION_2), updatedTag);
    }

    @Test
    public void testUpdateTagNoChangesToDisplayNameExceptForCase()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Update the tag with out changing the display name.
        Tag updatedTag = tagService.updateTag(new TagKey(TAG_TYPE, TAG), new TagUpdateRequest(TAG_DISPLAY_NAME.toLowerCase(), DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new Tag(updatedTag.getId(), new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME.toLowerCase(), DESCRIPTION_2), updatedTag);
    }

    @Test
    public void testDeleteTag()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG);

        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Validate that this tag exists.
        assertNotNull(tagDao.getTagByKey(tagKey));

        // Delete this tag.
        Tag deletedTag = tagService.deleteTag(new TagKey(TAG_TYPE, TAG));

        // Validate the returned object.
        assertEquals(new Tag(deletedTag.getId(), tagKey, TAG_DISPLAY_NAME, TAG_DESCRIPTION), deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(tagKey));
    }

    @Test
    public void testDeleteTagMissingRequiredParameters()
    {
        // Try to delete a tag when tag type is not specified.
        try
        {
            tagService.deleteTag(new TagKey(BLANK_TEXT, TAG));
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
    public void testDeleteTagTrimParameters()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG);

        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Validate that this tag exists.
        assertNotNull(tagDao.getTagByKey(tagKey));

        // Delete this tag using input parameters with leading and trailing empty spaces.
        Tag deletedTag = tagService.deleteTag(new TagKey(addWhitespace(TAG_TYPE), addWhitespace(TAG)));

        // Validate the returned object.
        assertEquals(new Tag(deletedTag.getId(), tagKey, TAG_DISPLAY_NAME, TAG_DESCRIPTION), deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(tagKey));
    }

    @Test
    public void testDeleteTagUpperCaseParameters()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG);

        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Validate that this tag exists.
        assertNotNull(tagDao.getTagByKey(tagKey));

        // Delete this tag using uppercase input parameters.
        Tag deletedTag = tagService.deleteTag(new TagKey(TAG_TYPE.toUpperCase(), TAG.toUpperCase()));

        // Validate the returned object.
        assertEquals(new Tag(deletedTag.getId(), tagKey, TAG_DISPLAY_NAME, TAG_DESCRIPTION), deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(tagKey));
    }

    @Test
    public void testDeleteTagLowerCaseParameters()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG);

        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Validate that this tag exists.
        assertNotNull(tagDao.getTagByKey(tagKey));

        // Delete this tag using uppercase input parameters.
        Tag deletedTag = tagService.deleteTag(new TagKey(TAG_TYPE.toLowerCase(), TAG.toLowerCase()));

        // Validate the returned object.
        assertEquals(new Tag(deletedTag.getId(), tagKey, TAG_DISPLAY_NAME, TAG_DESCRIPTION), deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(tagKey));
    }

    @Test
    public void testDeleteTagTagNoExists()
    {
        // Try to delete a non-existing tag.
        try
        {
            tagService.deleteTag(new TagKey(TAG_TYPE, TAG));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag not found with the tag type code \"%s\" and tag code \"%s\".", TAG_TYPE, TAG), e.getMessage());
        }
    }

    @Test
    public void testGetTags()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entities.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_2, TAG_TYPE_DISPLAY_NAME_2, DESCRIPTION);

        // Retrieve a list of tag keys.
        TagKeys resultTagKeys = tagService.getTags(TAG_TYPE);

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(Arrays.asList(new TagKey(TAG_TYPE, TAG), new TagKey(TAG_TYPE, TAG_2)), resultTagKeys.getTagKeys());
    }

    @Test
    public void testGetTagsMissingRequiredParameters()
    {
        // Try to get a tag when tag type is not specified.
        try
        {
            tagService.getTags(BLANK_TEXT);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag type with code \"%s\" doesn't exist.", BLANK_TEXT), e.getMessage());
        }
    }

    @Test
    public void testGetTagsTrimParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entities.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_2, TAG_TYPE_DISPLAY_NAME_2, TAG_DESCRIPTION_2);

        // Retrieve a list of tag keys using input parameters with leading and trailing empty spaces.
        TagKeys resultTagKeys = tagService.getTags(addWhitespace(TAG_TYPE));

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(Arrays.asList(new TagKey(TAG_TYPE, TAG), new TagKey(TAG_TYPE, TAG_2)), resultTagKeys.getTagKeys());
    }

    @Test
    public void testGetTagsUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entities.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_2, TAG_TYPE_DISPLAY_NAME_2, DESCRIPTION);

        // Retrieve a list of tag keys using uppercase input parameters.
        TagKeys resultTagKeys = tagService.getTags(TAG_TYPE.toUpperCase());

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(Arrays.asList(new TagKey(TAG_TYPE, TAG), new TagKey(TAG_TYPE, TAG_2)), resultTagKeys.getTagKeys());
    }

    @Test
    public void testGetTagsLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create and persist a tag entities.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_2, TAG_TYPE_DISPLAY_NAME_2, DESCRIPTION);

        // Retrieve a list of tag keys using lowercase input parameters.
        TagKeys resultTagKeys = tagService.getTags(TAG_TYPE.toLowerCase());

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(Arrays.asList(new TagKey(TAG_TYPE, TAG), new TagKey(TAG_TYPE, TAG_2)), resultTagKeys.getTagKeys());
    }

    @Test
    public void testGetTagsTagTypeNoExists()
    {
        // Try to retrieve a list of tag keys for a non-existing tag type.
        try
        {
            tagService.getTags(TAG_TYPE);
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
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Retrieve a list of tag keys, when none of the tags exist.
        TagKeys resultTagKeys = tagService.getTags(TAG_TYPE);

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(0, resultTagKeys.getTagKeys().size());
    }

}
