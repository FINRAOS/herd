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

import java.util.Arrays;

import org.junit.Test;

import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagKeys;
import org.finra.herd.model.api.xml.TagUpdateRequest;
import org.finra.herd.model.jpa.TagEntity;

/**
 * This class tests various functionality within the Tag REST controller
 */
public class TagRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateTag()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Create a tag.
        Tag tag = tagRestController.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION));

        // Validate the tag which was created.
        assertEquals(new Tag(tag.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION), tag);
    }

    @Test
    public void testGetTag()
    {
        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Retrieve the tag.
        Tag resultTag = tagRestController.getTag(TAG_TYPE, TAG_CODE);

        // Validate the returned object.
        assertEquals(new Tag(resultTag.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION), resultTag);
    }

    @Test
    public void testUpdateTag()
    {
        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Update the tag.
        Tag updatedTag = tagRestController.updateTag(TAG_TYPE, TAG_CODE, new TagUpdateRequest(TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2), updatedTag);
    }

    @Test
    public void testDeleteTag()
    {
        // Create a tag key.
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, DESCRIPTION);

        // Validate that this tag exists.
        assertNotNull(tagDao.getTagByKey(tagKey));

        // Delete this tag.
        Tag deletedTag = tagRestController.deleteTag(TAG_TYPE, TAG_CODE);

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME, DESCRIPTION), deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(tagKey));
    }

    @Test
    public void testGetTags()
    {
        // Create and persist tag entities.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_TYPE_DISPLAY_NAME, DESCRIPTION);
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE_2, TAG_TYPE_DISPLAY_NAME_2, DESCRIPTION);

        // Retrieve a list of tag keys.
        TagKeys resultTagKeys = tagRestController.getTags(TAG_TYPE);

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(Arrays.asList(new TagKey(TAG_TYPE, TAG_CODE), new TagKey(TAG_TYPE, TAG_CODE_2)), resultTagKeys.getTagKeys());
    }
}
