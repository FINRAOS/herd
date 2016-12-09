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
import java.util.List;

import com.google.common.collect.Sets;
import org.junit.Test;

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
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.service.impl.TagServiceImpl;

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
        Tag tag = tagRestController.createTag(new TagCreateRequest(new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION, null));

        // Validate the tag which was created.
        assertEquals(new Tag(tag.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION, tag.getUserId(), tag.getLastUpdatedByUserId(),
            tag.getUpdatedTime(), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG), tag);
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
        assertEquals(new Tag(tagEntity.getId(), tagKey, TAG_DISPLAY_NAME, DESCRIPTION, deletedTag.getUserId(), deletedTag.getLastUpdatedByUserId(),
            deletedTag.getUpdatedTime(), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG), deletedTag);

        // Ensure that this tag is no longer there.
        assertNull(tagDao.getTagByKey(tagKey));
    }

    @Test
    public void testGetTag()
    {
        // Create and persist a tag entity.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Retrieve the tag.
        Tag resultTag = tagRestController.getTag(TAG_TYPE, TAG_CODE);

        // Validate the returned object.
        assertEquals(new Tag(resultTag.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME, TAG_DESCRIPTION, resultTag.getUserId(),
            resultTag.getLastUpdatedByUserId(), resultTag.getUpdatedTime(), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG), resultTag);
    }

    @Test
    public void testGetTags()
    {
        // Create and persist tag entities.
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_TYPE_DISPLAY_NAME, DESCRIPTION);
        tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE_2, TAG_TYPE_DISPLAY_NAME_2, DESCRIPTION);

        // Retrieve a list of tag keys.
        TagListResponse resultTagKeys = tagRestController.getTags(TAG_TYPE, null);
        // Retrieve a list of tag keys using uppercase input parameters.
        List<TagChild> tagChildren = new ArrayList<>();
        tagChildren.add(new TagChild(new TagKey(TAG_TYPE, TAG_CODE), false));
        tagChildren.add(new TagChild(new TagKey(TAG_TYPE, TAG_CODE_2), false));

        // Validate the returned object.
        assertNotNull(resultTagKeys);
        assertEquals(tagChildren, resultTagKeys.getTagChildren());
    }

    @Test
    public void testSearchTags()
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER);

        // Create a root tag entity for the tag type.
        TagEntity rootTagEntity = tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Create two children for the root tag with tag display name in reverse order.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_2, TAG_DISPLAY_NAME_3, TAG_DESCRIPTION_2, rootTagEntity);
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG_CODE_3, TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_3, rootTagEntity);

        // Search the tags.
        TagSearchResponse tagSearchResponse = tagRestController.searchTags(
            new TagSearchRequest(Arrays.asList(new TagSearchFilter(Arrays.asList(new TagSearchKey(TAG_TYPE, TAG_CODE, NO_IS_PARENT_TAG_NULL_FLAG))))),
            Sets.newHashSet(TagServiceImpl.DISPLAY_NAME_FIELD, TagServiceImpl.DESCRIPTION_FIELD, TagServiceImpl.PARENT_TAG_KEY_FIELD,
                TagServiceImpl.HAS_CHILDREN_FIELD));

        // Validate the returned object.
        assertEquals(new TagSearchResponse(Arrays.asList(
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_3), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_3, NO_USER_ID, NO_USER_ID, NO_UPDATED_TIME,
                new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN),
            new Tag(NO_ID, new TagKey(TAG_TYPE, TAG_CODE_2), TAG_DISPLAY_NAME_3, TAG_DESCRIPTION_2, NO_USER_ID, NO_USER_ID, NO_UPDATED_TIME,
                new TagKey(TAG_TYPE, TAG_CODE), TAG_HAS_NO_CHILDREN))), tagSearchResponse);
    }

    @Test
    public void testUpdateTag()
    {
        // Create and persist a tag entity.
        TagEntity tagEntity = tagDaoTestHelper.createTagEntity(TAG_TYPE, TAG_CODE, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Update the tag.
        Tag updatedTag = tagRestController.updateTag(TAG_TYPE, TAG_CODE, new TagUpdateRequest(TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2, null));

        // Validate the returned object.
        assertEquals(new Tag(tagEntity.getId(), new TagKey(TAG_TYPE, TAG_CODE), TAG_DISPLAY_NAME_2, TAG_DESCRIPTION_2, updatedTag.getUserId(),
            updatedTag.getLastUpdatedByUserId(), updatedTag.getUpdatedTime(), NO_PARENT_TAG_KEY, NO_TAG_HAS_CHILDREN_FLAG), updatedTag);
    }
}
