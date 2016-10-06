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

import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagUpdateRequest;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;

/**
 * This class tests various functionality within the Tag REST controller
 */
public class TagRestControllerTest extends AbstractRestTest
{
    /**
     * Creates a tag type entity and persists it.
     *
     * @param code the specified tag type code.
     * @param displayName the specified tag type displayName.
     * @param position the specified tag type position.
     *
     * @return the newly created tag type entity.
     */
    private TagTypeEntity createTagType(final String code, final String displayName, final int position)
    {
        return tagTypeDaoTestHelper.createTagTypeEntity(code, displayName, position);
    }

    /**
     * Creates a tag entity and persists it.
     *
     * @param tagTypeEntity the specified tag type entity.
     * @param tagCode the specified tag code.
     * @param displayName the specified display name.
     * @param description the description.
     *
     * @return the newly created tag entity.
     */
    private TagEntity createTag(TagTypeEntity tagTypeEntity, String tagCode, String displayName, String description)
    {
        return tagDaoTestHelper.createTagEntity(tagTypeEntity, tagCode, displayName, description);
    }

    @Test
    public void testCreateTag()
    {
        createTagType(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);
        TagKey tagKey = new TagKey(TAG_TYPE, TAG);
        Tag tag = tagRestController.createTag(new TagCreateRequest(tagKey, TAG_DISPLAY_NAME, TAG_DESCRIPTION));

        Assert.assertEquals(new Tag(new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION), tag);
    }

    @Test
    public void testGetTag()
    {
        TagTypeEntity tagTypeEntity = createTagType(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);
        createTag(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        Tag tag = tagRestController.getTag(TAG_TYPE, TAG);
        Assert.assertEquals(new Tag(new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION), tag);
    }

    @Test
    public void testUpdateTag()
    {
        TagTypeEntity tagTypeEntity = createTagType(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);
        createTag(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        Tag tag = tagRestController.updateTag(TAG_TYPE, TAG, new TagUpdateRequest("newDisplayName", "newDescription"));
        Assert.assertEquals(new Tag(new TagKey(TAG_TYPE, TAG), "newDisplayName", "newDescription"), tag);
    }

    @Test
    public void testDeleteTag()
    {
        TagTypeEntity tagTypeEntity = createTagType(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);
        createTag(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        Tag tag = tagRestController.deleteTag(TAG_TYPE, TAG);
        Assert.assertEquals(new Tag(new TagKey(TAG_TYPE, TAG), TAG_DISPLAY_NAME, TAG_DESCRIPTION), tag);

        TagEntity tagEntity = tagDao.getTagByKey(new TagKey(TAG_TYPE, TAG));
        Assert.assertNull(tagEntity);
    }
}
