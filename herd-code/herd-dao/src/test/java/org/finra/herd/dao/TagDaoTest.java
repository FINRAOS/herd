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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;

public class TagDaoTest extends AbstractDaoTest
{
    /**
     * Validate a tag entity.
     *
     * @param tagEntity the tag entity to be validated.
     */
    private void validateTagEntity(TagEntity tagEntity)
    {
        Assert.assertNotNull(tagEntity);
        Assert.assertEquals("Unexpected tag type code.", TAG_TYPE, tagEntity.getTagType().getTypeCode());
        Assert.assertEquals("Unexpected tag code.", TAG, tagEntity.getTagCode());
        Assert.assertEquals("Unexpected display name.", TAG_DISPLAY_NAME, tagEntity.getDisplayName());
        Assert.assertEquals("Unexpected tag description.", TAG_DESCRIPTION, tagEntity.getDescription());
    }

    @Test
    public void testGetTagByKey()
    {
        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Create a tag entity.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Get tag entity and validate
        TagEntity tagEntity = tagDao.getTagByKey(new TagKey(TAG_TYPE, TAG));
        validateTagEntity(tagEntity);
    }

    @Test
    public void testGetTagByTagTypeAndDisplayName()
    {
        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Create a tag entity.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        // Get tag entity and validate
        TagEntity tagEntity = tagDao.getTagByTagTypeAndDisplayName(TAG_TYPE, TAG_DISPLAY_NAME);
        validateTagEntity(tagEntity);
    }

    @Test
    public void testGetTagsByTagType()
    {
        // Create a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Create a tag entity.
        tagDaoTestHelper.createTagEntity(tagTypeEntity, TAG, TAG_DISPLAY_NAME, TAG_DESCRIPTION);

        List<TagKey> resultTagKeys = tagDao.getTagsByTagType(TAG_TYPE);

        Assert.assertNotNull(resultTagKeys);
        Assert.assertTrue("Unexpected number of tags found", resultTagKeys.size() == 1);
        Assert.assertEquals("Unexpected tag type code.", TAG_TYPE, resultTagKeys.get(0).getTagTypeCode());
        Assert.assertEquals("Unexpected tag code.", TAG, resultTagKeys.get(0).getTagCode());
    }

    @Test
    public void testGetTagsByMultipleTagType()
    {
        //todo
    }

}
