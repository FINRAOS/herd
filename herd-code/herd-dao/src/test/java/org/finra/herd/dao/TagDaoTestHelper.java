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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;

@Component
public class TagDaoTestHelper
{
    @Autowired
    private TagDao tagDao;

    @Autowired
    private TagTypeDao tagTypeDao;

    @Autowired
    private TagTypeDaoTestHelper tagTypeDaoTestHelper;

    /**
     * Creates and persists a new tag entity.
     *
     * @param tagKey the tag key
     * @param tagDisplayName the tag display name
     * @param tagDescription the description of the tag
     *
     * @return the newly created tag entity
     */
    public TagEntity createTagEntity(TagKey tagKey, String tagDisplayName, String tagDescription)
    {
        return createTagEntity(tagKey.getTagTypeCode(), tagKey.getTagCode(), tagDisplayName, tagDescription);
    }

    /**
     * Creates and persists a new tag entity.
     *
     * @param tagType the tag type
     * @param tagCode the tag code
     * @param tagDisplayName the tag display name
     * @param tagDescription the description of the tag
     *
     * @return the newly created tag entity
     */
    public TagEntity createTagEntity(String tagType, String tagCode, String tagDisplayName, String tagDescription)
    {
        return createTagEntity(tagType, tagCode, tagDisplayName, tagDescription, null);
    }

    /**
     * Creates and persists a new tag entity.
     *
     * @param tagKey the tag key
     * @param tagDisplayName the tag display name
     * @param tagDescription the description of the tag
     * @param parentTagEntity the parent tag entity
     *
     * @return the newly created tag entity
     */
    public TagEntity createTagEntity(TagKey tagKey, String tagDisplayName, String tagDescription, TagEntity parentTagEntity)
    {
        return createTagEntity(tagKey.getTagTypeCode(), tagKey.getTagCode(), tagDisplayName, tagDescription, parentTagEntity);
    }

    /**
     * Creates and persists a new tag entity.
     *
     * @param tagType the tag type entity
     * @param tagCode the tag code
     * @param tagDisplayName the tag display name
     * @param tagDescription the description of the tag
     * @param parentTagEntity the parent tag entity
     *
     * @return the newly created tag entity
     */
    public TagEntity createTagEntity(String tagType, String tagCode, String tagDisplayName, String tagDescription, TagEntity parentTagEntity)
    {
        // Create a tag type entity if needed.
        TagTypeEntity tagTypeEntity = tagTypeDao.getTagTypeByKey(new TagTypeKey(tagType));
        if (tagTypeEntity == null)
        {
            tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(tagType, AbstractDaoTest.TAG_TYPE_DISPLAY_NAME, AbstractDaoTest.TAG_TYPE_ORDER);
        }

        return createTagEntity(tagTypeEntity, tagCode, tagDisplayName, tagDescription, parentTagEntity);
    }

    /**
     * Creates and persists a new tag entity.
     *
     * @param tagTypeEntity the tag type entity
     * @param tagCode the tag code
     * @param tagDisplayName the tag display name
     * @param tagDescription the description of the tag
     *
     * @return the newly created tag entity
     */
    public TagEntity createTagEntity(TagTypeEntity tagTypeEntity, String tagCode, String tagDisplayName, String tagDescription)
    {
        return createTagEntity(tagTypeEntity, tagCode, tagDisplayName, tagDescription, null);
    }

    /**
     * Creates and persists a new tag entity.
     *
     * @param tagTypeEntity the tag type entity
     * @param tagCode the tag code
     * @param tagDisplayName the tag display name
     * @param tagDescription the description of the tag
     * @param parentTagEntity the parent tag entity
     *
     * @return the newly created tag entity
     */
    public TagEntity createTagEntity(TagTypeEntity tagTypeEntity, String tagCode, String tagDisplayName, String tagDescription, TagEntity parentTagEntity)
    {
        TagEntity tagEntity = new TagEntity();

        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(tagCode);
        tagEntity.setDisplayName(tagDisplayName);
        tagEntity.setDescription(tagDescription);
        tagEntity.setParentTagEntity(parentTagEntity);

        return tagDao.saveAndRefresh(tagEntity);
    }
}
