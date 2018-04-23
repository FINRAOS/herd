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
package org.finra.herd.dao.helper;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.TagDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.TagEntity;

@Component
public class TagDaoHelper
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private TagDao tagDao;

    /**
     * Ensures that a tag entity does not exist for a specified tag type code and display name.
     *
     * @param tagCode the specified tag type code.
     * @param displayName the specified display name.
     */
    public void assertDisplayNameDoesNotExistForTag(String tagCode, String displayName)
    {
        TagEntity tagEntity = tagDao.getTagByTagTypeAndDisplayName(tagCode, displayName);

        if (tagEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Display name \"%s\" already exists for a tag with tag type \"%s\" and tag code \"%s\".", displayName, tagEntity.getTagType().getCode(),
                    tagEntity.getTagCode()));
        }
    }

    /**
     * Create a list of tag entities along with all its children tags down the hierarchy up to maximum allowed tag nesting level.
     *
     * @param parentTagEntity the parent tag entity
     *
     * @return the list of tag children entities
     */
    public List<TagEntity> getTagChildrenEntities(TagEntity parentTagEntity)
    {
        // Get the maximum allowed tag nesting level.
        Integer maxAllowedTagNesting = configurationHelper.getProperty(ConfigurationValue.MAX_ALLOWED_TAG_NESTING, Integer.class);

        // Build a list of the specified tag along with all its children tags down the hierarchy up to maximum allowed tag nesting level.
        List<TagEntity> parentTagEntities = new ArrayList<>();
        parentTagEntities.add(parentTagEntity);
        List<TagEntity> tagEntities = new ArrayList<>();
        for (int level = 0; !parentTagEntities.isEmpty() && level < maxAllowedTagNesting; level++)
        {
            parentTagEntities = tagDao.getChildrenTags(parentTagEntities);
            tagEntities.addAll(parentTagEntities);
        }

        return tagEntities;
    }

    /**
     * Gets a tag entity and ensure it exists.
     *
     * @param tagKey the tag key (case insensitive)
     *
     * @return the tag entity
     * @throws org.finra.herd.model.ObjectNotFoundException if the tag entity doesn't exist
     */
    public TagEntity getTagEntity(TagKey tagKey) throws ObjectNotFoundException
    {
        TagEntity tagEntity = tagDao.getTagByKey(tagKey);

        if (tagEntity == null)
        {
            throw new ObjectNotFoundException(
                String.format("Tag with code \"%s\" doesn't exist for tag type \"%s\".", tagKey.getTagCode(), tagKey.getTagTypeCode()));
        }

        return tagEntity;
    }

    /**
     * Validate the update tag request parent tag key. The parent tag should not be on the children tree of the updated tag. No more than MAX_HIERARCHY_LEVEL is
     * allowed to update parent-child relation.
     *
     * @param tagEntity the tag entity being updated
     * @param parentTagEntity the tag update request
     */
    public void validateParentTagEntity(TagEntity tagEntity, TagEntity parentTagEntity)
    {
        Integer maxAllowedTagNesting = configurationHelper.getProperty(ConfigurationValue.MAX_ALLOWED_TAG_NESTING, Integer.class);

        TagEntity localParentTagEntity = parentTagEntity;
        int level = 0;
        while (localParentTagEntity != null)
        {
            Assert.isTrue(!tagEntity.equals(localParentTagEntity), "Parent tag key cannot be the requested tag key or any of its children’s tag keys.");
            localParentTagEntity = localParentTagEntity.getParentTagEntity();
            if (level++ >= maxAllowedTagNesting)
            {
                throw new IllegalArgumentException("Exceeds maximum allowed tag nesting level of " + maxAllowedTagNesting);
            }
        }
    }

    /**
     * Validates parent tag type's code against the tag type's code.
     *
     * @param tagTypeCode the tag type's code
     * @param parentTagTypeCode the parent tag type's code
     */
    public void validateParentTagType(String tagTypeCode, String parentTagTypeCode)
    {
        Assert.isTrue(tagTypeCode.equalsIgnoreCase(parentTagTypeCode), "Tag type code in parent tag key must match the tag type code in the request.");
    }
}
