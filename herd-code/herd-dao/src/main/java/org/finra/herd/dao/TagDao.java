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

import org.finra.herd.model.api.xml.TagChild;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;

public interface TagDao extends BaseJpaDao
{
    /**
     * Gets a list of tag entities that are immediate children of the specified parent tag entities.
     *
     * @param parentTagEntities the list of parent tag entities
     *
     * @return the list of tag entities that are immediate children of the specified parent tag entities
     */
    public List<TagEntity> getChildrenTags(List<TagEntity> parentTagEntities);

    /**
     * Get a tag entity by its key.
     *
     * @param tagKey the tag key
     *
     * @return the tag entity for the specified key
     */
    public TagEntity getTagByKey(TagKey tagKey);

    /**
     * Get a tag entity by its tag type code and display name.
     *
     * @param tagTypeCode the specified tag type code
     * @param displayName the specified display name
     *
     * @return the tag entity for the specified parameters
     */
    public TagEntity getTagByTagTypeAndDisplayName(String tagTypeCode, String displayName);

    /**
     * Gets a list of all tag entities registered in the system. The list of tags returned by this method is sorted by tag type order number and by tag's
     * display name ascending.
     *
     * @return list of tag entities
     */
    public List<TagEntity> getTags();

    /**
     * Gets a list of tag child objects with children flags, whose parent tag code is the specified tag code.
     * <p/>
     * When tagCode is null, return tags of tagTypeCode whose have no parent (root tags).
     *
     * @param tagTypeCode the tag type code
     * @param parentTagCode the parent tag code, may be null
     *
     * @return list of tag child objects with children flags
     */
    public List<TagChild> getTagsByTagTypeAndParentTagCode(String tagTypeCode, String parentTagCode);

    /**
     * Gets a list of tag entities per specified parameters.
     *
     * @param tagTypeEntity the tag type entity
     * @param parentTagCode the parent tag code, may be null
     * @param isParentTagNull specifies if tags should have no parents (root tags). This flag is ignored when parent tag code is specified
     *
     * @return list of tag entities
     */
    public List<TagEntity> getTagsByTagTypeEntityAndParentTagCode(TagTypeEntity tagTypeEntity, String parentTagCode, Boolean isParentTagNull);
}
