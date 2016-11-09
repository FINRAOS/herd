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

import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.jpa.TagTypeEntity;

public interface TagTypeDao extends BaseJpaDao
{
    /**
     * Gets a tag type by its code.
     *
     * @param displayName the tag type display name (case-insensitive)
     *
     * @return the tag type entity for the specified display name
     */
    public TagTypeEntity getTagTypeByDisplayName(String displayName);

    /**
     * Gets a tag type by its key.
     *
     * @param tagTypeKey the tag type key (case-insensitive)
     *
     * @return the tag type entity for the specified key
     */
    public TagTypeEntity getTagTypeByKey(TagTypeKey tagTypeKey);

    /**
     * Gets an ordered list of tag type keys for all tag types defined in the system.
     *
     * @return the list of tag type keys
     */
    public List<TagTypeKey> getTagTypeKeys();

    /**
     * Gets an ordered list of tag type entities for all tag types defined in the system.
     *
     * @return the list of tag type entities
     */
    public List<TagTypeEntity> getTagTypes();
}
