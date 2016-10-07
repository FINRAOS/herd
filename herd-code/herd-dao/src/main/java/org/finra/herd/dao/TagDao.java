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

import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.TagEntity;

public interface TagDao extends BaseJpaDao
{
    /**
     * Get a tag by its key.
     *
     * @param tagKey the tag key.
     *
     * @return the tag entity for the specified key.
     */
    public TagEntity getTagByKey(TagKey tagKey);

    /**
     * Get a tag by its tag type code and display name.
     *
     * @param tagTypeCode the specified tag type code.
     *
     * @param displayName the specified display name.
     *
     * @return the tag entity for the specified parameters.
     */
    public TagEntity getTagByTagTypeAndDisplayName(String tagTypeCode, String displayName);

    /**
     * Gets an ordered list of tag keys for the tags defined in the system for the specified tag type code.
     *
     * @return the list of tags.
     */
    public List<TagKey> getTagsByTagType(String tagTypeCd);
}
