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

import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;

@Component
public class TagDaoTestHelper
{
    @Autowired
    private TagDao tagDao;

    /**
     * Creates and persists a new tag entity.
     *
     * @param tagTypeEntity the specified tag type entity.
     * @param displayName the specified display name.
     * @param description the specified description.
     *
     * @return the newly created tag entity.
     */
    public TagEntity createTagEntity(TagTypeEntity tagTypeEntity, String tagCode, String displayName, String description)
    {
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(tagCode);
        tagEntity.setDisplayName(displayName);
        tagEntity.setDescription(description);
        return tagDao.saveAndRefresh(tagEntity);
    }
}
