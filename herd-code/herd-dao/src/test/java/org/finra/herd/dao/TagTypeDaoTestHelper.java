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

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.jpa.TagTypeEntity;

@Component
public class TagTypeDaoTestHelper
{
    @Autowired
    private TagTypeDao tagTypeDao;

    /**
     * Creates and persists a new tag type entity.
     *
     * @param typeCode the tag type code
     * @param displayName the display name
     * @param orderNumber the sorting number
     *
     * @return the newly created tag type entity.
     */
    public TagTypeEntity createTagTypeEntity(String typeCode, String displayName, Integer orderNumber)
    {
        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setTypeCode(typeCode);
        tagTypeEntity.setOrderNumber(orderNumber);
        tagTypeEntity.setDisplayName(displayName);
        return tagTypeDao.saveAndRefresh(tagTypeEntity);
    }

    /**
     * Creates and persists a new tag type entity.
     *
     * @return the newly created tag type entity.
     */
    public TagTypeEntity createTagTypeEntity()
    {
        return createTagTypeEntity("TagTypeTest" + AbstractDaoTest.getRandomSuffix(), "TagTypeTest" + AbstractDaoTest.getRandomSuffix(), 1);
    }

    /**
     * Returns a list of test tag type keys.
     *
     * @return the list of test tag type keys
     */
    public List<TagTypeKey> getTestTagTypeKeys()
    {
        return Arrays.asList(new TagTypeKey(AbstractDaoTest.TAG_TYPE), new TagTypeKey(AbstractDaoTest.TAG_TYPE_2));
    }
}
