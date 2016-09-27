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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.jpa.TagTypeEntity;

public class TagTypeDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetTagTypeByKey()
    {
        // Create a namespace entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Retrieve the namespace entity.
        TagTypeEntity resultTagTypeEntity = tagTypeDao.getTagTypeByKey(new TagTypeKey(TAG_TYPE));

        // Validate the results.
        assertEquals(TAG_TYPE, resultTagTypeEntity.getTypeCode());
        assertEquals(TAG_TYPE_DISPLAY_NAME, resultTagTypeEntity.getDisplayName());
        assertEquals(new Integer(1), resultTagTypeEntity.getOrderNumber());
    }

    @Test
    public void testGetTagTypes()
    {
        // Create and persist namespace entities.
        int orderNumber = 1;
        for (TagTypeKey key : tagTypeDaoTestHelper.getTestTagTypeKeys())
        {
            tagTypeDaoTestHelper.createTagTypeEntity(key.getTagTypeCode(), TAG_TYPE_DISPLAY_NAME, orderNumber++);
        }

        // Retrieve a list of namespace keys.
        List<TagTypeKey> resultTagTypeKeys = tagTypeDao.getTagTypes();

        // Validate the returned object.
        assertNotNull(resultTagTypeKeys);
        assertTrue(resultTagTypeKeys.containsAll(tagTypeDaoTestHelper.getTestTagTypeKeys()));
    }
}
