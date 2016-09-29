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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.finra.herd.model.api.xml.TagType;
import org.finra.herd.model.api.xml.TagTypeCreateRequest;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagTypeKeys;
import org.finra.herd.model.api.xml.TagTypeUpdateRequest;
import org.finra.herd.model.jpa.TagTypeEntity;

/**
 * This class tests various functionality within the namespace REST controller.
 */
public class TagTypeRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateTagType() throws Exception
    {
        // Create a tag type.
        TagType resultTagType = tagTypeRestController.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1));

        // Validate the returned object.
        assertEquals(new TagType(resultTagType.getId(), new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1), resultTagType);
    }

    @Test
    public void testGetTagType() throws Exception
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Retrieve the tag type.
        TagType resultTagType = tagTypeRestController.getTagType(TAG_TYPE);

        // Validate the returned object.
        assertEquals(new TagType(tagTypeEntity.getId(), new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1), resultTagType);
    }

    @Test
    public void testGetTagTypes() throws Exception
    {
        // Create and persist tag type entities.
        tagTypeDaoTestHelper.createTagTypeEntity(tagTypeDaoTestHelper.getTestTagTypeKeys().get(0).getTagTypeCode(), TAG_TYPE_DISPLAY_NAME, 1);
        tagTypeDaoTestHelper.createTagTypeEntity(tagTypeDaoTestHelper.getTestTagTypeKeys().get(1).getTagTypeCode(), TAG_TYPE_DISPLAY_NAME_2, 2);

        // Retrieve a list of tag type keys.
        TagTypeKeys resultTagTypeKeys = tagTypeRestController.getTagTypes();

        // Validate the returned object.
        assertNotNull(resultTagTypeKeys);
        assertNotNull(resultTagTypeKeys.getTagTypeKeys());
        assertTrue(resultTagTypeKeys.getTagTypeKeys().size() >= tagTypeDaoTestHelper.getTestTagTypeKeys().size());
        for (TagTypeKey key : tagTypeDaoTestHelper.getTestTagTypeKeys())
        {
            assertTrue(resultTagTypeKeys.getTagTypeKeys().contains(key));
        }
    }

    @Test
    public void testDeleteTagType() throws Exception
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Validate that this tag type exists.
        TagTypeKey tagTypeKey = new TagTypeKey(TAG_TYPE);
        assertNotNull(tagTypeDao.getTagTypeByKey(tagTypeKey));

        // Delete this tag type.
        TagType deletedTagType = tagTypeRestController.deleteTagType(TAG_TYPE);

        // Validate the returned object.
        assertEquals(new TagType(tagTypeEntity.getId(), new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1), deletedTagType);

        // Ensure that this tag type is no longer there.
        assertNull(tagTypeDao.getTagTypeByKey(tagTypeKey));
    }

    @Test
    public void testUpdateTagType() throws Exception
    {
        // Create and persist a tag type entity.
        TagTypeEntity tagTypeEntity = tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Retrieve the tag type.
        TagType resultTagType = tagTypeRestController.updateTagType(TAG_TYPE, new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2, 2));

        // Validate the returned object.
        assertEquals(new TagType(tagTypeEntity.getId(), new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME_2, 2), resultTagType);
    }
}
