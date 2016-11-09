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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.TagType;
import org.finra.herd.model.api.xml.TagTypeCreateRequest;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagTypeKeys;
import org.finra.herd.model.api.xml.TagTypeSearchRequest;
import org.finra.herd.model.api.xml.TagTypeSearchResponse;
import org.finra.herd.model.api.xml.TagTypeUpdateRequest;
import org.finra.herd.service.impl.TagTypeServiceImpl;

public class TagTypeServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateTagType() throws Exception
    {
        // Create a tag type.
        TagType resultTagType = tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1), resultTagType);
    }

    @Test
    public void testCreateTagTypeDisplayNameAlreadyExists()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Try to create a duplicate tag type instance (uses the same display name).
        try
        {
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE_2), TAG_TYPE_DISPLAY_NAME, 2));
            fail("Should throw an AlreadyExistsException when display name already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Display name \"%s\" already exists for tag type \"%s\".", TAG_TYPE_DISPLAY_NAME, TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testCreateTagTypeInvalidParameters()
    {
        // Try to create a tag type instance when tag type code contains a forward slash character.
        try
        {
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(addSlash(TAG_TYPE)), TAG_TYPE_DISPLAY_NAME, 1));
            fail("Should throw an IllegalArgumentException when tag type code contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Tag type code can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateTagTypeMissingRequiredParameters()
    {
        // Try to create a tag type instance when tag type code is not specified.
        try
        {
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(BLANK_TEXT), TAG_TYPE_DISPLAY_NAME, 1));
            fail("Should throw an IllegalArgumentException when tag type code is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to create a tag type instance when display name is not specified.
        try
        {
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), BLANK_TEXT, 1));
            fail("Should throw an IllegalArgumentException when display name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A display name must be specified.", e.getMessage());
        }

        // Try to create a tag type instance when tag type order is not specified.
        try
        {
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, null));
            fail("Should throw an IllegalArgumentException when tag type order is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type order must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateTagTypeTagTypeAlreadyExists()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Try to create a duplicate tag type instance (uses the same tag type code).
        try
        {
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME_2, 2));
            fail("Should throw an AlreadyExistsException when tag type already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create tag type with code \"%s\" because it already exists.", TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testCreateTagTypeTrimParameters() throws Exception
    {
        // Create a tag type.
        TagType resultTagType =
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(addWhitespace(TAG_TYPE)), addWhitespace(TAG_TYPE_DISPLAY_NAME), 1));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1), resultTagType);
    }

    @Test
    public void testDeleteTagType()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Delete the tag type
        TagType resultTagType = tagTypeService.deleteTagType(new TagTypeKey(TAG_TYPE));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1), resultTagType);

        // Ensure that this tag type is no longer there.
        assertNull(tagTypeDao.getTagTypeByKey(resultTagType.getTagTypeKey()));
    }

    @Test
    public void testDeleteTagTypeLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE.toUpperCase(), TAG_TYPE_DISPLAY_NAME.toUpperCase(), 1);

        // Delete the tag type
        TagType resultTagType = tagTypeService.deleteTagType(new TagTypeKey(TAG_TYPE.toLowerCase()));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE.toUpperCase()), TAG_TYPE_DISPLAY_NAME.toUpperCase(), 1), resultTagType);

        // Ensure that this tag type is no longer there.
        assertNull(tagTypeDao.getTagTypeByKey(resultTagType.getTagTypeKey()));
    }

    @Test
    public void testDeleteTagTypeMissingRequiredParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Try to delete a tag type instance when tag type code is not specified.
        try
        {
            tagTypeService.deleteTagType(new TagTypeKey(BLANK_TEXT));
            fail("Should throw an ObjectNotFoundException when tag type code is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteTagTypeTagTypeNotExists()
    {
        // Try to delete a tag type instance when tag type code is not specified.
        try
        {
            tagTypeService.deleteTagType(new TagTypeKey(TAG_TYPE));
            fail("Should throw an ObjectNotFoundException when tag type code does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag type with code \"%s\" doesn't exist.", TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testDeleteTagTypeTrimParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Delete the tag type
        TagType resultTagType = tagTypeService.deleteTagType(new TagTypeKey(addWhitespace(TAG_TYPE)));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1), resultTagType);

        // Ensure that this tag type is no longer there.
        assertNull(tagTypeDao.getTagTypeByKey(resultTagType.getTagTypeKey()));
    }

    @Test
    public void testDeleteTagTypeUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE.toLowerCase(), TAG_TYPE_DISPLAY_NAME.toLowerCase(), 1);

        // Delete the tag type
        TagType resultTagType = tagTypeService.deleteTagType(new TagTypeKey(TAG_TYPE.toUpperCase()));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE.toLowerCase()), TAG_TYPE_DISPLAY_NAME.toLowerCase(), 1), resultTagType);

        // Ensure that this tag type is no longer there.
        assertNull(tagTypeDao.getTagTypeByKey(resultTagType.getTagTypeKey()));
    }

    @Test
    public void testGetTagType()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Retrieve the tag type
        TagType resultTagType = tagTypeService.getTagType(new TagTypeKey(TAG_TYPE));

        // Validate the returned object.

        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1), resultTagType);
    }

    @Test
    public void testGetTagTypeLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE.toUpperCase(), TAG_TYPE_DISPLAY_NAME.toUpperCase(), 1);

        // Retrieve the tag type
        TagType resultTagType = tagTypeService.getTagType(new TagTypeKey(TAG_TYPE.toLowerCase()));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE.toUpperCase()), TAG_TYPE_DISPLAY_NAME.toUpperCase(), 1), resultTagType);
    }

    @Test
    public void testGetTagTypeMissingRequiredParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Try to retrieve a tag type instance when tag type code is not specified.
        try
        {
            tagTypeService.getTagType(new TagTypeKey(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when tag type code is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetTagTypeTagTypeNotExists()
    {
        // Try to retrieve a tag type instance when tag type code is not registered.
        try
        {
            tagTypeService.getTagType(new TagTypeKey(TAG_TYPE));
            fail("Should throw an ObjectNotFoundException when tag type code does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag type with code \"%s\" doesn't exist.", TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testGetTagTypeTrimParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Retrieve the tag type
        TagType resultTagType = tagTypeService.getTagType(new TagTypeKey(addWhitespace(TAG_TYPE)));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, 1), resultTagType);
    }

    @Test
    public void testGetTagTypeUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE.toLowerCase(), TAG_TYPE_DISPLAY_NAME.toLowerCase(), 1);

        // Retrieve the tag type
        TagType resultTagType = tagTypeService.getTagType(new TagTypeKey(TAG_TYPE.toUpperCase()));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE.toLowerCase()), TAG_TYPE_DISPLAY_NAME.toLowerCase(), 1), resultTagType);
    }

    @Test
    public void testGetTagTypes()
    {
        // Create and persist tag type entities.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE_2, TAG_TYPE_DISPLAY_NAME_2, 2);

        // Retrieve the tag type keys
        TagTypeKeys resultTagTypeKeys = tagTypeService.getTagTypes();

        // Validate the returned object.
        assertEquals(2, resultTagTypeKeys.getTagTypeKeys().size());
        assertEquals(new TagTypeKey(TAG_TYPE), resultTagTypeKeys.getTagTypeKeys().get(0));
        assertEquals(new TagTypeKey(TAG_TYPE_2), resultTagTypeKeys.getTagTypeKeys().get(1));
    }

    @Test
    public void testGetTagTypesTagTypesNoExist()
    {
        // Retrieve the tag type keys
        TagTypeKeys resultTagTypeKeys = tagTypeService.getTagTypes();

        // Validate the returned object.
        assertEquals(0, resultTagTypeKeys.getTagTypeKeys().size());
    }

    @Test
    public void testSearchTagTypes()
    {
        // Create and persist tag type entities with tag type order values in reverse order.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2);
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE_2, TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER);

        // Search tag types.
        TagTypeSearchResponse tagTypeSearchResponse = tagTypeService
            .searchTagTypes(new TagTypeSearchRequest(), Sets.newHashSet(TagTypeServiceImpl.DISPLAY_NAME_FIELD, TagTypeServiceImpl.TAG_TYPE_ORDER_FIELD));

        // Validate the returned object.
        assertEquals(new TagTypeSearchResponse(Arrays.asList(new TagType(new TagTypeKey(TAG_TYPE_2), TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER),
            new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2))), tagTypeSearchResponse);
    }

    @Test
    public void testSearchTagTypesMissingOptionalParameters()
    {
        // Create and persist tag type entities with tag type order values in reverse order.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2);
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE_2, TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER);

        // Search tag types without specifying optional parameters.
        assertEquals(new TagTypeSearchResponse(Arrays.asList(new TagType(new TagTypeKey(TAG_TYPE_2), NO_TAG_TYPE_DISPLAY_NAME, NO_TAG_TYPE_ORDER),
            new TagType(new TagTypeKey(TAG_TYPE), NO_TAG_TYPE_DISPLAY_NAME, NO_TAG_TYPE_ORDER))),
            tagTypeService.searchTagTypes(new TagTypeSearchRequest(), NO_SEARCH_RESPONSE_FIELDS));

        // Search tag types without specifying optional parameters except for the display name field option.
        assertEquals(new TagTypeSearchResponse(Arrays.asList(new TagType(new TagTypeKey(TAG_TYPE_2), TAG_TYPE_DISPLAY_NAME_2, NO_TAG_TYPE_ORDER),
            new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, NO_TAG_TYPE_ORDER))),
            tagTypeService.searchTagTypes(new TagTypeSearchRequest(), Sets.newHashSet(TagTypeServiceImpl.DISPLAY_NAME_FIELD)));

        // Search tag types without specifying optional parameters except for the tag type order field option.
        assertEquals(new TagTypeSearchResponse(Arrays.asList(new TagType(new TagTypeKey(TAG_TYPE_2), NO_TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER),
            new TagType(new TagTypeKey(TAG_TYPE), NO_TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2))),
            tagTypeService.searchTagTypes(new TagTypeSearchRequest(), Sets.newHashSet(TagTypeServiceImpl.TAG_TYPE_ORDER_FIELD)));
    }

    @Test
    public void testSearchTagTypesMissingRequiredParameters()
    {
        // Try to search tag types when tag type search request is not specified.
        try
        {
            tagTypeService.searchTagTypes(null, Sets.newHashSet(TagTypeServiceImpl.DISPLAY_NAME_FIELD, TagTypeServiceImpl.TAG_TYPE_ORDER_FIELD));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type search request must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateTagType()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Update the tag type
        TagType resultTagType = tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2, 2));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME_2, 2), resultTagType);
    }

    @Test
    public void testUpdateTagTypeDisplayNameAlreadyExists()
    {
        // Create and persist two tag type entities with the second one having display name that we want to update to.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE_2, TAG_TYPE_DISPLAY_NAME_2, 2);

        // Try to update a tag type instance when display name already exists.
        try
        {
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2, 3));
            fail("Should throw an AlreadyExistsException when display name already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Display name \"%s\" already exists for tag type \"%s\".", TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_2), e.getMessage());
        }
    }

    @Test
    public void testUpdateTagTypeLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE.toUpperCase(), TAG_TYPE_DISPLAY_NAME.toUpperCase(), 1);

        // Update the tag type
        TagType resultTagType =
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE.toLowerCase()), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2.toLowerCase(), 2));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE.toUpperCase()), TAG_TYPE_DISPLAY_NAME_2.toLowerCase(), 2), resultTagType);
    }

    @Test
    public void testUpdateTagTypeMissingRequiredParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Try to update a tag type instance when tag type code is not specified.
        try
        {
            tagTypeService.updateTagType(new TagTypeKey(BLANK_TEXT), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2, 2));
            fail("Should throw an IllegalArgumentException when tag type code is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to update a tag type instance when display name is not specified.
        try
        {
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(BLANK_TEXT, 2));
            fail("Should throw an IllegalArgumentException when display name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A display name must be specified.", e.getMessage());
        }

        // Try to update a tag type instance when tag type order is not specified.
        try
        {
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2, null));
            fail("Should throw an IllegalArgumentException when tag type order is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type order must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateTagTypeNoChangesToDisplayName()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Update the tag type without changing the display name.
        TagType updatedTagType = tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE_2));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE_2), updatedTagType);
    }

    @Test
    public void testUpdateTagTypeNoChangesToDisplayNameExceptForCase()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE);

        // Update the tag type with the new display name that only changes case.
        TagType updatedTagType =
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME.toLowerCase(), INTEGER_VALUE_2));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME.toLowerCase(), INTEGER_VALUE_2), updatedTagType);
    }

    @Test
    public void testUpdateTagTypeTagTypeNotExists()
    {
        // Try to update a tag type instance when display name is not specified.
        try
        {
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME, 1));
            fail("Should throw an ObjectNotFoundException when tag type code does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag type with code \"%s\" doesn't exist.", TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testUpdateTagTypeTrimParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, 1);

        // Update the tag type
        TagType resultTagType =
            tagTypeService.updateTagType(new TagTypeKey(addWhitespace(TAG_TYPE)), new TagTypeUpdateRequest(addWhitespace(TAG_TYPE_DISPLAY_NAME_2), 2));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME_2, 2), resultTagType);
    }

    @Test
    public void testUpdateTagTypeUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE.toLowerCase(), TAG_TYPE_DISPLAY_NAME.toLowerCase(), 1);

        // Update the tag type
        TagType resultTagType =
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE.toUpperCase()), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2.toUpperCase(), 2));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE.toLowerCase()), TAG_TYPE_DISPLAY_NAME_2.toUpperCase(), 2), resultTagType);
    }
}
