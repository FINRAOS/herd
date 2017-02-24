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
import static org.junit.Assert.assertNotNull;
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
        TagType resultTagType =
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION), resultTagType);
    }

    @Test
    public void testCreateTagTypeDisplayNameAlreadyExists()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Try to create a duplicate tag type instance (uses the same display name).
        try
        {
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE_2), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2));
            fail();
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
            tagTypeService
                .createTagType(new TagTypeCreateRequest(new TagTypeKey(addSlash(TAG_TYPE)), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION));
            fail();
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
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(BLANK_TEXT), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to create a tag type instance when display name is not specified.
        try
        {
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), BLANK_TEXT, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A display name must be specified.", e.getMessage());
        }

        // Try to create a tag type instance when tag type order is not specified.
        try
        {
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, null, TAG_TYPE_DESCRIPTION));
            fail();
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
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Try to create a duplicate tag type instance (uses the same tag type code).
        try
        {
            tagTypeService.createTagType(new TagTypeCreateRequest(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2));
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create tag type with code \"%s\" because it already exists.", TAG_TYPE), e.getMessage());
        }
    }

    @Test
    public void testCreateTagTypeTrimParameters() throws Exception
    {
        // Create a tag type using input parameters with leading and trailing empty spaces.
        TagType resultTagType = tagTypeService.createTagType(
            new TagTypeCreateRequest(new TagTypeKey(addWhitespace(TAG_TYPE)), addWhitespace(TAG_TYPE_DISPLAY_NAME), TAG_TYPE_ORDER,
                addWhitespace(TAG_TYPE_DESCRIPTION)));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, addWhitespace(TAG_TYPE_DESCRIPTION)), resultTagType);
    }

    @Test
    public void testDeleteTagType()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Delete the tag type.
        TagType resultTagType = tagTypeService.deleteTagType(new TagTypeKey(TAG_TYPE));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION), resultTagType);

        // Ensure that this tag type is no longer there.
        assertNull(tagTypeDao.getTagTypeByKey(resultTagType.getTagTypeKey()));
    }

    @Test
    public void testDeleteTagTypeLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Delete the tag type using lower case input parameters.
        TagType resultTagType = tagTypeService.deleteTagType(new TagTypeKey(TAG_TYPE.toLowerCase()));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION), resultTagType);

        // Ensure that this tag type is no longer there.
        assertNull(tagTypeDao.getTagTypeByKey(resultTagType.getTagTypeKey()));
    }

    @Test
    public void testDeleteTagTypeMissingRequiredParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Try to delete a tag type instance when tag type code is not specified.
        try
        {
            tagTypeService.deleteTagType(new TagTypeKey(BLANK_TEXT));
            fail();
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
            tagTypeService.deleteTagType(new TagTypeKey(I_DO_NOT_EXIST));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Tag type with code \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }
    }

    @Test
    public void testDeleteTagTypeTrimParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Delete the tag type using input parameters with leading and trailing empty spaces.
        TagType resultTagType = tagTypeService.deleteTagType(new TagTypeKey(addWhitespace(TAG_TYPE)));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION), resultTagType);

        // Ensure that this tag type is no longer there.
        assertNull(tagTypeDao.getTagTypeByKey(resultTagType.getTagTypeKey()));
    }

    @Test
    public void testDeleteTagTypeUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Delete the tag type using upper case input parameters.
        TagType resultTagType = tagTypeService.deleteTagType(new TagTypeKey(TAG_TYPE.toUpperCase()));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION), resultTagType);

        // Ensure that this tag type is no longer there.
        assertNull(tagTypeDao.getTagTypeByKey(resultTagType.getTagTypeKey()));
    }

    @Test
    public void testGetTagType()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Retrieve the tag type.
        TagType resultTagType = tagTypeService.getTagType(new TagTypeKey(TAG_TYPE));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION), resultTagType);
    }

    @Test
    public void testGetTagTypeLowerCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Retrieve the tag type using lower case input parameters.
        TagType resultTagType = tagTypeService.getTagType(new TagTypeKey(TAG_TYPE.toLowerCase()));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION), resultTagType);
    }

    @Test
    public void testGetTagTypeMissingRequiredParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Try to retrieve a tag type instance when tag type code is not specified.
        try
        {
            tagTypeService.getTagType(new TagTypeKey(BLANK_TEXT));
            fail();
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
            fail();
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
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Retrieve the tag type using input parameters with leading and trailing empty spaces.
        TagType resultTagType = tagTypeService.getTagType(new TagTypeKey(addWhitespace(TAG_TYPE)));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION), resultTagType);
    }

    @Test
    public void testGetTagTypeUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Retrieve the tag type using upper case input parameters.
        TagType resultTagType = tagTypeService.getTagType(new TagTypeKey(TAG_TYPE.toUpperCase()));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION), resultTagType);
    }

    @Test
    public void testGetTagTypes()
    {
        // Create and persist tag type entities.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE_2, TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2);

        // Retrieve the tag type keys.
        TagTypeKeys resultTagTypeKeys = tagTypeService.getTagTypes();

        // Validate the returned object.
        assertNotNull(resultTagTypeKeys);
        assertEquals(Arrays.asList(new TagTypeKey(TAG_TYPE), new TagTypeKey(TAG_TYPE_2)), resultTagTypeKeys.getTagTypeKeys());
    }

    @Test
    public void testGetTagTypesTagTypesNoExist()
    {
        // Retrieve the tag type keys.
        TagTypeKeys resultTagTypeKeys = tagTypeService.getTagTypes();

        // Validate the returned object.
        assertNotNull(resultTagTypeKeys);
        assertEquals(0, resultTagTypeKeys.getTagTypeKeys().size());
    }

    @Test
    public void testSearchTagTypes()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagTypeSearchTesting();

        // Search tag types.
        TagTypeSearchResponse tagTypeSearchResponse = tagTypeService.searchTagTypes(new TagTypeSearchRequest(),
            Sets.newHashSet(TagTypeServiceImpl.DISPLAY_NAME_FIELD, TagTypeServiceImpl.TAG_TYPE_ORDER_FIELD, TagTypeServiceImpl.DESCRIPTION_FIELD));

        // Validate the returned object.
        assertEquals(new TagTypeSearchResponse(Arrays
            .asList(new TagType(new TagTypeKey(TAG_TYPE_2), TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION_2),
                new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION))), tagTypeSearchResponse);
    }

    @Test
    public void testSearchTagTypesInvalidParameters()
    {
        // Try to search tag types using a un-supported search response field option.
        try
        {
            tagTypeService.searchTagTypes(new TagTypeSearchRequest(), Sets.newHashSet("INVALID_FIELD_OPTION"));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Search response field \"invalid_field_option\" is not supported.", e.getMessage());
        }
    }

    @Test
    public void testSearchTagTypesLowerCaseParameters()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagTypeSearchTesting();

        // Search tag types using lower case input parameters.
        TagTypeSearchResponse tagTypeSearchResponse = tagTypeService.searchTagTypes(new TagTypeSearchRequest(),
            Sets.newHashSet(TagTypeServiceImpl.DISPLAY_NAME_FIELD.toLowerCase(), TagTypeServiceImpl.TAG_TYPE_ORDER_FIELD.toLowerCase(),
                TagTypeServiceImpl.DESCRIPTION_FIELD.toLowerCase()));

        // Validate the returned object.
        assertEquals(new TagTypeSearchResponse(Arrays
            .asList(new TagType(new TagTypeKey(TAG_TYPE_2), TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION_2),
                new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION))), tagTypeSearchResponse);
    }

    @Test
    public void testSearchTagTypesMissingOptionalParameters()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagTypeSearchTesting();

        // Search tag types without specifying optional parameters.
        assertEquals(new TagTypeSearchResponse(Arrays
            .asList(new TagType(new TagTypeKey(TAG_TYPE_2), NO_TAG_TYPE_DISPLAY_NAME, NO_TAG_TYPE_ORDER, NO_TAG_TYPE_DESCRIPTION),
                new TagType(new TagTypeKey(TAG_TYPE), NO_TAG_TYPE_DISPLAY_NAME, NO_TAG_TYPE_ORDER, NO_TAG_TYPE_DESCRIPTION))),
            tagTypeService.searchTagTypes(new TagTypeSearchRequest(), NO_SEARCH_RESPONSE_FIELDS));

        // Search tag types without specifying optional parameters except for the display name field option.
        assertEquals(new TagTypeSearchResponse(Arrays
            .asList(new TagType(new TagTypeKey(TAG_TYPE_2), TAG_TYPE_DISPLAY_NAME_2, NO_TAG_TYPE_ORDER, NO_TAG_TYPE_DESCRIPTION),
                new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, NO_TAG_TYPE_ORDER, NO_TAG_TYPE_DESCRIPTION))),
            tagTypeService.searchTagTypes(new TagTypeSearchRequest(), Sets.newHashSet(TagTypeServiceImpl.DISPLAY_NAME_FIELD)));

        // Search tag types without specifying optional parameters except for the tag type order field option.
        assertEquals(new TagTypeSearchResponse(Arrays
            .asList(new TagType(new TagTypeKey(TAG_TYPE_2), NO_TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, NO_TAG_TYPE_DESCRIPTION),
                new TagType(new TagTypeKey(TAG_TYPE), NO_TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2, NO_TAG_TYPE_DESCRIPTION))),
            tagTypeService.searchTagTypes(new TagTypeSearchRequest(), Sets.newHashSet(TagTypeServiceImpl.TAG_TYPE_ORDER_FIELD)));

        // Search tag types without specifying optional parameters except for the description field option.
        assertEquals(new TagTypeSearchResponse(Arrays
            .asList(new TagType(new TagTypeKey(TAG_TYPE_2), NO_TAG_TYPE_DISPLAY_NAME, NO_TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION_2),
                new TagType(new TagTypeKey(TAG_TYPE), NO_TAG_TYPE_DISPLAY_NAME, NO_TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION))),
            tagTypeService.searchTagTypes(new TagTypeSearchRequest(), Sets.newHashSet(TagTypeServiceImpl.DESCRIPTION_FIELD)));
    }

    @Test
    public void testSearchTagTypesMissingRequiredParameters()
    {
        // Try to search tag types when tag type search request is not specified.
        try
        {
            tagTypeService.searchTagTypes(null,
                Sets.newHashSet(TagTypeServiceImpl.DISPLAY_NAME_FIELD, TagTypeServiceImpl.TAG_TYPE_ORDER_FIELD, TagTypeServiceImpl.DESCRIPTION_FIELD));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type search request must be specified.", e.getMessage());
        }
    }

    @Test
    public void testSearchTagTypesTrimParameters()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagTypeSearchTesting();

        // Search tag types by using input parameters with leading and trailing empty spaces.
        TagTypeSearchResponse tagTypeSearchResponse = tagTypeService.searchTagTypes(new TagTypeSearchRequest(),
            Sets.newHashSet(addWhitespace(TagTypeServiceImpl.DISPLAY_NAME_FIELD), addWhitespace(TagTypeServiceImpl.TAG_TYPE_ORDER_FIELD),
                addWhitespace(TagTypeServiceImpl.DESCRIPTION_FIELD)));

        // Validate the returned object.
        assertEquals(new TagTypeSearchResponse(Arrays
            .asList(new TagType(new TagTypeKey(TAG_TYPE_2), TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION_2),
                new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION))), tagTypeSearchResponse);
    }

    @Test
    public void testSearchTagTypesUpperCaseParameters()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForTagTypeSearchTesting();

        // Search tag types using upper case input parameters.
        TagTypeSearchResponse tagTypeSearchResponse = tagTypeService.searchTagTypes(new TagTypeSearchRequest(),
            Sets.newHashSet(TagTypeServiceImpl.DISPLAY_NAME_FIELD.toUpperCase(), TagTypeServiceImpl.TAG_TYPE_ORDER_FIELD.toUpperCase(),
                TagTypeServiceImpl.DESCRIPTION_FIELD.toUpperCase()));

        // Validate the returned object.
        assertEquals(new TagTypeSearchResponse(Arrays
            .asList(new TagType(new TagTypeKey(TAG_TYPE_2), TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION_2),
                new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION))), tagTypeSearchResponse);
    }

    @Test
    public void testUpdateTagType()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Update the tag type.
        TagType resultTagType =
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2), resultTagType);
    }

    @Test
    public void testUpdateTagTypeDisplayNameAlreadyExists()
    {
        // Create and persist two tag type entities with the second one having display name that we want to update to.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE_2, TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2);

        // Try to update a tag type instance when display name already exists.
        try
        {
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2));
            fail();
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
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Update the tag type using lower case input parameters.
        TagType resultTagType = tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE.toLowerCase()),
            new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2.toLowerCase(), TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2.toLowerCase()));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME_2.toLowerCase(), TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2.toLowerCase()),
            resultTagType);
    }

    @Test
    public void testUpdateTagTypeMissingRequiredParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Try to update a tag type instance when tag type code is not specified.
        try
        {
            tagTypeService
                .updateTagType(new TagTypeKey(BLANK_TEXT), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A tag type code must be specified.", e.getMessage());
        }

        // Try to update a tag type instance when display name is not specified.
        try
        {
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(BLANK_TEXT, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A display name must be specified.", e.getMessage());
        }

        // Try to update a tag type instance when tag type order is not specified.
        try
        {
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2, null, TAG_TYPE_DESCRIPTION_2));
            fail();
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
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE, TAG_TYPE_DESCRIPTION);

        // Update the tag type without changing the display name.
        TagType updatedTagType =
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE_2, TAG_TYPE_DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE_2, TAG_TYPE_DESCRIPTION_2), updatedTagType);
    }

    @Test
    public void testUpdateTagTypeNoChangesToDisplayNameExceptForCase()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, INTEGER_VALUE, TAG_TYPE_DESCRIPTION);

        // Update the tag type with the new display name that only changes case.
        TagType updatedTagType = tagTypeService
            .updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME.toLowerCase(), INTEGER_VALUE_2, TAG_TYPE_DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME.toLowerCase(), INTEGER_VALUE_2, TAG_TYPE_DESCRIPTION_2), updatedTagType);
    }

    @Test
    public void testUpdateTagTypeTagTypeNotExists()
    {
        // Try to update a tag type instance when display name is not specified.
        try
        {
            tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE), new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION));
            fail();
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
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Update the tag type using input parameters with leading and trailing empty spaces.
        TagType resultTagType = tagTypeService.updateTagType(new TagTypeKey(addWhitespace(TAG_TYPE)),
            new TagTypeUpdateRequest(addWhitespace(TAG_TYPE_DISPLAY_NAME_2), TAG_TYPE_ORDER_2, addWhitespace(TAG_TYPE_DESCRIPTION_2)));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER_2, addWhitespace(TAG_TYPE_DESCRIPTION_2)), resultTagType);
    }

    @Test
    public void testUpdateTagTypeUpperCaseParameters()
    {
        // Create and persist a tag type entity.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION);

        // Update the tag type using upper case input parameters.
        TagType resultTagType = tagTypeService.updateTagType(new TagTypeKey(TAG_TYPE.toUpperCase()),
            new TagTypeUpdateRequest(TAG_TYPE_DISPLAY_NAME_2.toUpperCase(), TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2.toUpperCase()));

        // Validate the returned object.
        assertEquals(new TagType(new TagTypeKey(TAG_TYPE), TAG_TYPE_DISPLAY_NAME_2.toUpperCase(), TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION_2.toUpperCase()),
            resultTagType);
    }

    /**
     * Creates database entities required for the tag type search service unit tests.
     */
    private void createDatabaseEntitiesForTagTypeSearchTesting()
    {
        // Create and persist tag type entities with tag type order values in reverse order.
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE, TAG_TYPE_DISPLAY_NAME, TAG_TYPE_ORDER_2, TAG_TYPE_DESCRIPTION);
        tagTypeDaoTestHelper.createTagTypeEntity(TAG_TYPE_2, TAG_TYPE_DISPLAY_NAME_2, TAG_TYPE_ORDER, TAG_TYPE_DESCRIPTION_2);
    }
}
