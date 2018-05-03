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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.TagDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;

/**
 * This class tests functionality within the TagDaoHelper class.
 */
public class TagDaoHelperTest extends AbstractDaoTest
{
    @InjectMocks
    private TagDaoHelper tagDaoHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private TagDao tagDao;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testAssertDisplayNameDoesNotExistForTag()
    {
        // Create a tag type entity
        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(TAG_TYPE_CODE);

        // Create a tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(TAG_CODE);
        tagEntity.setDisplayName(TAG_DISPLAY_NAME);

        // Setup when clauses
        when(tagDao.getTagByTagTypeAndDisplayName(TAG_CODE, TAG_DISPLAY_NAME)).thenReturn(tagEntity);

        // Call method under test
        try
        {
            tagDaoHelper.assertDisplayNameDoesNotExistForTag(TAG_CODE, TAG_DISPLAY_NAME);
            fail("Expected to throw an exception.");
        }
        catch (AlreadyExistsException alreadyExistsException)
        {
            // Confirm the exception message is correct
            assertThat("Exception message is not correct.", alreadyExistsException.getMessage(), is(equalTo(String
                .format("Display name \"%s\" already exists for a tag with tag type \"%s\" and tag code \"%s\".", TAG_DISPLAY_NAME,
                    tagEntity.getTagType().getCode(), tagEntity.getTagCode()))));
        }

        // Setup verify clauses
        verify(tagDao).getTagByTagTypeAndDisplayName(TAG_CODE, TAG_DISPLAY_NAME);
        verifyNoMoreInteractions(configurationHelper, tagDao);
    }

    @Test
    public void testAssertDisplayNameDoesNotExistForTagWithNullTagEntity()
    {
        // Setup when clauses
        when(tagDao.getTagByTagTypeAndDisplayName(TAG_CODE, TAG_DISPLAY_NAME)).thenReturn(null);

        // Call method under test
        try
        {
            tagDaoHelper.assertDisplayNameDoesNotExistForTag(TAG_CODE, TAG_DISPLAY_NAME);
        }
        catch (AlreadyExistsException alreadyExistsException)
        {
            // Confirm the exception does not occur
            fail("Should not have caught an exception.");
        }

        // Setup verify clauses
        verify(tagDao).getTagByTagTypeAndDisplayName(TAG_CODE, TAG_DISPLAY_NAME);
        verifyNoMoreInteractions(configurationHelper, tagDao);
    }

    @Test
    public void testGetTagChildrenEntities()
    {
        // Create a tag type entity
        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(TAG_TYPE_CODE);

        // Create a tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(TAG_CODE);
        tagEntity.setDisplayName(TAG_DISPLAY_NAME);

        // Create a parent tag entities array list
        List<TagEntity> parentTagEntities = new ArrayList<>();
        parentTagEntities.add(tagEntity);

        // Setup when clauses
        when(configurationHelper.getProperty(ConfigurationValue.MAX_ALLOWED_TAG_NESTING, Integer.class)).thenReturn(1);
        when(tagDao.getChildrenTags(parentTagEntities)).thenReturn(parentTagEntities);

        // Call the method under test
        List<TagEntity> result = tagDaoHelper.getTagChildrenEntities(tagEntity);

        // Confirm the result value
        assertThat("Result not equal to parent tag entities list.", result, is(equalTo(parentTagEntities)));

        // Setup verify clauses
        verify(configurationHelper).getProperty(ConfigurationValue.MAX_ALLOWED_TAG_NESTING, Integer.class);
        verify(tagDao).getChildrenTags(parentTagEntities);
        verifyNoMoreInteractions(configurationHelper, tagDao);
    }

    @Test
    public void testGetTagEntity()
    {
        // Create a tag type entity
        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(TAG_TYPE_CODE);

        // Create a tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(TAG_CODE);
        tagEntity.setDisplayName(TAG_DISPLAY_NAME);

        // Create a tag key
        TagKey tagKey = new TagKey(TAG_TYPE_CODE, TAG_CODE);

        // Setup when clauses
        when(tagDao.getTagByKey(tagKey)).thenReturn(tagEntity);

        // Call method under test
        TagEntity result = tagDaoHelper.getTagEntity(tagKey);

        // Confirm the result value
        assertThat("Result not equal to tag entity.", result, is(equalTo(tagEntity)));

        // Setup verify clauses
        verify(tagDao).getTagByKey(tagKey);
        verifyNoMoreInteractions(configurationHelper, tagDao);
    }

    @Test
    public void testGetTagEntityWithNullTagEntity()
    {
        // Create a tag key
        TagKey tagKey = new TagKey(TAG_TYPE_CODE, TAG_CODE);

        // Setup when clauses
        when(tagDao.getTagByKey(tagKey)).thenReturn(null);

        // Call method under test
        try
        {
            tagDaoHelper.getTagEntity(tagKey);
            fail("Expected to throw an exception.");
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            // Confirm the exception message is correct
            assertThat("Exception message is not correct.", objectNotFoundException.getMessage(),
                is(equalTo(String.format("Tag with code \"%s\" doesn't exist for tag type \"%s\".", tagKey.getTagCode(), tagKey.getTagTypeCode()))));
        }

        // Setup verify clauses
        verify(tagDao).getTagByKey(tagKey);
        verifyNoMoreInteractions(configurationHelper, tagDao);
    }

    @Test
    public void testValidateParentTagEntity()
    {
        // Create a tag type entity
        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(TAG_TYPE_CODE);

        // Create a tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(TAG_CODE);
        tagEntity.setDisplayName(TAG_DISPLAY_NAME);

        // Create a parent tag entity
        TagEntity parentTagEntity = new TagEntity();
        parentTagEntity.setTagType(tagTypeEntity);
        parentTagEntity.setTagCode(TAG_CODE_2);
        parentTagEntity.setDisplayName(TAG_DISPLAY_NAME_2);
        parentTagEntity.setChildrenTagEntities(Lists.newArrayList(tagEntity));

        // Set the parent tag entity on the child tag entity
        tagEntity.setParentTagEntity(parentTagEntity);

        // Setup when clauses
        when(configurationHelper.getProperty(ConfigurationValue.MAX_ALLOWED_TAG_NESTING, Integer.class)).thenReturn(1);

        // Call the method under test
        tagDaoHelper.validateParentTagEntity(tagEntity, parentTagEntity);

        // Setup verify clauses
        verify(configurationHelper).getProperty(ConfigurationValue.MAX_ALLOWED_TAG_NESTING, Integer.class);
        verifyNoMoreInteractions(configurationHelper, tagDao);
    }

    @Test
    public void testValidateParentTagEntityWithLevelGreaterThanMaxAllowedTagNesting()
    {
        // Create a tag type entity
        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(TAG_TYPE_CODE);

        // Create a tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(TAG_CODE);
        tagEntity.setDisplayName(TAG_DISPLAY_NAME);

        // Create a parent tag entity
        TagEntity parentTagEntity = new TagEntity();
        parentTagEntity.setTagType(tagTypeEntity);
        parentTagEntity.setTagCode(TAG_CODE_2);
        parentTagEntity.setDisplayName(TAG_DISPLAY_NAME_2);
        parentTagEntity.setChildrenTagEntities(Lists.newArrayList(tagEntity));

        // Set the parent tag entity on the child tag entity
        tagEntity.setParentTagEntity(parentTagEntity);

        // Setup when clauses
        when(configurationHelper.getProperty(ConfigurationValue.MAX_ALLOWED_TAG_NESTING, Integer.class)).thenReturn(0);

        // Call the method under test
        try
        {
            tagDaoHelper.validateParentTagEntity(tagEntity, parentTagEntity);
            fail("Expected to throw an exception.");
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Confirm the exception message is correct
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("Exceeds maximum allowed tag nesting level of " + 0)));
        }

        // Setup verify clauses
        verify(configurationHelper).getProperty(ConfigurationValue.MAX_ALLOWED_TAG_NESTING, Integer.class);
        verifyNoMoreInteractions(configurationHelper, tagDao);
    }


    @Test
    public void testValidateParentTagEntityWithTagEntityEqualToParentTagEntity()
    {
        // Create a tag type entity
        TagTypeEntity tagTypeEntity = new TagTypeEntity();
        tagTypeEntity.setCode(TAG_TYPE_CODE);

        // Create a tag entity
        TagEntity tagEntity = new TagEntity();
        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(TAG_CODE);
        tagEntity.setDisplayName(TAG_DISPLAY_NAME);

        // Setup when clauses
        when(configurationHelper.getProperty(ConfigurationValue.MAX_ALLOWED_TAG_NESTING, Integer.class)).thenReturn(0);

        // Call the method under test
        try
        {
            tagDaoHelper.validateParentTagEntity(tagEntity, tagEntity);
            fail("Expected to throw an exception.");
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Confirm the exception message is correct
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("Parent tag key cannot be the requested tag key or any of its children’s tag keys.")));
        }

        // Setup verify clauses
        verify(configurationHelper).getProperty(ConfigurationValue.MAX_ALLOWED_TAG_NESTING, Integer.class);
        verifyNoMoreInteractions(configurationHelper, tagDao);
    }

    @Test
    public void testValidateParentTagType()
    {
        try
        {
            // Call the method under test
            tagDaoHelper.validateParentTagType(TAG_TYPE_CODE, TAG_TYPE_CODE);
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Confirm the exception does not occur
            fail("Should not have caught an exception.");
        }
    }

    @Test
    public void testValidateParentTagTypeWithUnmatchedTagTypeCode()
    {
        try
        {
            // Call the method under test
            tagDaoHelper.validateParentTagType(TAG_TYPE_CODE, TAG_TYPE_CODE_2);
            fail("Expected to throw an exception.");
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Confirm the exception message is correct
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("Tag type code in parent tag key must match the tag type code in the request.")));
        }
    }
}
