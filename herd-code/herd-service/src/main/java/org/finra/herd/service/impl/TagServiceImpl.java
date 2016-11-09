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
package org.finra.herd.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.TagDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagChild;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagListResponse;
import org.finra.herd.model.api.xml.TagSearchFilter;
import org.finra.herd.model.api.xml.TagSearchKey;
import org.finra.herd.model.api.xml.TagSearchRequest;
import org.finra.herd.model.api.xml.TagSearchResponse;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagUpdateRequest;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.service.TagService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.TagDaoHelper;
import org.finra.herd.service.helper.TagHelper;
import org.finra.herd.service.helper.TagTypeDaoHelper;

/**
 * The tag service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class TagServiceImpl implements TagService
{
    // Constant to hold the display name field option for the search response.
    public final static String DESCRIPTION_FIELD = "description".toLowerCase();

    // Constant to hold the display name field option for the search response.
    public final static String DISPLAY_NAME_FIELD = "displayName".toLowerCase();

    // Constant to hold the hasChildren field option for the search response.
    public final static String HAS_CHILDREN_FIELD = "hasChildren".toLowerCase();

    // Constant to hold the parent tag key field option for the search response.
    public final static String PARENT_TAG_KEY_FIELD = "parentTagKey".toLowerCase();

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private TagDao tagDao;

    @Autowired
    private TagDaoHelper tagDaoHelper;

    @Autowired
    private TagHelper tagHelper;

    @Autowired
    private TagTypeDaoHelper tagTypeDaoHelper;

    @Override
    public Tag createTag(TagCreateRequest request)
    {
        // Validate and trim the request parameters.
        validateTagCreateRequest(request);

        // Get the tag type and ensure it exists.
        TagTypeEntity tagTypeEntity = tagTypeDaoHelper.getTagTypeEntity(new TagTypeKey(request.getTagKey().getTagTypeCode()));

        // Validate that the tag entity does not already exist.
        TagEntity tagEntity = tagDao.getTagByKey(request.getTagKey());
        if (tagEntity != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create tag with tag type code \"%s\" and tag code \"%s\" because it already exists.", request.getTagKey().getTagTypeCode(),
                    request.getTagKey().getTagCode()));
        }

        // Validate that the specified display name does not already exist for the specified tag type
        tagDaoHelper.assertDisplayNameDoesNotExistForTag(request.getTagKey().getTagTypeCode(), request.getDisplayName());
        TagEntity parentTagEntity = null;
        if (request.getParentTagKey() != null)
        {
            parentTagEntity = tagDaoHelper.getTagEntity(request.getParentTagKey());
        }

        // Create and persist a new tag entity from the information in the request.
        tagEntity = createTagEntity(tagTypeEntity, request.getTagKey().getTagCode(), request.getDisplayName(), request.getDescription(), parentTagEntity);

        // Create and return the tag object from the persisted entity.
        return createTagFromEntity(tagEntity);
    }

    @Override
    public Tag deleteTag(TagKey tagKey)
    {
        // Validate and trim the tag key.
        tagHelper.validateTagKey(tagKey);

        // Retrieve and ensure that a Tag already exists for the given tag key.
        TagEntity tagEntity = tagDaoHelper.getTagEntity(tagKey);

        // delete the tag.
        tagDao.delete(tagEntity);

        // Create and return the tag object from the deleted entity.
        return createTagFromEntity(tagEntity);
    }

    @Override
    public Tag getTag(TagKey tagKey)
    {
        // Perform validation and trim.
        tagHelper.validateTagKey(tagKey);

        // Retrieve and ensure that a Tag already exists with the specified key.
        TagEntity tagEntity = tagDaoHelper.getTagEntity(tagKey);

        // Create and return the tag object from the entity which was retrieved.
        return createTagFromEntity(tagEntity);
    }

    @Override
    public TagListResponse getTags(String tagTypeCode, String tagCode)
    {
        // Validate and trim the tag type code.
        String tagTypeCodeLocal = alternateKeyHelper.validateStringParameter("tag type code", tagTypeCode);

        String cleanTagCode = tagCode;

        // Retrieve and ensure that a tag type exists for the specified tag type code.
        tagTypeDaoHelper.getTagTypeEntity(new TagTypeKey(tagTypeCodeLocal));

        // Get the list of tag keys.
        TagListResponse response = new TagListResponse();

        //not root, need to set the tag key and parent tag key
        //getTag method will validate the requested tag exists
        if (tagCode != null)
        {
            cleanTagCode = alternateKeyHelper.validateStringParameter("tag code", tagCode);
            TagKey tagKey = new TagKey(tagTypeCodeLocal, cleanTagCode);
            Tag tag = getTag(tagKey);
            response.setTagKey(tag.getTagKey());
            response.setParentTagKey(tag.getParentTagKey());
        }

        List<TagChild> tagChildren = tagDao.getTagsByTagTypeAndParentTagCode(tagTypeCodeLocal, cleanTagCode);

        response.setTagChildren(tagChildren);

        return response;
    }

    @Override
    public TagSearchResponse searchTags(TagSearchRequest request, Set<String> fields)
    {
        // Validate and trim the request parameters.
        validateTagSearchRequest(request);

        // Validate and trim the search response fields.
        validateSearchResponseFields(fields);

        // Get the tag search key.
        TagSearchKey tagSearchKey = request.getTagSearchFilters().get(0).getTagSearchKeys().get(0);

        // Retrieve and ensure that a tag type exists for the specified tag type code.
        TagTypeEntity tagTypeEntity = tagTypeDaoHelper.getTagTypeEntity(new TagTypeKey(tagSearchKey.getTagTypeCode()));

        // Retrieve the tag types.
        List<TagEntity> tagEntities =
            tagDao.getTagsByTagTypeEntityAndParentTagCode(tagTypeEntity, tagSearchKey.getParentTagCode(), tagSearchKey.isIsParentTagNull());

        // Build the list of tags.
        List<Tag> tags = new ArrayList<>();
        for (TagEntity tagEntity : tagEntities)
        {
            tags.add(createTagFromEntity(tagEntity, false, fields.contains(DISPLAY_NAME_FIELD), fields.contains(DESCRIPTION_FIELD), false, false,
                fields.contains(PARENT_TAG_KEY_FIELD), fields.contains(HAS_CHILDREN_FIELD)));
        }

        // Build and return the tag search response.
        return new TagSearchResponse(tags);
    }

    @Override
    public Tag updateTag(TagKey tagKey, TagUpdateRequest tagUpdateRequest)
    {
        // Perform validation and trim
        tagHelper.validateTagKey(tagKey);

        // Perform validation and trim
        validateTagUpdateRequest(tagUpdateRequest);

        // Retrieve and ensure that a tag already exists with the specified key.
        TagEntity tagEntity = tagDaoHelper.getTagEntity(tagKey);

        // Validate the display name does not already exist for another tag for this tag type in the database.
        if (!StringUtils.equalsIgnoreCase(tagEntity.getDisplayName(), tagUpdateRequest.getDisplayName()))
        {
            // Validate that the description is different.
            tagDaoHelper.assertDisplayNameDoesNotExistForTag(tagKey.getTagTypeCode(), tagUpdateRequest.getDisplayName());
        }

        //validate parent key if there is one
        tagDaoHelper.validateUpdateTagParentKey(tagEntity, tagUpdateRequest);

        // Update and persist the tag entity.
        updateTagEntity(tagEntity, tagUpdateRequest);

        // Create and return the tag object from the tag entity.
        return createTagFromEntity(tagEntity);
    }

    /**
     * Creates and persists a new Tag entity.
     *
     * @param tagTypeEntity the specified tag type entity.
     * @param displayName the specified display name.
     * @param description the specified description.
     * @param parentTagEntity the specified parent tag entity
     *
     * @return the newly created tag entity.
     */
    private TagEntity createTagEntity(TagTypeEntity tagTypeEntity, String tagCode, String displayName, String description, TagEntity parentTagEntity)
    {
        TagEntity tagEntity = new TagEntity();

        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(tagCode);
        tagEntity.setDisplayName(displayName);
        tagEntity.setDescription(description);
        tagEntity.setParentTagEntity(parentTagEntity);

        return tagDao.saveAndRefresh(tagEntity);
    }

    /**
     * Creates the tag from the persisted entity.
     *
     * @param tagEntity the tag entity
     *
     * @return the tag
     */
    private Tag createTagFromEntity(TagEntity tagEntity)
    {
        return createTagFromEntity(tagEntity, true, true, true, true, true, true, false);
    }

    /**
     * Creates the tag from the persisted entity.
     *
     * @param tagEntity the tag entity
     * @param includeId specifies to include the display name field
     * @param includeDisplayName specifies to include the display name field
     * @param includeDescription specifies to include the description field
     * @param includeUserId specifies to include the display name field
     * @param includeUpdatedTime specifies to include the display name field
     * @param includeParentTagKey specifies to include the parent tag key field
     * @param includeHasChildren specifies to include the hasChildren field
     *
     * @return the tag
     */
    private Tag createTagFromEntity(TagEntity tagEntity, boolean includeId, boolean includeDisplayName, boolean includeDescription, boolean includeUserId,
        boolean includeUpdatedTime, boolean includeParentTagKey, boolean includeHasChildren)
    {
        Tag tag = new Tag();

        if (includeId)
        {
            tag.setId(tagEntity.getId());
        }

        tag.setTagKey(new TagKey(tagEntity.getTagType().getCode(), tagEntity.getTagCode()));

        if (includeDisplayName)
        {
            tag.setDisplayName(tagEntity.getDisplayName());
        }

        if (includeDescription)
        {
            tag.setDescription(tagEntity.getDescription());
        }

        if (includeUserId)
        {
            tag.setUserId(tagEntity.getCreatedBy());
        }

        if (includeUpdatedTime)
        {
            tag.setUpdatedTime(HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()));
        }

        if (includeParentTagKey)
        {
            TagEntity parentTagEntity = tagEntity.getParentTagEntity();
            if (parentTagEntity != null)
            {
                tag.setParentTagKey(new TagKey(parentTagEntity.getTagType().getCode(), parentTagEntity.getTagCode()));
            }
        }

        if (includeHasChildren)
        {
            tag.setHasChildren(!tagEntity.getChildrenTagEntities().isEmpty());
        }

        return tag;
    }

    /**
     * Returns valid search response fields.
     *
     * @return the set of valid search response fields
     */
    private Set<String> getValidSearchResponseFields()
    {
        return ImmutableSet.of(DISPLAY_NAME_FIELD, DESCRIPTION_FIELD, PARENT_TAG_KEY_FIELD, HAS_CHILDREN_FIELD);
    }

    /**
     * Updates and persists the tag entity per the specified update request.
     *
     * @param tagEntity the specified tag entity.
     * @param tagUpdateRequest the specified tag update request.
     */
    private void updateTagEntity(TagEntity tagEntity, TagUpdateRequest tagUpdateRequest)
    {
        tagEntity.setDisplayName(tagUpdateRequest.getDisplayName());
        tagEntity.setDescription(tagUpdateRequest.getDescription());

        if (tagUpdateRequest.getParentTagKey() != null)
        {
            TagEntity parentTagEntity = tagDaoHelper.getTagEntity(tagUpdateRequest.getParentTagKey());
            tagEntity.setParentTagEntity(parentTagEntity);
        }
        else
        {
            tagEntity.setParentTagEntity(null);
        }

        tagDao.saveAndRefresh(tagEntity);
    }

    /**
     * Validates the search response fields. This method also trims and lowers the fields.
     *
     * @param fields the search response fields
     */
    private void validateSearchResponseFields(Set<String> fields)
    {
        // Create a local copy of the fields set so that we can stream it to modify the fields set
        Set<String> localCopy = new HashSet<>(fields);

        // Clear the fields set
        fields.clear();

        // Add to the fields set field the strings both trimmed and lower cased and filter out empty and null strings
        localCopy.stream().filter(StringUtils::isNotBlank).map(String::trim).map(String::toLowerCase).forEachOrdered(fields::add);

        // Validate the field names
        fields.forEach(
            field -> Assert.isTrue(getValidSearchResponseFields().contains(field), String.format("Search response field \"%s\" is not supported.", field)));
    }

    /**
     * Validate the tag create request. This method also trims the request parameters.
     *
     * @param tagCreateRequest the tag create request.
     */
    private void validateTagCreateRequest(TagCreateRequest tagCreateRequest)
    {
        Assert.notNull(tagCreateRequest, "A tag create request must be specified.");
        tagHelper.validateTagKey(tagCreateRequest.getTagKey());

        if (tagCreateRequest.getParentTagKey() != null)
        {
            tagHelper.validateTagKey(tagCreateRequest.getParentTagKey());
        }

        tagCreateRequest.setDisplayName(alternateKeyHelper.validateStringParameter("display name", tagCreateRequest.getDisplayName()));
        tagDaoHelper.validateCreateTagParentKey(tagCreateRequest);
    }

    /**
     * Validate the tag search request. This method also trims the request parameters.
     *
     * @param tagSearchRequest the tag search request
     */
    private void validateTagSearchRequest(TagSearchRequest tagSearchRequest)
    {
        Assert.notNull(tagSearchRequest, "A tag search request must be specified.");

        Assert.isTrue(CollectionUtils.size(tagSearchRequest.getTagSearchFilters()) == 1 && tagSearchRequest.getTagSearchFilters().get(0) != null,
            "Exactly one tag search filter must be specified.");

        // Get the tag search filter.
        TagSearchFilter tagSearchFilter = tagSearchRequest.getTagSearchFilters().get(0);

        Assert.isTrue(CollectionUtils.size(tagSearchFilter.getTagSearchKeys()) == 1 && tagSearchFilter.getTagSearchKeys().get(0) != null,
            "Exactly one tag search key must be specified.");

        // Get the tag search key.
        TagSearchKey tagSearchKey = tagSearchFilter.getTagSearchKeys().get(0);

        tagSearchKey.setTagTypeCode(alternateKeyHelper.validateStringParameter("tag type code", tagSearchKey.getTagTypeCode()));

        if (tagSearchKey.getParentTagCode() != null)
        {
            tagSearchKey.setParentTagCode(tagSearchKey.getParentTagCode().trim());
        }

        // Fail validation when parent tag code is specified along with the isParentTagNull flag set to true.
        Assert.isTrue(StringUtils.isBlank(tagSearchKey.getParentTagCode()) || BooleanUtils.isNotTrue(tagSearchKey.isIsParentTagNull()),
            "A parent tag code can not be specified when isParentTagNull flag is set to true.");
    }

    /**
     * Validates the tag update request. This method also trims the request parameters.
     *
     * @param tagUpdateRequest the specified tag update request.
     */
    private void validateTagUpdateRequest(TagUpdateRequest tagUpdateRequest)
    {
        Assert.notNull(tagUpdateRequest, "A tag update request must be specified.");
        tagUpdateRequest.setDisplayName(alternateKeyHelper.validateStringParameter("display name", tagUpdateRequest.getDisplayName()));

        if (tagUpdateRequest.getParentTagKey() != null)
        {
            tagHelper.validateTagKey(tagUpdateRequest.getParentTagKey());
        }
    }
}
