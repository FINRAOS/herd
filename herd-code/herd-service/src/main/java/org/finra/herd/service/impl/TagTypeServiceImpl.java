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

import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_UPDATE;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.TagDao;
import org.finra.herd.dao.TagTypeDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.TagType;
import org.finra.herd.model.api.xml.TagTypeCreateRequest;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagTypeKeys;
import org.finra.herd.model.api.xml.TagTypeSearchRequest;
import org.finra.herd.model.api.xml.TagTypeSearchResponse;
import org.finra.herd.model.api.xml.TagTypeUpdateRequest;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.service.SearchableService;
import org.finra.herd.service.TagTypeService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.SearchIndexUpdateHelper;
import org.finra.herd.service.helper.TagTypeDaoHelper;
import org.finra.herd.service.helper.TagTypeHelper;

/**
 * The tag type service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class TagTypeServiceImpl implements TagTypeService, SearchableService
{
    // Constant to hold the description field option for the search response.
    public final static String DESCRIPTION_FIELD = "description".toLowerCase();

    // Constant to hold the display name field option for the search response.
    public final static String DISPLAY_NAME_FIELD = "displayName".toLowerCase();

    // Constant to hold the tag type order field option for the search response.
    public final static String TAG_TYPE_ORDER_FIELD = "tagTypeOrder".toLowerCase();

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @Autowired
    private TagDao tagDao;

    @Autowired
    private TagTypeDao tagTypeDao;

    @Autowired
    private TagTypeDaoHelper tagTypeDaoHelper;

    @Autowired
    private TagTypeHelper tagTypeHelper;

    @Override
    public TagType createTagType(TagTypeCreateRequest request)
    {
        // Validate and trim the request parameters.
        validateTagTypeCreateRequest(request);

        // Validate the tag type does not already exist in the database.
        if (tagTypeDao.getTagTypeByKey(request.getTagTypeKey()) != null)
        {
            throw new AlreadyExistsException(
                String.format("Unable to create tag type with code \"%s\" because it already exists.", request.getTagTypeKey().getTagTypeCode()));
        }

        // Validate the display name does not already exist in the database
        tagTypeDaoHelper.assertTagTypeDisplayNameDoesNotExist(request.getDisplayName());

        // Create and persist a new tag type entity from the request information.
        TagTypeEntity tagTypeEntity = createTagTypeEntity(request);

        // Create and return the tag type object from the persisted entity.
        return createTagTypeFromEntity(tagTypeEntity);
    }

    @Override
    public TagType deleteTagType(TagTypeKey tagTypeKey)
    {
        // Perform validation and trim.
        tagTypeHelper.validateTagTypeKey(tagTypeKey);

        // Retrieve and ensure that a tag type already exists with the specified key.
        TagTypeEntity tagTypeEntity = tagTypeDaoHelper.getTagTypeEntity(tagTypeKey);

        // Delete the tag type.
        tagTypeDao.delete(tagTypeEntity);

        // Create and return the tag type object from the deleted entity.
        return createTagTypeFromEntity(tagTypeEntity);
    }

    @Override
    public TagType getTagType(TagTypeKey tagTypeKey)
    {
        // Perform validation and trim.
        tagTypeHelper.validateTagTypeKey(tagTypeKey);

        // Retrieve and ensure that a tag type already exists with the specified key.
        TagTypeEntity tagTypeEntity = tagTypeDaoHelper.getTagTypeEntity(tagTypeKey);

        // Create and return the tag type object from the persisted entity.
        return createTagTypeFromEntity(tagTypeEntity);
    }

    @Override
    public TagTypeKeys getTagTypes()
    {
        return new TagTypeKeys(tagTypeDao.getTagTypeKeys());
    }

    @Override
    public Set<String> getValidSearchResponseFields()
    {
        return ImmutableSet.of(DISPLAY_NAME_FIELD, TAG_TYPE_ORDER_FIELD, DESCRIPTION_FIELD);
    }

    @Override
    public TagTypeSearchResponse searchTagTypes(TagTypeSearchRequest request, Set<String> fields)
    {
        // Validate the request.
        Assert.notNull(request, "A tag type search request must be specified.");

        // Validate and trim the search response fields.
        validateSearchResponseFields(fields);

        // Retrieve the tag types.
        List<TagTypeEntity> tagTypeEntities = tagTypeDao.getTagTypes();

        // Build the list of tag types.
        List<TagType> tagTypes = new ArrayList<>();
        for (TagTypeEntity tagTypeEntity : tagTypeEntities)
        {
            tagTypes.add(createTagTypeFromEntity(tagTypeEntity, fields.contains(DISPLAY_NAME_FIELD), fields.contains(TAG_TYPE_ORDER_FIELD),
                fields.contains(DESCRIPTION_FIELD)));
        }

        // Build and return the search response.
        return new TagTypeSearchResponse(tagTypes);
    }

    @PublishNotificationMessages
    @Override
    public TagType updateTagType(TagTypeKey tagTypeKey, TagTypeUpdateRequest request)
    {
        // Perform validation and trim.
        tagTypeHelper.validateTagTypeKey(tagTypeKey);

        // Perform validation and trim the alternate key parameters.
        validateTagTypeUpdateRequest(request);

        // Retrieve and ensure that a tag type already exists with the specified key.
        TagTypeEntity tagTypeEntity = tagTypeDaoHelper.getTagTypeEntity(tagTypeKey);

        // Validate the display name does not already exist for another tag type.
        if (!StringUtils.equalsIgnoreCase(tagTypeEntity.getDisplayName(), request.getDisplayName()))
        {
            // Validate that the description is different.
            tagTypeDaoHelper.assertTagTypeDisplayNameDoesNotExist(request.getDisplayName());
        }

        // Update and persist the tag type entity.
        updateTagTypeEntity(tagTypeEntity, request);

        // Notify the search index that a business object definition must be updated.
        List<TagEntity> tagEntities = tagDao.getTagsByTagTypeEntityAndParentTagCode(tagTypeEntity, null, null);
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionsInSearchIndex(businessObjectDefinitionDao.getBusinessObjectDefinitions(tagEntities),
            SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Notify the tag search index that tags must be updated.
        searchIndexUpdateHelper.modifyTagsInSearchIndex(tagEntities, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return the tag type from the persisted entity.
        return createTagTypeFromEntity(tagTypeEntity);
    }

    /**
     * Creates and persists a new tag type entity.
     *
     * @param request the tag type create request
     *
     * @return the newly created tag type entity
     */
    private TagTypeEntity createTagTypeEntity(TagTypeCreateRequest request)
    {
        TagTypeEntity tagTypeEntity = new TagTypeEntity();

        tagTypeEntity.setCode(request.getTagTypeKey().getTagTypeCode());
        tagTypeEntity.setDisplayName(request.getDisplayName());
        tagTypeEntity.setOrderNumber(request.getTagTypeOrder());
        tagTypeEntity.setDescription(request.getDescription());

        return tagTypeDao.saveAndRefresh(tagTypeEntity);
    }

    /**
     * Creates the tag type registration from the persisted entity.
     *
     * @param tagTypeEntity the tag type registration entity
     *
     * @return the tag type registration
     */
    private TagType createTagTypeFromEntity(TagTypeEntity tagTypeEntity)
    {
        return createTagTypeFromEntity(tagTypeEntity, true, true, true);
    }

    /**
     * Creates the tag type registration from the persisted entity.
     *
     * @param tagTypeEntity the tag type registration entity
     * @param includeDisplayName specifies to include the display name field
     * @param includeTagTypeOrder specifies to include the tag type order field
     * @param includeDescription specifies to include the description field
     *
     * @return the tag type registration
     */
    private TagType createTagTypeFromEntity(TagTypeEntity tagTypeEntity, boolean includeDisplayName, boolean includeTagTypeOrder, boolean includeDescription)
    {
        TagType tagType = new TagType();

        TagTypeKey tagTypeKey = new TagTypeKey();
        tagType.setTagTypeKey(tagTypeKey);
        tagTypeKey.setTagTypeCode(tagTypeEntity.getCode());

        if (includeDisplayName)
        {
            tagType.setDisplayName(tagTypeEntity.getDisplayName());
        }

        if (includeTagTypeOrder)
        {
            tagType.setTagTypeOrder(tagTypeEntity.getOrderNumber());
        }

        if (includeDescription)
        {
            tagType.setDescription(tagTypeEntity.getDescription());
        }

        return tagType;
    }

    /**
     * Update and persist the tag type per specified update request.
     *
     * @param tagTypeEntity the tag type entity
     * @param request the tag type update request
     */
    private void updateTagTypeEntity(TagTypeEntity tagTypeEntity, TagTypeUpdateRequest request)
    {
        // Update the tag type entity fields.
        tagTypeEntity.setDisplayName(request.getDisplayName());
        tagTypeEntity.setOrderNumber(request.getTagTypeOrder());
        tagTypeEntity.setDescription(request.getDescription());

        // Persist and refresh the entity.
        tagTypeDao.saveAndRefresh(tagTypeEntity);
    }

    /**
     * Validates the tag type create request. This method also trims the request parameters.
     *
     * @param request the tag type create request
     */
    private void validateTagTypeCreateRequest(TagTypeCreateRequest request)
    {
        Assert.notNull(request, "A tag type create request must be specified.");
        tagTypeHelper.validateTagTypeKey(request.getTagTypeKey());
        request.setDisplayName(alternateKeyHelper.validateStringParameter("display name", request.getDisplayName()));
        Assert.notNull(request.getTagTypeOrder(), "A tag type order must be specified.");
    }

    /**
     * Validates the tag type update request. This method also trims the request parameters.
     *
     * @param request the tag type update request
     */
    private void validateTagTypeUpdateRequest(TagTypeUpdateRequest request)
    {
        Assert.notNull(request, "A tag type update request must be specified.");
        request.setDisplayName(alternateKeyHelper.validateStringParameter("display name", request.getDisplayName()));
        Assert.notNull(request.getTagTypeOrder(), "A tag type order must be specified.");
    }
}
