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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.TagTypeDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.TagType;
import org.finra.herd.model.api.xml.TagTypeCreateRequest;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagTypeKeys;
import org.finra.herd.model.api.xml.TagTypeUpdateRequest;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.service.TagTypeService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.TagTypeDaoHelper;
import org.finra.herd.service.helper.TagTypeHelper;

/**
 * The tag type service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class TagTypeServiceImpl implements TagTypeService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;
    @Autowired
    private TagTypeHelper tagTypeHelper;

    @Autowired
    private TagTypeDaoHelper tagTypeDaoHelper;

    @Autowired
    private TagTypeDao tagTypeDao;

    @Override
    public TagType createTagType(TagTypeCreateRequest request)
    {
        // Validate and trim the request parameters.
        validateTagTypeCreateRequest(request);

        // Validate the tag type does not already exist in the database.
        TagTypeEntity tagTypeEntity = tagTypeDao.getTagTypeByKey(request.getTagTypeKey());
        if (tagTypeEntity != null)
        {
            throw new AlreadyExistsException(
                String.format("Unable to create tag type with code \"%s\" because it already exists.", request.getTagTypeKey().getTagTypeCode()));
        }

        // Validate the display name does not already exist in the database
        tagTypeDaoHelper.assertTagTypeDisplayNameDoesNotExist(request.getDisplayName());

        // Create and persist a new tag type entity from the request information.
        tagTypeEntity = createTagTypeEntity(request.getTagTypeKey().getTagTypeCode(), request.getDisplayName(), request.getTagTypeOrder());

        // Create and return the tag type object from the persisted entity.
        return createTagTypeFromEntity(tagTypeEntity);
    }

    @Override
    public TagType updateTagType(TagTypeKey tagTypeKey, TagTypeUpdateRequest request)
    {
        // Perform validation and trim.
        tagTypeHelper.validateTagTypeKey(tagTypeKey);

        // Perform validation and trim the alternate key parameters.
        validateTagTypeUpdateRequest(request);

        // Retrieve and ensure that a tag type already exists with the specified key.
        TagTypeEntity tagTypeEntity = tagTypeDaoHelper.getTagTypeEntity(tagTypeKey);

        // Validate the display name does not already exist in the database
        tagTypeDaoHelper.assertTagTypeDisplayNameDoesNotExist(request.getDisplayName());

        updateTagTypeEntity(tagTypeEntity, request);

        // Create and return the business object format object from the persisted entity.
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
    public TagType deleteTagType(TagTypeKey tagTypeKey)
    {
        // Perform validation and trim.
        tagTypeHelper.validateTagTypeKey(tagTypeKey);

        // Retrieve and ensure that a tag type already exists with the specified key.
        TagTypeEntity namespaceEntity = tagTypeDaoHelper.getTagTypeEntity(tagTypeKey);

        // Delete the tag type.
        tagTypeDao.delete(namespaceEntity);

        // Create and return the tag type object from the deleted entity.
        return createTagTypeFromEntity(namespaceEntity);
    }

    @Override
    public TagTypeKeys getTagTypes()
    {
        return new TagTypeKeys(tagTypeDao.getTagTypes());
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

        // Validate display name
        request.setDisplayName(alternateKeyHelper.validateStringParameter("display name", request.getDisplayName()));

        // Validate order number
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

        // Validate display name
        request.setDisplayName(alternateKeyHelper.validateStringParameter("display name", request.getDisplayName()));

        // Validate order number
        Assert.notNull(request.getTagTypeOrder(), "A tag type order must be specified.");
    }

    /**
     * Creates and persists a new tag type entity.
     *
     * @param tagTypeCode the tag type code
     * @param displayName the display name
     * @param tagTypeOrder the tag type order number
     *
     * @return the newly created tag type entity
     */
    private TagTypeEntity createTagTypeEntity(String tagTypeCode, String displayName, int tagTypeOrder)
    {
        TagTypeEntity tagTypeEntity = new TagTypeEntity();

        tagTypeEntity.setTypeCode(tagTypeCode);
        tagTypeEntity.setDisplayName(displayName);
        tagTypeEntity.setOrderNumber(tagTypeOrder);

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
        TagType tagType = new TagType();

        tagType.setId(tagTypeEntity.getId());

        TagTypeKey tagTypeKey = new TagTypeKey();
        tagType.setTagTypeKey(tagTypeKey);
        tagTypeKey.setTagTypeCode(tagTypeEntity.getTypeCode());

        tagType.setDisplayName(tagTypeEntity.getDisplayName());
        tagType.setTagTypeOrder(tagTypeEntity.getOrderNumber());

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
        tagTypeEntity.setDisplayName(request.getDisplayName());
        tagTypeEntity.setOrderNumber(request.getTagTypeOrder());

        // Persist and refresh the entity.
        tagTypeDao.saveAndRefresh(tagTypeEntity);
    }
}
