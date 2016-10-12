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

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.TagDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagListResponse;
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
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private TagHelper tagHelper;

    @Autowired
    private TagTypeDaoHelper tagTypeDaoHelper;

    @Autowired
    private TagDao tagDao;

    @Autowired
    private TagDaoHelper tagDaoHelper;

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

        // Create and persist a new tag entity from the information in the request.
        tagEntity = createTagEntity(tagTypeEntity, request.getTagKey().getTagCode(), request.getDisplayName(), request.getDescription());

        // Create and return the tag object from the persisted entity.
        return createTagFromEntity(tagEntity);
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

        // Update and persist the tag entity.
        updateTagEntity(tagEntity, tagUpdateRequest);

        // Create and return the tag object from the tag entity.
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
    public TagListResponse getTags(String tagTypeCode, String tagCode)
    {
        // Validate and trim the tag type code.
        String tagTypeCodeLocal = alternateKeyHelper.validateStringParameter("tag type code", tagTypeCode);

        // Retrieve and ensure that a tag type exists for the specified tag type code.
        tagTypeDaoHelper.getTagTypeEntity(new TagTypeKey(tagTypeCodeLocal));

        // Get the list of tag keys.
        TagListResponse response = new TagListResponse();
        
        
        
        return null;
             
        //return new TagKeys(tagDao.getTagsByTagType(tagTypeCodeLocal));
    }

    /**
     * Validate the tag create request.
     *
     * @param tagCreateRequest the tag create request.
     */
    private void validateTagCreateRequest(TagCreateRequest tagCreateRequest)
    {
        Assert.notNull(tagCreateRequest, "A tag create request must be specified.");
        tagHelper.validateTagKey(tagCreateRequest.getTagKey());
        tagCreateRequest.setDisplayName(alternateKeyHelper.validateStringParameter("display name", tagCreateRequest.getDisplayName()));
    }

    /**
     * Validates the tag update request.
     *
     * @param tagUpdateRequest the specified tag update request.
     */
    private void validateTagUpdateRequest(TagUpdateRequest tagUpdateRequest)
    {
        Assert.notNull(tagUpdateRequest, "A tag update request must be specified.");
        tagUpdateRequest.setDisplayName(alternateKeyHelper.validateStringParameter("display name", tagUpdateRequest.getDisplayName()));
    }

    /**
     * Creates and persists a new Tag entity.
     *
     * @param tagTypeEntity the specified tag type entity.
     * @param displayName the specified display name.
     * @param description the specified description.
     *
     * @return the newly created tag entity.
     */
    private TagEntity createTagEntity(TagTypeEntity tagTypeEntity, String tagCode, String displayName, String description)
    {
        TagEntity tagEntity = new TagEntity();

        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(tagCode);
        tagEntity.setDisplayName(displayName);
        tagEntity.setDescription(description);

        return tagDao.saveAndRefresh(tagEntity);
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

        tagDao.saveAndRefresh(tagEntity);
    }

    /**
     * Creates the tag registration from the persisted entity.
     *
     * @param tagEntity the tag registration entity.
     *
     * @return the tag registration.
     */
    private Tag createTagFromEntity(TagEntity tagEntity)
    {
        Tag tag = new Tag();

        tag.setId(tagEntity.getId());
        tag.setTagKey(new TagKey(tagEntity.getTagType().getCode(), tagEntity.getTagCode()));
        tag.setDisplayName(tagEntity.getDisplayName());
        tag.setDescription(tagEntity.getDescription());

        return tag;
    }
}
