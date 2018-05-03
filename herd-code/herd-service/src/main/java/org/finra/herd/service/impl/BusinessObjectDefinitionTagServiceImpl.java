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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.BusinessObjectDefinitionTagDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTag;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKeys;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionTagEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.BusinessObjectDefinitionTagService;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.SearchIndexUpdateHelper;
import org.finra.herd.dao.helper.TagDaoHelper;
import org.finra.herd.service.helper.TagHelper;

/**
 * The business object definition tag service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDefinitionTagServiceImpl implements BusinessObjectDefinitionTagService
{
    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private BusinessObjectDefinitionTagDao businessObjectDefinitionTagDao;

    @Autowired
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @Autowired
    private TagDaoHelper tagDaoHelper;

    @Autowired
    private TagHelper tagHelper;

    @NamespacePermission(fields = "#request.businessObjectDefinitionTagKey.businessObjectDefinitionKey.namespace", permissions = {
        NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT, NamespacePermissionEnum.WRITE})
    @Override
    public BusinessObjectDefinitionTag createBusinessObjectDefinitionTag(BusinessObjectDefinitionTagCreateRequest request)
    {
        // Validate and trim the business object definition tag create request.
        validateBusinessObjectDefinitionTagCreateRequest(request);

        // Get the business object definition entity and ensure it exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(request.getBusinessObjectDefinitionTagKey().getBusinessObjectDefinitionKey());

        // Get the tag entity and ensure it exists.
        TagEntity tagEntity = tagDaoHelper.getTagEntity(request.getBusinessObjectDefinitionTagKey().getTagKey());

        // Ensure a business object definition tag for the specified business object definition and tag doesn't already exist.
        if (businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByParentEntities(businessObjectDefinitionEntity, tagEntity) != null)
        {
            throw new AlreadyExistsException(String.format("Tag with tag type \"%s\" and code \"%s\" already exists for business object definition {%s}.",
                request.getBusinessObjectDefinitionTagKey().getTagKey().getTagTypeCode(), request.getBusinessObjectDefinitionTagKey().getTagKey().getTagCode(),
                businessObjectDefinitionHelper
                    .businessObjectDefinitionKeyToString(request.getBusinessObjectDefinitionTagKey().getBusinessObjectDefinitionKey())));
        }

        // Create and persist a business object definition tag entity.
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity =
            createBusinessObjectDefinitionTagEntity(businessObjectDefinitionEntity, tagEntity);

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return the business object definition tag object from the persisted entity.
        return createBusinessObjectDefinitionTagFromEntity(businessObjectDefinitionTagEntity);
    }

    @NamespacePermission(fields = "#businessObjectDefinitionTagKey.businessObjectDefinitionKey.namespace",
        permissions = {NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT, NamespacePermissionEnum.WRITE})
    @Override
    public BusinessObjectDefinitionTag deleteBusinessObjectDefinitionTag(BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey)
    {
        // Validate and trim the business object definition tag key.
        validateBusinessObjectDefinitionTagKey(businessObjectDefinitionTagKey);

        // Retrieve and ensure that a business object definition tag exists.
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity = getBusinessObjectDefinitionTagEntity(businessObjectDefinitionTagKey);

        // Delete this business object format.
        businessObjectDefinitionTagDao.delete(businessObjectDefinitionTagEntity);

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionTagKey.getBusinessObjectDefinitionKey()),
            SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return the business object definition tag object from the deleted entity.
        return createBusinessObjectDefinitionTagFromEntity(businessObjectDefinitionTagEntity);
    }

    @Override
    public BusinessObjectDefinitionTagKeys getBusinessObjectDefinitionTagsByBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        // Validate the business object definition key.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Get the business object definition entity and ensure it exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Retrieve and return a list of business object definition tag keys.
        return new BusinessObjectDefinitionTagKeys(
            businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagsByBusinessObjectDefinitionEntity(businessObjectDefinitionEntity));
    }

    @Override
    public BusinessObjectDefinitionTagKeys getBusinessObjectDefinitionTagsByTag(TagKey tagKey)
    {
        // Validate the tag key.
        tagHelper.validateTagKey(tagKey);

        // Get the tag entity and ensure it exists.
        TagEntity tagEntity = tagDaoHelper.getTagEntity(tagKey);

        //Create a list of tag entities along with all its children tags down the hierarchy up to maximum allowed tag nesting level.
        List<TagEntity> tagEntities = new ArrayList<>();
        tagEntities.add(tagEntity);
        tagEntities.addAll(tagDaoHelper.getTagChildrenEntities(tagEntity));

        // Retrieve and return a list of business object definition tag keys.
        return new BusinessObjectDefinitionTagKeys(businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagsByTagEntities(tagEntities));
    }

    /**
     * Creates and persists a new business object definition tag entity from the business object definition and the tag entities.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param tagEntity the tag entity
     *
     * @return the newly created business object definition tag entity
     */
    private BusinessObjectDefinitionTagEntity createBusinessObjectDefinitionTagEntity(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        TagEntity tagEntity)
    {
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity = new BusinessObjectDefinitionTagEntity();

        businessObjectDefinitionTagEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionTagEntity.setTag(tagEntity);

        return businessObjectDefinitionTagDao.saveAndRefresh(businessObjectDefinitionTagEntity);
    }

    /**
     * Creates a business object definition tag from the persisted entity.
     *
     * @param businessObjectDefinitionTagEntity the business object definition tag entity
     *
     * @return the business object definition tag
     */
    private BusinessObjectDefinitionTag createBusinessObjectDefinitionTagFromEntity(BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity)
    {
        return new BusinessObjectDefinitionTag(businessObjectDefinitionTagEntity.getId(), getBusinessObjectDefinitionTagKey(businessObjectDefinitionTagEntity));
    }

    /**
     * Gets a business object definition tag entity on the key and makes sure that it exists.
     *
     * @param businessObjectDefinitionTagKey the business object definition tag key
     *
     * @return the business object definition tag entity
     */
    private BusinessObjectDefinitionTagEntity getBusinessObjectDefinitionTagEntity(BusinessObjectDefinitionTagKey businessObjectDefinitionTagKey)
    {
        BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity =
            businessObjectDefinitionTagDao.getBusinessObjectDefinitionTagByKey(businessObjectDefinitionTagKey);

        if (businessObjectDefinitionTagEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Tag with tag type \"%s\" and code \"%s\" does not exist for business object definition {%s}.",
                businessObjectDefinitionTagKey.getTagKey().getTagTypeCode(), businessObjectDefinitionTagKey.getTagKey().getTagCode(),
                businessObjectDefinitionHelper.businessObjectDefinitionKeyToString(businessObjectDefinitionTagKey.getBusinessObjectDefinitionKey())));
        }

        return businessObjectDefinitionTagEntity;
    }

    /**
     * Creates a business object definition tag key from the entity.
     *
     * @param businessObjectDefinitionTagEntity the business object definition entity
     *
     * @return the business object definition tag key
     */
    private BusinessObjectDefinitionTagKey getBusinessObjectDefinitionTagKey(BusinessObjectDefinitionTagEntity businessObjectDefinitionTagEntity)
    {
        return new BusinessObjectDefinitionTagKey(
            new BusinessObjectDefinitionKey(businessObjectDefinitionTagEntity.getBusinessObjectDefinition().getNamespace().getCode(),
                businessObjectDefinitionTagEntity.getBusinessObjectDefinition().getName()),
            new TagKey(businessObjectDefinitionTagEntity.getTag().getTagType().getCode(), businessObjectDefinitionTagEntity.getTag().getTagCode()));
    }

    /**
     * Validates the business object definition tag create request. This method also trims the request parameters.
     *
     * @param request the business object definition tag create request
     */
    private void validateBusinessObjectDefinitionTagCreateRequest(BusinessObjectDefinitionTagCreateRequest request)
    {
        Assert.notNull(request, "A business object definition tag create request must be specified.");
        validateBusinessObjectDefinitionTagKey(request.getBusinessObjectDefinitionTagKey());
    }

    /**
     * Validates the business object definition tag key. This method also trims the key parameters.
     *
     * @param key the business object definition tag key
     */
    private void validateBusinessObjectDefinitionTagKey(BusinessObjectDefinitionTagKey key)
    {
        Assert.notNull(key, "A business object definition tag key must be specified.");
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(key.getBusinessObjectDefinitionKey());
        tagHelper.validateTagKey(key.getTagKey());
    }
}
