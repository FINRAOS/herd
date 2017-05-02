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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.AttributeValueListDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.service.AttributeValueListService;
import org.finra.herd.service.NamespaceService;
import org.finra.herd.service.helper.AttributeValueListHelper;


/**
 * The attribute value list service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class AttributeValueListServiceImpl implements AttributeValueListService
{
    @Autowired
    private AttributeValueListDao attributeValueListDao;

    @Autowired
    private AttributeValueListHelper attributeValueListHelper;

    @Autowired
    private NamespaceService namespaceService;

    @NamespacePermission(fields = "#request.attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public AttributeValueList createAttributeValueList(AttributeValueListCreateRequest attributeValueListCreateRequest) throws ObjectNotFoundException
    {
        // Validate and trim the request parameters.
        validateAttributeValueListCreateRequest(attributeValueListCreateRequest);

        NamespaceKey namespaceKey = new NamespaceKey(attributeValueListCreateRequest.getAttributeValueListKey().getNamespace());
        if (namespaceService.getNamespace(namespaceKey) == null)
        {
            throw new ObjectNotFoundException(String
                .format("No namespace available as \"%s\". To create a new attribute value list an existing namespace is required.",
                    attributeValueListCreateRequest.getAttributeValueListKey().getNamespace()));

        }

        // Validate the attribute value list does not already exist in the database.
        if (attributeValueListDao.getAttributeValueListByKey(attributeValueListCreateRequest.getAttributeValueListKey()) != null)
        {
            throw new AlreadyExistsException(String.format("Unable to create attribute value list with code \"%s\" and \"%s\" because it already exists.",
                attributeValueListCreateRequest.getAttributeValueListKey().getNamespace(),
                attributeValueListCreateRequest.getAttributeValueListKey().getAttributeValueListName()));
        }

        // Create and persist a new attribute value list entity from the request information.
        AttributeValueListEntity attributeValueListEntity = createAttributeValueListEntity(attributeValueListCreateRequest);

        // Create and return the attribute value list object from the persisted entity.
        return createAttributeValueListFromEntity(attributeValueListEntity);
    }

    public AttributeValueListEntity createAttributeValueListEntity(AttributeValueListCreateRequest attributeValueListCreateRequest)
    {
        // Create a new entity.
        AttributeValueListEntity attributeValueListEntity =
            attributeValueListHelper.getAttributeValueListEntity(attributeValueListCreateRequest.getAttributeValueListKey());

        // Persist and return the new entity.
        return attributeValueListDao.saveAndRefresh(attributeValueListEntity);
    }

    /**
     * Creates the partition key group from the persisted entity.
     *
     * @param attributeValueListEntity the attribute value list entity
     *
     * @return the partition key group
     */
    public AttributeValueList createAttributeValueListFromEntity(AttributeValueListEntity attributeValueListEntity)
    {
        // Create the partition key group.
        AttributeValueList attributeValueList = new AttributeValueList();

        AttributeValueListKey attributeValueListKey = new AttributeValueListKey();
        attributeValueListKey.setNamespace(attributeValueListEntity.getNamespace().getCode());
        attributeValueListKey.setAttributeValueListName(attributeValueListEntity.getAttributeValueListName());

        attributeValueList.setAttributeValueListKey(attributeValueListKey);
        attributeValueList.setId(attributeValueListEntity.getId());

        return attributeValueList;
    }

    @NamespacePermission(fields = "#attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AttributeValueListKey deleteAttributeValueList(AttributeValueListKey attributeValueListKey)
    {
        // Perform validation and trim.
        attributeValueListHelper.validateAttributeValueListKey(attributeValueListKey);

        // Retrieve and ensure that a attribute value list already exists with the specified key.
        AttributeValueListEntity attributeValueListEntity = attributeValueListHelper.getAttributeValueListEntity(attributeValueListKey);

        // Delete the attribute value list.
        attributeValueListDao.delete(attributeValueListEntity);

        // Create and return the attribute value list object from the deleted entity.
        return new AttributeValueListKey(attributeValueListEntity.getNamespace().getCode(), attributeValueListEntity.getAttributeValueListName());
    }

    @NamespacePermission(fields = "#attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AttributeValueList getAttributeValueList(AttributeValueListKey attributeValueListKey)
    {
        AttributeValueListEntity attributeValueListEntity = attributeValueListDao.getAttributeValueListByKey(attributeValueListKey);
        return createAttributeValueListFromEntity(attributeValueListEntity);
    }

    @NamespacePermission(fields = "#attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AttributeValueListKeys getAttributeValueListKeys()
    {
        return attributeValueListDao.getAttributeValueListKeys();
    }

    /**
     * Validates attribute value list create request.
     *
     * @param attributeValueListCreateRequest
     */
    private void validateAttributeValueListCreateRequest(AttributeValueListCreateRequest attributeValueListCreateRequest)
    {
        Assert.notNull(attributeValueListCreateRequest, "A Attribute value list create request must be specified.");
        attributeValueListHelper.validateAttributeValueListKey(attributeValueListCreateRequest.getAttributeValueListKey());
    }
}
