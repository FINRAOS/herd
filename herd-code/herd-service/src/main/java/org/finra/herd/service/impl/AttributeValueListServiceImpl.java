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
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.service.AttributeValueListService;
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

    @NamespacePermission(fields = "#request.attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public AttributeValueList createAttributeValueList(AttributeValueListCreateRequest attributeValueListCreateRequest)
    {
        // Validate and trim the request parameters.
        validateAttributeValueListCreateRequest(attributeValueListCreateRequest);

        // Validate the tag type does not already exist in the database.
        if (attributeValueListDao.getAttributeValueListByKey(attributeValueListCreateRequest.getAttributeValueListKey()) != null)
        {
            throw new AlreadyExistsException(String.format("Unable to create attribute value list with code \"%s\" and \"%s\" because it already exists.",
                attributeValueListCreateRequest.getAttributeValueListKey().getNamespace(),
                attributeValueListCreateRequest.getAttributeValueListKey().getAttributeValueListName()));
        }

        // Validate the tag type does not already exist in the database.
        if (attributeValueListDao.getAttributeValueListByKey(attributeValueListCreateRequest.getAttributeValueListKey()) != null)
        {
            throw new AlreadyExistsException(String.format("Unable to create attribute value list with code \"%s\" because it already exists.",
                attributeValueListCreateRequest.getAttributeValueListKey()));
        }

        // Create and persist a new tag type entity from the request information.
        AttributeValueListEntity attributeValueListEntity = createAttributeValueListEntity(attributeValueListCreateRequest);

        // Create and return the tag type object from the persisted entity.
        return new AttributeValueList(attributeValueListEntity.getId(),
            new AttributeValueListKey(attributeValueListEntity.getNamespace().getCode(), attributeValueListEntity.getAttributeValueListName()));
    }

    @NamespacePermission(fields = "#attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AttributeValueList getAttributeValueList(AttributeValueListKey attributeValueListKey)
    {
        return attributeValueListDao.getAttributeValueListByKey(attributeValueListKey);
    }

    @NamespacePermission(fields = "#attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AttributeValueListKey deleteAttributeValueList(AttributeValueListKey attributeValueListKey)
    {
        // Perform validation and trim.
        attributeValueListHelper.validateAttributeValueListKey(attributeValueListKey);

        // Retrieve and ensure that a tag type already exists with the specified key.
        AttributeValueListEntity attributeValueListEntity = attributeValueListHelper.getAttributeValueListEntity(attributeValueListKey);

        // Delete the tag type.
        attributeValueListDao.delete(attributeValueListEntity);

        // Create and return the tag type object from the deleted entity.
        return new AttributeValueListKey(attributeValueListEntity.getNamespace().getCode(), attributeValueListEntity.getAttributeValueListName());
    }

    @NamespacePermission(fields = "#attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AttributeValueListKeys getAttributeValueListKeys()
    {
        return attributeValueListDao.getAttributeValueListKeys();
    }

    private AttributeValueListEntity createAttributeValueListEntity(AttributeValueListCreateRequest attributeValueListCreateRequest)
    {
        // Create a new entity.
        AttributeValueListEntity attributeValueListEntity =
            attributeValueListHelper.getAttributeValueListEntity(attributeValueListCreateRequest.getAttributeValueListKey());

        // Persist and return the new entity.
        return attributeValueListDao.saveAndRefresh(attributeValueListEntity);
    }

    private void validateAttributeValueListCreateRequest(AttributeValueListCreateRequest attributeValueListCreateRequest)
    {
        Assert.notNull(attributeValueListCreateRequest, "A Attribute value list create request must be specified.");
        attributeValueListHelper.validateAttributeValueListKey(attributeValueListCreateRequest.getAttributeValueListKey());
    }
}
