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
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.AttributeValueListService;
import org.finra.herd.service.helper.AttributeValueListDaoHelper;
import org.finra.herd.service.helper.AttributeValueListHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;
import org.finra.herd.service.helper.NamespaceSecurityHelper;


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
    private AttributeValueListDaoHelper attributeValueListDaoHelper;

    @Autowired
    private AttributeValueListHelper attributeValueListHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private NamespaceSecurityHelper namespaceSecurityHelper;

    @NamespacePermission(fields = "#request.attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public AttributeValueList createAttributeValueList(AttributeValueListCreateRequest request)
    {
        // Validate and trim the request parameters.
        attributeValueListHelper.validateAttributeValueListCreateRequest(request);

        // Get the attribute value list key.
        AttributeValueListKey attributeValueListKey = request.getAttributeValueListKey();

        // Retrieve the namespace entity and validate it exists.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(request.getAttributeValueListKey().getNamespace());

        // Validate the attribute value list does not already exist.
        if (attributeValueListDao.getAttributeValueListByKey(request.getAttributeValueListKey()) != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create attribute value list with name \"%s\" because it already exists for namespace \"%s\".",
                    attributeValueListKey.getAttributeValueListName(), attributeValueListKey.getNamespace()));
        }

        // Create and persist a new attribute value list entity from the request information.
        AttributeValueListEntity attributeValueListEntity = createAttributeValueListEntity(request, namespaceEntity);

        // Create and return the attribute value list object from the persisted entity.
        return attributeValueListDaoHelper.createAttributeValueListFromEntity(attributeValueListEntity);
    }

    @NamespacePermission(fields = "#attributeValueListKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public AttributeValueList deleteAttributeValueList(AttributeValueListKey attributeValueListKey)
    {
        // Perform validation and trim.
        attributeValueListHelper.validateAttributeValueListKey(attributeValueListKey);

        // Retrieve and ensure that an attribute value list already exists with the specified key.
        AttributeValueListEntity attributeValueListEntity = attributeValueListDaoHelper.getAttributeValueListEntity(attributeValueListKey);

        // Delete the attribute value list.
        attributeValueListDao.delete(attributeValueListEntity);

        // Create and return the attribute value list object from the deleted entity.
        return attributeValueListDaoHelper.createAttributeValueListFromEntity(attributeValueListEntity);
    }

    @NamespacePermission(fields = "#attributeValueListKey.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public AttributeValueList getAttributeValueList(AttributeValueListKey attributeValueListKey)
    {
        // Perform validation and trim.
        attributeValueListHelper.validateAttributeValueListKey(attributeValueListKey);

        // Retrieve and ensure that an attribute value list already exists with the specified key.
        AttributeValueListEntity attributeValueListEntity = attributeValueListDaoHelper.getAttributeValueListEntity(attributeValueListKey);

        // Create and return the attribute value list object from the deleted entity.
        return attributeValueListDaoHelper.createAttributeValueListFromEntity(attributeValueListEntity);
    }

    @Override
    public AttributeValueListKeys getAttributeValueLists()
    {
        // Get the namespaces which the current user is authorized to READ.
        Set<String> authorizedNamespaces = namespaceSecurityHelper.getAuthorizedNamespaces(NamespacePermissionEnum.READ);

        // Create an empty list of keys.
        List<AttributeValueListKey> attributeValueListKeys = new ArrayList<>();

        // Continue the processing only when the list of authorized namespaces is not empty.
        if (CollectionUtils.isNotEmpty(authorizedNamespaces))
        {
            attributeValueListKeys.addAll(attributeValueListDao.getAttributeValueLists(authorizedNamespaces));
        }

        // Return the list of keys.
        return new AttributeValueListKeys(attributeValueListKeys);
    }

    /**
     * Creates and persists a new attribute value list entity from the request information.
     *
     * @param request the request
     * @param namespaceEntity the namespace
     *
     * @return the newly created attribute value list entity
     */
    private AttributeValueListEntity createAttributeValueListEntity(AttributeValueListCreateRequest request, NamespaceEntity namespaceEntity)
    {
        // Create a new entity.
        AttributeValueListEntity attributeValueListEntity = new AttributeValueListEntity();
        attributeValueListEntity.setNamespace(namespaceEntity);
        attributeValueListEntity.setName(request.getAttributeValueListKey().getAttributeValueListName());

        // Persist and return the newly created entity.
        return attributeValueListDao.saveAndRefresh(attributeValueListEntity);
    }
}
