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
package org.finra.dm.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.dm.dao.DmDao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.AlreadyExistsException;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.api.xml.Namespace;
import org.finra.dm.model.api.xml.NamespaceCreateRequest;
import org.finra.dm.model.api.xml.NamespaceKey;
import org.finra.dm.model.api.xml.NamespaceKeys;
import org.finra.dm.service.NamespaceService;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.DmHelper;

/**
 * The namespace service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class NamespaceServiceImpl implements NamespaceService
{
    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private DmDao dmDao;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    /**
     * {@inheritDoc}
     */
    @Override
    public Namespace createNamespace(NamespaceCreateRequest request)
    {
        // Perform the validation.
        validateNamespaceCreateRequest(request);

        // Get the namespace key.
        NamespaceKey namespaceKey = new NamespaceKey(request.getNamespaceCode());

        // Ensure a namespace with the specified namespace key doesn't already exist.
        NamespaceEntity namespaceEntity = dmDao.getNamespaceByKey(namespaceKey);
        if (namespaceEntity != null)
        {
            throw new AlreadyExistsException(
                String.format("Unable to create namespace \"%s\" because it already exists.", namespaceKey.getNamespaceCode()));
        }

        // Create a namespace entity from the request information.
        namespaceEntity = createNamespaceEntity(request);

        // Persist the new entity.
        namespaceEntity = dmDao.saveAndRefresh(namespaceEntity);

        // Create and return the namespace object from the persisted entity.
        return createNamespaceFromEntity(namespaceEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Namespace getNamespace(NamespaceKey namespaceKey)
    {
        // Perform validation and trim.
        dmHelper.validateNamespaceKey(namespaceKey);

        // Retrieve and ensure that a namespace already exists with the specified key.
        NamespaceEntity namespaceEntity = dmDaoHelper.getNamespaceEntity(namespaceKey.getNamespaceCode());

        // Create and return the namespace object from the persisted entity.
        return createNamespaceFromEntity(namespaceEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Namespace deleteNamespace(NamespaceKey namespaceKey)
    {
        // Perform validation and trim.
        dmHelper.validateNamespaceKey(namespaceKey);

        // Retrieve and ensure that a namespace already exists with the specified key.
        NamespaceEntity namespaceEntity = dmDaoHelper.getNamespaceEntity(namespaceKey.getNamespaceCode());

        // Delete the namespace.
        dmDao.delete(namespaceEntity);

        // Create and return the namespace object from the deleted entity.
        return createNamespaceFromEntity(namespaceEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NamespaceKeys getNamespaces()
    {
        NamespaceKeys namespaceKeys = new NamespaceKeys();
        namespaceKeys.getNamespaceKeys().addAll(dmDao.getNamespaces());
        return namespaceKeys;
    }

    /**
     * Validates the namespace create request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateNamespaceCreateRequest(NamespaceCreateRequest request)
    {
        // Validate.
        Assert.hasText(request.getNamespaceCode(), "A namespace code must be specified.");

        // Remove leading and trailing spaces.
        request.setNamespaceCode(request.getNamespaceCode().trim());
    }

    /**
     * Creates a new namespace entity from the request information.
     *
     * @param request the request
     *
     * @return the newly created namespace entity
     */
    private NamespaceEntity createNamespaceEntity(NamespaceCreateRequest request)
    {
        // Create a new entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(request.getNamespaceCode());
        return namespaceEntity;
    }

    /**
     * Creates the namespace from the persisted entity.
     *
     * @param namespaceEntity the newly persisted namespace entity.
     *
     * @return the namespace.
     */
    private Namespace createNamespaceFromEntity(NamespaceEntity namespaceEntity)
    {
        // Create the namespace information.
        Namespace namespace = new Namespace();
        namespace.setNamespaceCode(namespaceEntity.getCode());
        return namespace;
    }
}
