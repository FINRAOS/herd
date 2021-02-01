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

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.NamespaceIamRoleAuthorizationDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorization;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationKey;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationKeys;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceIamRoleAuthorizationEntity;
import org.finra.herd.service.NamespaceIamRoleAuthorizationService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.NamespaceDaoHelper;

@Service
@Transactional
public class NamespaceIamRoleAuthorizationServiceImpl implements NamespaceIamRoleAuthorizationService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private NamespaceIamRoleAuthorizationDao namespaceIamRoleAuthorizationDao;

    @NamespacePermission(fields = "#request?.namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public NamespaceIamRoleAuthorization createNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationCreateRequest request)
    {
        Assert.notNull(request, "NamespaceIamRoleAuthorizationCreateRequest must be specified");
        Assert.hasText(request.getNamespaceIamRoleAuthorizationKey().getNamespace(), "Namespace must be specified");
        Assert.hasText(request.getNamespaceIamRoleAuthorizationKey().getIamRoleName(), "IAM role name must be specified");

        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(request.getNamespaceIamRoleAuthorizationKey().getNamespace().trim());

        assertNamespaceIamRoleAuthorizationNotExist(namespaceEntity);

        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            createNamespaceIamRoleAuthorizationEntity(namespaceEntity, request.getNamespaceIamRoleAuthorizationKey().getIamRoleName(),
                request.getIamRoleDescription());
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity);

        return new NamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationEntity.getId(),
            new NamespaceIamRoleAuthorizationKey(namespaceIamRoleAuthorizationEntity.getNamespace().getCode(),
                namespaceIamRoleAuthorizationEntity.getIamRoleName()), namespaceIamRoleAuthorizationEntity.getDescription());
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public NamespaceIamRoleAuthorization deleteNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey)
    {
        Assert.hasText(namespaceIamRoleAuthorizationKey.getNamespace(), "Namespace must be specified");
        Assert.hasText(namespaceIamRoleAuthorizationKey.getIamRoleName(), "IAM role name must be specified");
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespaceIamRoleAuthorizationKey.getNamespace().trim());

        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey);

        // If no namespace IAM role authorization entity is found, throw an object not exception.
        if (namespaceIamRoleAuthorizationEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Namespace IAM role authorization for namespace \"%s\" and IAM role name \"%s\" does not exist",
                namespaceIamRoleAuthorizationKey.getNamespace(), namespaceIamRoleAuthorizationKey.getIamRoleName()));
        }

        namespaceIamRoleAuthorizationDao.delete(namespaceIamRoleAuthorizationEntity);

        return new NamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationEntity.getId(),
            new NamespaceIamRoleAuthorizationKey(namespaceIamRoleAuthorizationEntity.getNamespace().getCode(),
                namespaceIamRoleAuthorizationEntity.getIamRoleName()), namespaceIamRoleAuthorizationEntity.getDescription());
    }

    @Override
    public NamespaceIamRoleAuthorization getNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey)
    {
        // Validate and trim the namespace IAM role authorization key.
        validateAndTrimNamespaceIamRoleAuthorizationKey(namespaceIamRoleAuthorizationKey);

        // Get the namespace IAM role authorization entity.
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey);

        // If no namespace IAM role authorization entity is found, throw an object not exception.
        if (namespaceIamRoleAuthorizationEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Namespace IAM role authorization for namespace \"%s\" and IAM role name \"%s\" does not exist",
                namespaceIamRoleAuthorizationKey.getNamespace(), namespaceIamRoleAuthorizationKey.getIamRoleName()));
        }

        return new NamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationEntity.getId(),
            new NamespaceIamRoleAuthorizationKey(namespaceIamRoleAuthorizationEntity.getNamespace().getCode(),
                namespaceIamRoleAuthorizationEntity.getIamRoleName()), namespaceIamRoleAuthorizationEntity.getDescription());
    }

    @Override
    public NamespaceIamRoleAuthorizationKeys getNamespaceIamRoleAuthorizations()
    {
        // Create a new namespace IAM role authorization keys object to hold the keys obtained from the get namespace IAM role authorizations DAO call.
        NamespaceIamRoleAuthorizationKeys namespaceIamRoleAuthorizationKeys = new NamespaceIamRoleAuthorizationKeys();

        // Get all of the namespace IAM role authorization entities.
        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities =
            namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(null);

        // For each namespace IAM role authorization entity convert it to a new namespace IAM role authorization key and add that key to the list of keys in
        // the namespace authorization IAM role key object.
        for (NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity : namespaceIamRoleAuthorizationEntities)
        {
            namespaceIamRoleAuthorizationKeys.getNamespaceIamRoleAuthorizationKeys().add(
                new NamespaceIamRoleAuthorizationKey(namespaceIamRoleAuthorizationEntity.getNamespace().getCode(),
                    namespaceIamRoleAuthorizationEntity.getIamRoleName()));
        }

        return namespaceIamRoleAuthorizationKeys;
    }

    @Override
    public NamespaceIamRoleAuthorizationKeys getNamespaceIamRoleAuthorizationsByIamRoleName(String iamRoleName)
    {
        return null;
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public NamespaceIamRoleAuthorizationKeys getNamespaceIamRoleAuthorizationsByNamespace(String namespace)
    {
        return null;
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public NamespaceIamRoleAuthorization updateNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey,
        NamespaceIamRoleAuthorizationUpdateRequest request)
    {
        Assert.hasText(namespaceIamRoleAuthorizationKey.getNamespace(), "Namespace must be specified");
        Assert.hasText(namespaceIamRoleAuthorizationKey.getIamRoleName(), "IAM role name must be specified");
        Assert.notNull(request, "NamespaceIamRoleAuthorizationUpdateRequest must be specified");
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespaceIamRoleAuthorizationKey.getNamespace().trim());

        // Get the namespace IAM role authorization entity.
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey);

        // If no namespace IAM role authorization entity is found, throw an object not exception.
        if (namespaceIamRoleAuthorizationEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Namespace IAM role authorization for namespace \"%s\" and IAM role name \"%s\" does not exist",
                namespaceIamRoleAuthorizationKey.getNamespace(), namespaceIamRoleAuthorizationKey.getIamRoleName()));
        }

        // Update the namespace IAM role authorization entity.
        namespaceIamRoleAuthorizationEntity.setDescription(request.getIamRoleDescription());

        // Persist the namespace IAM role authorization entity.
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity);

        return new NamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationEntity.getId(),
            new NamespaceIamRoleAuthorizationKey(namespaceIamRoleAuthorizationEntity.getNamespace().getCode(),
                namespaceIamRoleAuthorizationEntity.getIamRoleName()), namespaceIamRoleAuthorizationEntity.getDescription());
    }

    /**
     * Asserts that no NamespaceIamRoleAuthorizationEntities exist for the given namespace. Throws a AlreadyExistsException if any
     * NamespaceIamRoleAuthorizationEntity exist.
     *
     * @param namespaceEntity The namespace entity
     */
    private void assertNamespaceIamRoleAuthorizationNotExist(NamespaceEntity namespaceEntity)
    {
        if (CollectionUtils.isNotEmpty(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(namespaceEntity)))
        {
            throw new AlreadyExistsException(String.format("Namespace IAM role authorizations with namespace \"%s\" already exist", namespaceEntity.getCode()));
        }
    }

    /**
     * Creates a new NamespaceIamRoleAuthorizationEntity from the given parameters.
     *
     * @param namespaceEntity The namespace entity
     * @param iamRoleName The IAM role name
     * @param iamRoleDescription The IAM role description
     *
     * @return The NamespaceIamRoleAuthorizationEntity
     */
    private NamespaceIamRoleAuthorizationEntity createNamespaceIamRoleAuthorizationEntity(NamespaceEntity namespaceEntity, String iamRoleName,
        String iamRoleDescription)
    {
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity.setNamespace(namespaceEntity);
        namespaceIamRoleAuthorizationEntity.setIamRoleName(iamRoleName.trim());
        if (StringUtils.isNotBlank(iamRoleDescription))
        {
            namespaceIamRoleAuthorizationEntity.setDescription(iamRoleDescription.trim());
        }
        return namespaceIamRoleAuthorizationEntity;
    }

    /**
     * Validates a namespace IAM role authorization key. This method also trims the key parameters.
     *
     * @param namespaceIamRoleAuthorizationKey the namespace IAM role key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    void validateAndTrimNamespaceIamRoleAuthorizationKey(NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey) throws IllegalArgumentException
    {
        Assert.notNull(namespaceIamRoleAuthorizationKey, "A namespace IAM role authorization key must be specified.");
        namespaceIamRoleAuthorizationKey.setNamespace(
            alternateKeyHelper.validateStringParameter("namespace", namespaceIamRoleAuthorizationKey.getNamespace()));
        namespaceIamRoleAuthorizationKey.setIamRoleName(
            alternateKeyHelper.validateStringParameter("An", "IAM role name", namespaceIamRoleAuthorizationKey.getIamRoleName()));
    }
}
