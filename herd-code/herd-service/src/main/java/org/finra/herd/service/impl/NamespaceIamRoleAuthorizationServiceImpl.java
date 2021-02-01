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
    public NamespaceIamRoleAuthorization createNamespaceIamRoleAuthorization(final NamespaceIamRoleAuthorizationCreateRequest request)
    {
        // Validate the namespace IAM role authorization create request.
        Assert.notNull(request, "NamespaceIamRoleAuthorizationCreateRequest must be specified");
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey = request.getNamespaceIamRoleAuthorizationKey();
        Assert.notNull(namespaceIamRoleAuthorizationKey, "NamespaceIamRoleAuthorizationKey must be specified");
        Assert.hasText(namespaceIamRoleAuthorizationKey.getNamespace(), "Namespace must be specified");
        Assert.hasText(namespaceIamRoleAuthorizationKey.getIamRoleName(), "IAM role name must be specified");

        // Retrieve the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespaceIamRoleAuthorizationKey.getNamespace().trim());

        // Verify that a namespace IAM role authorization for this namespace and IAM role name does not already exist.
        assertNamespaceIamRoleAuthorizationEntityNotExist(namespaceIamRoleAuthorizationKey);

        // Create the new namespace IAM role authorization entity.
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            createNamespaceIamRoleAuthorizationEntity(namespaceEntity, request.getNamespaceIamRoleAuthorizationKey().getIamRoleName(),
                request.getIamRoleDescription());

        // Persist the namespace IAM role authorization entity.
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity);

        return getNamespaceIamRoleAuthorizationFromEntity(namespaceIamRoleAuthorizationEntity);
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public NamespaceIamRoleAuthorization deleteNamespaceIamRoleAuthorization(final NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey)
    {
        // Validate the namespace IAM role authorization key.
        Assert.notNull(namespaceIamRoleAuthorizationKey, "NamespaceIamRoleAuthorizationKey must be specified");
        Assert.hasText(namespaceIamRoleAuthorizationKey.getNamespace(), "Namespace must be specified");
        Assert.hasText(namespaceIamRoleAuthorizationKey.getIamRoleName(), "IAM role name must be specified");
        namespaceDaoHelper.getNamespaceEntity(namespaceIamRoleAuthorizationKey.getNamespace().trim());

        // Get the namespace IAM role authorization entity.
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            retrieveAndValidateNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey);

        // Delete the namespace IAM role authorization entity.
        namespaceIamRoleAuthorizationDao.delete(namespaceIamRoleAuthorizationEntity);

        return getNamespaceIamRoleAuthorizationFromEntity(namespaceIamRoleAuthorizationEntity);
    }

    @Override
    public NamespaceIamRoleAuthorization getNamespaceIamRoleAuthorization(final NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey)
    {
        // Validate and trim the namespace IAM role authorization key.
        validateAndTrimNamespaceIamRoleAuthorizationKey(namespaceIamRoleAuthorizationKey);

        // Get the namespace IAM role authorization entity.
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            retrieveAndValidateNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey);

        return getNamespaceIamRoleAuthorizationFromEntity(namespaceIamRoleAuthorizationEntity);
    }

    @Override
    public NamespaceIamRoleAuthorizationKeys getNamespaceIamRoleAuthorizations()
    {
        // Get all of the namespace IAM role authorization entities.
        return getNamespaceIamRoleAuthorizationKeysFromEntityLists(namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(null));
    }

    @Override
    public NamespaceIamRoleAuthorizationKeys getNamespaceIamRoleAuthorizationsByIamRoleName(String iamRoleName)
    {
        // Validate the IAM role name.
        iamRoleName = alternateKeyHelper.validateStringParameter("An", "IAM role name", iamRoleName);

        // Get the namespace IAM role authorization entities by IAM role name.
        return getNamespaceIamRoleAuthorizationKeysFromEntityLists(
            namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizationsByIamRoleName(iamRoleName));
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public NamespaceIamRoleAuthorizationKeys getNamespaceIamRoleAuthorizationsByNamespace(String namespace)
    {
        // Validate the namespace.
        namespace = alternateKeyHelper.validateStringParameter("namespace", namespace);
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespace);

        // Get the namespace IAM role authorization entities by namespace.
        return getNamespaceIamRoleAuthorizationKeysFromEntityLists(
            namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(namespaceEntity));
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public NamespaceIamRoleAuthorization updateNamespaceIamRoleAuthorization(final NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey,
        final NamespaceIamRoleAuthorizationUpdateRequest request)
    {
        // Validate the namespace IAM role authorization key and namespace IAM role authorization update request.
        Assert.notNull(namespaceIamRoleAuthorizationKey, "NamespaceIamRoleAuthorizationKey must be specified");
        Assert.hasText(namespaceIamRoleAuthorizationKey.getNamespace(), "Namespace must be specified");
        Assert.hasText(namespaceIamRoleAuthorizationKey.getIamRoleName(), "IAM role name must be specified");
        Assert.notNull(request, "NamespaceIamRoleAuthorizationUpdateRequest must be specified");
        namespaceDaoHelper.getNamespaceEntity(namespaceIamRoleAuthorizationKey.getNamespace().trim());

        // Get the namespace IAM role authorization entity.
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            retrieveAndValidateNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey);

        // Update the namespace IAM role authorization entity.
        namespaceIamRoleAuthorizationEntity.setDescription(request.getIamRoleDescription());

        // Persist the namespace IAM role authorization entity.
        namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity);

        return getNamespaceIamRoleAuthorizationFromEntity(namespaceIamRoleAuthorizationEntity);
    }

    /**
     * Asserts that no NamespaceIamRoleAuthorizationEntity exists for the given namespace Iam role authorization key. Throws a AlreadyExistsException if any
     * NamespaceIamRoleAuthorizationEntity exists.
     *
     * @param namespaceIamRoleAuthorizationKey The namespace IAM role authorization key
     *
     * @throws AlreadyExistsException if the namespace IAM role authorization entity already exists
     */
    private void assertNamespaceIamRoleAuthorizationEntityNotExist(final NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey)
    {
        // Check to see if a namespace IAM role authorization entity exists.
        if (namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey) != null)
        {
            throw new AlreadyExistsException(String.format("Namespace IAM role authorization with namespace \"%s\" and IAM role name \"%s\" already exists",
                namespaceIamRoleAuthorizationKey.getNamespace(), namespaceIamRoleAuthorizationKey.getIamRoleName()));
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
    private NamespaceIamRoleAuthorizationEntity createNamespaceIamRoleAuthorizationEntity(final NamespaceEntity namespaceEntity, final String iamRoleName,
        final String iamRoleDescription)
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
     * Converts a namespace IAM role authorization entity into a namespace IAM role authorization.
     *
     * @param namespaceIamRoleAuthorizationEntity the namespace IAM role authorization entity
     *
     * @return the namespace IAM role authorization
     */
    private NamespaceIamRoleAuthorization getNamespaceIamRoleAuthorizationFromEntity(
        final NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity)
    {
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey =
            new NamespaceIamRoleAuthorizationKey(namespaceIamRoleAuthorizationEntity.getNamespace().getCode(),
                namespaceIamRoleAuthorizationEntity.getIamRoleName());

        return new NamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationEntity.getId(), namespaceIamRoleAuthorizationKey,
            namespaceIamRoleAuthorizationEntity.getDescription());
    }

    /**
     * Converts a namespace IAM role authorization entity list into a namespace IAM role authorization keys object.
     *
     * @param namespaceIamRoleAuthorizationEntities a list of namespace IAM role authorization entities
     *
     * @return the namespace IAM role authorization keys
     */
    private NamespaceIamRoleAuthorizationKeys getNamespaceIamRoleAuthorizationKeysFromEntityLists(
        final List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities)
    {
        // Create a new namespace IAM role authorization keys object to hold the keys obtained from the get namespace IAM role authorizations DAO call.
        NamespaceIamRoleAuthorizationKeys namespaceIamRoleAuthorizationKeys = new NamespaceIamRoleAuthorizationKeys();

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

    /**
     * Retrieves a namespace IAM role authorization entity by the namespace IAM role authorization key and validates that the entity exists.
     *
     * @param namespaceIamRoleAuthorizationKey the namespace IAM role key
     *
     * @return the namespace IAM role authorization entity
     * @throws ObjectNotFoundException if the namespace IAM role authorization is not found
     */
    private NamespaceIamRoleAuthorizationEntity retrieveAndValidateNamespaceIamRoleAuthorization(
        final NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey)
    {
        // Get the namespace IAM role authorization entity.
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity =
            namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey);

        // If no namespace IAM role authorization entity is found, throw an object not exception.
        if (namespaceIamRoleAuthorizationEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Namespace IAM role authorization for namespace \"%s\" and IAM role name \"%s\" does not exist",
                namespaceIamRoleAuthorizationKey.getNamespace(), namespaceIamRoleAuthorizationKey.getIamRoleName()));
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
