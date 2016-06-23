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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import org.finra.herd.model.api.xml.IamRole;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorization;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizations;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NamespaceIamRoleAuthorizationEntity;
import org.finra.herd.service.NamespaceIamRoleAuthorizationService;
import org.finra.herd.service.helper.NamespaceDaoHelper;

@Service
@Transactional
public class NamespaceIamRoleAuthorizationServiceImpl implements NamespaceIamRoleAuthorizationService
{
    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private NamespaceIamRoleAuthorizationDao namespaceIamRoleAuthorizationDao;

    @NamespacePermission(fields = "#request?.namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public NamespaceIamRoleAuthorization createNamespaceIamRoleAuthorization(NamespaceIamRoleAuthorizationCreateRequest request)
    {
        Assert.notNull(request, "NamespaceIamRoleAuthorizationCreateRequest must be specified");
        Assert.hasText(request.getNamespace(), "Namespace must be specified");
        validateIamRoles(request.getIamRoles());

        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(request.getNamespace().trim());

        assertNamespaceIamRoleAuthorizationNotExist(namespaceEntity);

        NamespaceIamRoleAuthorization result = new NamespaceIamRoleAuthorization(namespaceEntity.getCode(), new ArrayList<>());
        for (IamRole iamRole : request.getIamRoles())
        {
            NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = createNamespaceIamRoleAuthorizationEntity(namespaceEntity, iamRole);
            namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity);
            result.getIamRoles().add(new IamRole(namespaceIamRoleAuthorizationEntity.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getDescription()));
        }
        return result;
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public NamespaceIamRoleAuthorization getNamespaceIamRoleAuthorization(String namespace)
    {
        Assert.hasText(namespace, "Namespace must be specified");

        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespace.trim());

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = getNamespaeIamRoleAuthorizationEntities(namespaceEntity);

        NamespaceIamRoleAuthorization result = new NamespaceIamRoleAuthorization(namespaceEntity.getCode(), new ArrayList<>());
        for (NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity : namespaceIamRoleAuthorizationEntities)
        {
            result.getIamRoles().add(new IamRole(namespaceIamRoleAuthorizationEntity.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getDescription()));
        }
        return result;
    }

    @Override
    public NamespaceIamRoleAuthorizations getNamespaceIamRoleAuthorizations()
    {
        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities =
            namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(null);

        Map<String, NamespaceIamRoleAuthorization> map = new LinkedHashMap<>();
        for (NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity : namespaceIamRoleAuthorizationEntities)
        {
            String namespace = namespaceIamRoleAuthorizationEntity.getNamespace().getCode();

            NamespaceIamRoleAuthorization namespaceIamRoleAuthorization = map.get(namespace);
            if (namespaceIamRoleAuthorization == null)
            {
                map.put(namespace, namespaceIamRoleAuthorization = new NamespaceIamRoleAuthorization(namespace, new ArrayList<>()));
            }
            namespaceIamRoleAuthorization.getIamRoles()
                .add(new IamRole(namespaceIamRoleAuthorizationEntity.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getDescription()));
        }

        return new NamespaceIamRoleAuthorizations(new ArrayList<>(map.values()));
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public NamespaceIamRoleAuthorization updateNamespaceIamRoleAuthorization(String namespace, NamespaceIamRoleAuthorizationUpdateRequest request)
    {
        Assert.hasText(namespace, "Namespace must be specified");
        Assert.notNull(request, "NamespaceIamRoleAuthorizationCreateRequest must be specified");
        validateIamRoles(request.getIamRoles());

        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespace.trim());

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = getNamespaeIamRoleAuthorizationEntities(namespaceEntity);

        for (NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity : namespaceIamRoleAuthorizationEntities)
        {
            namespaceIamRoleAuthorizationDao.delete(namespaceIamRoleAuthorizationEntity);
        }

        NamespaceIamRoleAuthorization result = new NamespaceIamRoleAuthorization(namespaceEntity.getCode(), new ArrayList<>());
        for (IamRole iamRole : request.getIamRoles())
        {
            NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = createNamespaceIamRoleAuthorizationEntity(namespaceEntity, iamRole);
            namespaceIamRoleAuthorizationDao.saveAndRefresh(namespaceIamRoleAuthorizationEntity);
            result.getIamRoles().add(new IamRole(namespaceIamRoleAuthorizationEntity.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getDescription()));
        }
        return result;
    }

    @NamespacePermission(fields = "#namespace", permissions = NamespacePermissionEnum.GRANT)
    @Override
    public NamespaceIamRoleAuthorization deleteNamespaceIamRoleAuthorization(String namespace)
    {
        Assert.hasText(namespace, "Namespace must be specified");

        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(namespace.trim());

        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities = getNamespaeIamRoleAuthorizationEntities(namespaceEntity);

        NamespaceIamRoleAuthorization result = new NamespaceIamRoleAuthorization(namespaceEntity.getCode(), new ArrayList<>());
        for (NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity : namespaceIamRoleAuthorizationEntities)
        {
            namespaceIamRoleAuthorizationDao.delete(namespaceIamRoleAuthorizationEntity);
            result.getIamRoles().add(new IamRole(namespaceIamRoleAuthorizationEntity.getIamRoleName(), namespaceIamRoleAuthorizationEntity.getDescription()));
        }
        return result;
    }

    /**
     * Asserts that the given IAM roles are valid as a user input. The IAM roles are valid if not null, not empty, and each element's name is not null and not
     * blank.
     *
     * @param iamRoles The list of IAM roles to validate
     */
    private void validateIamRoles(List<IamRole> iamRoles)
    {
        Assert.notNull(iamRoles, "At least 1 IAM roles must be specified");
        Assert.isTrue(iamRoles.size() > 0, "At least 1 IAM roles must be specified");
        for (IamRole iamRole : iamRoles)
        {
            Assert.notNull(iamRole, "IAM role must be specified");
            Assert.hasText(iamRole.getIamRoleName(), "IAM role name must be specified");
        }
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
     * @param iamRole The IAM role
     *
     * @return The NamespaceIamRoleAuthorizationEntity
     */
    private NamespaceIamRoleAuthorizationEntity createNamespaceIamRoleAuthorizationEntity(NamespaceEntity namespaceEntity, IamRole iamRole)
    {
        NamespaceIamRoleAuthorizationEntity namespaceIamRoleAuthorizationEntity = new NamespaceIamRoleAuthorizationEntity();
        namespaceIamRoleAuthorizationEntity.setNamespace(namespaceEntity);
        namespaceIamRoleAuthorizationEntity.setIamRoleName(iamRole.getIamRoleName().trim());
        if (StringUtils.isNotBlank(iamRole.getIamRoleDescription()))
        {
            namespaceIamRoleAuthorizationEntity.setDescription(iamRole.getIamRoleDescription().trim());
        }
        return namespaceIamRoleAuthorizationEntity;
    }

    /**
     * Gets a list of NamespaceIamRoleAuthorizationEntities for the given namespace. Throws a ObjectNotFoundException if the result is empty.
     *
     * @param namespaceEntity The namespace entity
     *
     * @return List of NamespaceIamRoleAuthorizationEntity
     */
    private List<NamespaceIamRoleAuthorizationEntity> getNamespaeIamRoleAuthorizationEntities(NamespaceEntity namespaceEntity)
    {
        List<NamespaceIamRoleAuthorizationEntity> namespaceIamRoleAuthorizationEntities =
            namespaceIamRoleAuthorizationDao.getNamespaceIamRoleAuthorizations(namespaceEntity);

        if (CollectionUtils.isEmpty(namespaceIamRoleAuthorizationEntities))
        {
            throw new ObjectNotFoundException(String.format("Namespace IAM role authorizations for namespace \"%s\" do not exist", namespaceEntity.getCode()));
        }
        return namespaceIamRoleAuthorizationEntities;
    }
}
