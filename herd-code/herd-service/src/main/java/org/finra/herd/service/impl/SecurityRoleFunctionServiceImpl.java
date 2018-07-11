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

import org.finra.herd.dao.SecurityRoleFunctionDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.SecurityFunctionKey;
import org.finra.herd.model.api.xml.SecurityRoleFunction;
import org.finra.herd.model.api.xml.SecurityRoleFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKeys;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;
import org.finra.herd.service.SecurityRoleFunctionService;
import org.finra.herd.service.helper.SecurityFunctionDaoHelper;
import org.finra.herd.service.helper.SecurityFunctionHelper;
import org.finra.herd.service.helper.SecurityRoleDaoHelper;
import org.finra.herd.service.helper.SecurityRoleFunctionDaoHelper;
import org.finra.herd.service.helper.SecurityRoleFunctionHelper;
import org.finra.herd.service.helper.SecurityRoleHelper;

/**
 * The security role to function mapping service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class SecurityRoleFunctionServiceImpl implements SecurityRoleFunctionService
{
    @Autowired
    private SecurityFunctionDaoHelper securityFunctionDaoHelper;

    @Autowired
    private SecurityFunctionHelper securityFunctionHelper;

    @Autowired
    private SecurityRoleDaoHelper securityRoleDaoHelper;

    @Autowired
    private SecurityRoleFunctionDao securityRoleFunctionDao;

    @Autowired
    private SecurityRoleFunctionDaoHelper securityRoleFunctionDaoHelper;

    @Autowired
    private SecurityRoleFunctionHelper securityRoleFunctionHelper;

    @Autowired
    private SecurityRoleHelper securityRoleHelper;

    @Override
    public SecurityRoleFunction createSecurityRoleFunction(SecurityRoleFunctionCreateRequest securityRoleFunctionCreateRequest)
    {
        // Validate and trim the security role to function mapping create request.
        securityRoleFunctionHelper.validateAndTrimSecurityRoleFunctionCreateRequest(securityRoleFunctionCreateRequest);

        // Ensure a security role to function mapping with the specified parameters doesn't already exist.
        SecurityRoleFunctionEntity securityRoleFunctionEntity =
            securityRoleFunctionDao.getSecurityRoleFunctionByKey(securityRoleFunctionCreateRequest.getSecurityRoleFunctionKey());
        if (securityRoleFunctionEntity != null)
        {
            throw new AlreadyExistsException(String.format(
                "Unable to create security role to function mapping for \"%s\" security role name and \"%s\" security function name because it already exists.",
                securityRoleFunctionCreateRequest.getSecurityRoleFunctionKey().getSecurityRoleName(),
                securityRoleFunctionCreateRequest.getSecurityRoleFunctionKey().getSecurityFunctionName()));
        }

        // Retrieve and ensure that a security role with the specified name exists.
        SecurityRoleEntity securityRoleEntity =
            securityRoleDaoHelper.getSecurityRoleEntity(securityRoleFunctionCreateRequest.getSecurityRoleFunctionKey().getSecurityRoleName());

        // Retrieve and ensure that a security function with the specified name exists.
        SecurityFunctionEntity securityFunctionEntity =
            securityFunctionDaoHelper.getSecurityFunctionEntity(securityRoleFunctionCreateRequest.getSecurityRoleFunctionKey().getSecurityFunctionName());

        // Creates and persist a security role to function mapping entity.
        securityRoleFunctionEntity = createSecurityRoleFunctionEntity(securityRoleEntity, securityFunctionEntity);

        // Persist the new entity.
        securityRoleFunctionDao.saveAndRefresh(securityRoleFunctionEntity);

        // Create a security role to function mapping object from the entity and return it.
        return createSecurityRoleFunctionFromEntity(securityRoleFunctionEntity);
    }

    @Override
    public SecurityRoleFunction deleteSecurityRoleFunction(SecurityRoleFunctionKey securityRoleFunctionKey)
    {
        // Validate and trim the security role to function mapping key.
        securityRoleFunctionHelper.validateAndTrimSecurityRoleFunctionKey(securityRoleFunctionKey);

        // Retrieve and ensure that a security role to function mapping with the specified key exists.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = securityRoleFunctionDaoHelper.getSecurityRoleFunctionEntity(securityRoleFunctionKey);

        // Delete this security role to function mapping.
        securityRoleFunctionDao.delete(securityRoleFunctionEntity);

        // Create a security role to function mapping object from the deleted entity and return it.
        return createSecurityRoleFunctionFromEntity(securityRoleFunctionEntity);
    }

    @Override
    public SecurityRoleFunction getSecurityRoleFunction(SecurityRoleFunctionKey securityRoleFunctionKey)
    {
        // Validate and trim the security role to function mapping key.
        securityRoleFunctionHelper.validateAndTrimSecurityRoleFunctionKey(securityRoleFunctionKey);

        // Retrieve and ensure that a security role to function mapping with the specified key exists.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = securityRoleFunctionDaoHelper.getSecurityRoleFunctionEntity(securityRoleFunctionKey);

        // Create a security role to function mapping object from the deleted entity and return it.
        return createSecurityRoleFunctionFromEntity(securityRoleFunctionEntity);
    }

    @Override
    public SecurityRoleFunctionKeys getSecurityRoleFunctions()
    {
        // Retrieve and return a list of security role to function mapping keys for all security role to function mappings registered in the system.
        SecurityRoleFunctionKeys securityRoleFunctionKeys = new SecurityRoleFunctionKeys();
        securityRoleFunctionKeys.getSecurityRoleFunctionKeys().addAll(securityRoleFunctionDao.getSecurityRoleFunctionKeys());
        return securityRoleFunctionKeys;
    }

    @Override
    public SecurityRoleFunctionKeys getSecurityRoleFunctionsBySecurityFunction(SecurityFunctionKey securityFunctionKey)
    {
        // Validate and trim the security role to function key.
        securityFunctionHelper.validateAndTrimSecurityFunctionKey(securityFunctionKey);

        // Retrieve and return a list of security role to function mapping keys for the specified security function.
        SecurityRoleFunctionKeys securityRoleFunctionKeys = new SecurityRoleFunctionKeys();
        securityRoleFunctionKeys.getSecurityRoleFunctionKeys()
            .addAll(securityRoleFunctionDao.getSecurityRoleFunctionKeysBySecurityFunction(securityFunctionKey.getSecurityFunctionName()));
        return securityRoleFunctionKeys;
    }

    @Override
    public SecurityRoleFunctionKeys getSecurityRoleFunctionsBySecurityRole(SecurityRoleKey securityRoleKey)
    {
        // Validate and trim the security role to function key.
        securityRoleHelper.validateAndTrimSecurityRoleKey(securityRoleKey);

        // Retrieve and return a list of security role to function mapping keys for the specified security role.
        SecurityRoleFunctionKeys securityRoleFunctionKeys = new SecurityRoleFunctionKeys();
        securityRoleFunctionKeys.getSecurityRoleFunctionKeys()
            .addAll(securityRoleFunctionDao.getSecurityRoleFunctionKeysBySecurityRole(securityRoleKey.getSecurityRoleName()));
        return securityRoleFunctionKeys;
    }

    /**
     * Creates a security role to function mapping.
     *
     * @param securityRoleEntity the security role entity
     * @param securityFunctionEntity the security function entity
     *
     * @return the security role to function mapping entity
     */
    private SecurityRoleFunctionEntity createSecurityRoleFunctionEntity(SecurityRoleEntity securityRoleEntity, SecurityFunctionEntity securityFunctionEntity)
    {
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();
        securityRoleFunctionEntity.setSecurityRole(securityRoleEntity);
        securityRoleFunctionEntity.setSecurityFunction(securityFunctionEntity);
        return securityRoleFunctionEntity;
    }

    /**
     * Creates a security role to function mapping from the specified entity.
     *
     * @param securityRoleFunctionEntity the security role to function entity
     *
     * @return the security role to function mapping
     */
    private SecurityRoleFunction createSecurityRoleFunctionFromEntity(SecurityRoleFunctionEntity securityRoleFunctionEntity)
    {
        return new SecurityRoleFunction(securityRoleFunctionEntity.getId(),
            new SecurityRoleFunctionKey(securityRoleFunctionEntity.getSecurityRole().getCode(), securityRoleFunctionEntity.getSecurityFunction().getCode()));
    }
}
