package org.finra.herd.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.SecurityRoleDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.SecurityRole;
import org.finra.herd.model.api.xml.SecurityRoleCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.api.xml.SecurityRoleKeys;
import org.finra.herd.model.api.xml.SecurityRoleUpdateRequest;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.service.SecurityRoleService;
import org.finra.herd.service.helper.AlternateKeyHelper;

/**
 * The security role service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class SecurityRoleServiceImpl implements SecurityRoleService
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    SecurityRoleDao securityRoleDao;

    @Override
    public SecurityRole createSecurityRole(SecurityRoleCreateRequest securityRoleCreateRequest)
    {
        // Validate security roles create request.
        Assert.notNull(securityRoleCreateRequest, "A security role create request must be specified.");
        String securityRoleName = alternateKeyHelper.validateStringParameter("security role name", securityRoleCreateRequest.getSecurityRoleName());
        securityRoleCreateRequest.setSecurityRoleName(securityRoleName);

        // Ensure a security role with the specified security role name doesn't already exist.
        SecurityRoleEntity securityRoleEntity = securityRoleDao.getSecurityRoleByName(securityRoleCreateRequest.getSecurityRoleName());
        if (securityRoleEntity != null)
        {
            throw new AlreadyExistsException(
                String.format("Unable to create security role \"%s\" because it already exists.", securityRoleCreateRequest.getSecurityRoleName()));
        }

        // Create a security role entity from the request information.
        securityRoleEntity = createSecurityRoleEntity(securityRoleCreateRequest);

        // Persist the new entity.
        securityRoleEntity = securityRoleDao.saveAndRefresh(securityRoleEntity);

        // Create and return the security role object from the persisted entity.
        return createSecurityRoleFromEntity(securityRoleEntity);
    }

    @Override
    public SecurityRole getSecurityRole(SecurityRoleKey securityRoleKey)
    {
        // validate and trim security role key.
        validateSecurityRoleKey(securityRoleKey);

        // get the security role entity.
        SecurityRoleEntity securityRoleEntity = securityRoleDao.getSecurityRoleByName(securityRoleKey.getSecurityRoleName());
        if (securityRoleEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Security role with name \"%s\" doesn't exist.", securityRoleKey.getSecurityRoleName()));
        }

        // create and return the security role from the security role entity.
        return createSecurityRoleFromEntity(securityRoleEntity);
    }

    @Override
    public SecurityRole deleteSecurityRole(SecurityRoleKey securityRoleKey)
    {
        // validate and trim security role key.
        validateSecurityRoleKey(securityRoleKey);

        // get the security role entity.
        SecurityRoleEntity securityRoleEntity = securityRoleDao.getSecurityRoleByName(securityRoleKey.getSecurityRoleName());
        if (securityRoleEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Security role with name \"%s\" doesn't exist.", securityRoleKey.getSecurityRoleName()));
        }

        // Delete the security role.
        securityRoleDao.delete(securityRoleEntity);

        // Create and return the security role object from the deleted entity.
        return createSecurityRoleFromEntity(securityRoleEntity);
    }

    @Override
    public SecurityRole updateSecurityRole(SecurityRoleKey securityRoleKey, SecurityRoleUpdateRequest securityRoleUpdateRequest)
    {
        // Validate and trim security role key.
        validateSecurityRoleKey(securityRoleKey);

        // Validate security role update request.
        Assert.notNull(securityRoleUpdateRequest, "A security role update request must be specified.");

        // Retrieve and ensure that the security role exists.
        SecurityRoleEntity securityRoleEntity = securityRoleDao.getSecurityRoleByName(securityRoleKey.getSecurityRoleName());
        if (securityRoleEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Security role with name \"%s\" doesn't exist.", securityRoleKey.getSecurityRoleName()));
        }

        // Update the security role entity.
        securityRoleEntity.setDescription(securityRoleUpdateRequest.getDescription());

        // Persist the entity.
        securityRoleEntity = securityRoleDao.saveAndRefresh(securityRoleEntity);

        // Create and return the security role from the security role entity.
        return createSecurityRoleFromEntity(securityRoleEntity);
    }

    @Override
    public SecurityRoleKeys getSecurityRoles()
    {
        // Get and return all the security role keys.
        SecurityRoleKeys securityRoleKeys = new SecurityRoleKeys();
        securityRoleKeys.getSecurityRoleKeys()
            .addAll(securityRoleDao.getSecurityRoleKeys());
        return securityRoleKeys;
    }

    /**
     * Creates a new security role entity from the request information
     *
     * @param securityRoleCreateRequest the request
     *
     * @return the newly created security role entity
     */
    private SecurityRoleEntity createSecurityRoleEntity(SecurityRoleCreateRequest securityRoleCreateRequest)
    {
        // Create a new entity.
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();
        securityRoleEntity.setCode(securityRoleCreateRequest.getSecurityRoleName());
        securityRoleEntity.setDescription(securityRoleCreateRequest.getDescription());
        return securityRoleEntity;
    }

    /**
     * Creates a new security role from the security role entity
     *
     * @param securityRoleEntity entity
     *
     * @return security role created
     */
    private SecurityRole createSecurityRoleFromEntity(SecurityRoleEntity securityRoleEntity)
    {
        return new SecurityRole(securityRoleEntity.getCode(),securityRoleEntity.getDescription());
    }

    /**
     * Validates and trims the security role key
     *
     * @param securityRoleKey securityRoleKey
     *
     * @return security role validated and trimmed
     */
    private String validateSecurityRoleKey(SecurityRoleKey securityRoleKey)
    {
        Assert.notNull(securityRoleKey, "A security role key must be specified.");
        return alternateKeyHelper.validateStringParameter("security role name", securityRoleKey.getSecurityRoleName());
    }
}
