package org.finra.herd.service;


import org.finra.herd.model.api.xml.SecurityRole;
import org.finra.herd.model.api.xml.SecurityRoleCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.api.xml.SecurityRoleKeys;
import org.finra.herd.model.api.xml.SecurityRoleUpdateRequest;

/**
 * The security Role service.
 */
public interface SecurityRoleService
{
    /**
     * Creates a new security role
     *
     * @param securityRoleCreateRequest the security role create request
     *
     * @return the created security role
     */
    SecurityRole createSecurityRole(SecurityRoleCreateRequest securityRoleCreateRequest);

    /**
     * Gets a security role
     *
     * @param securityRoleKey the security role key
     *
     * @return the retrieved security role
     */
    SecurityRole getSecurityRole(SecurityRoleKey securityRoleKey);

    /**
     * Deletes a security role
     *
     * @param securityRoleName the security role name
     *
     * @return the security role that was deleted
     */
    SecurityRole deleteSecurityRole(SecurityRoleKey securityRoleName);

    /**
     * Updates a security role
     *
     * @param securityRoleKey the security role key
     *
     * @param securityRoleUpdateRequest the security role update request
     *
     * @return the security role that was updated
     */
    SecurityRole updateSecurityRole(SecurityRoleKey securityRoleKey , SecurityRoleUpdateRequest securityRoleUpdateRequest);

    /**
     * Gets a list of security role keys for all security roles defined in the system
     *
     * @return the security role keys
     */
    SecurityRoleKeys getSecurityRoles();
}
