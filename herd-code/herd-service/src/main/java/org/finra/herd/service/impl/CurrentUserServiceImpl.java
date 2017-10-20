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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.SecurityRoleDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.UserAuthorizations;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.service.CurrentUserService;

/**
 * The current user service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class CurrentUserServiceImpl implements CurrentUserService
{
    @Autowired
    private SecurityRoleDao securityRoleDao;

    @Override
    public UserAuthorizations getCurrentUser()
    {
        // Create the user authorizations.
        UserAuthorizations userAuthorizations = new UserAuthorizations();

        // Get the application user.
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication != null)
        {
            SecurityUserWrapper securityUserWrapper = (SecurityUserWrapper) authentication.getPrincipal();
            ApplicationUser applicationUser = securityUserWrapper.getApplicationUser();
            userAuthorizations.setUserId(applicationUser.getUserId());

            // If roles are present on the application user then filter the herd-specific security roles and add that information to the Current user.
            if (CollectionUtils.isNotEmpty(applicationUser.getRoles()))
            {
                userAuthorizations.setSecurityRoles(new ArrayList<>(getValidSecurityRoles(applicationUser.getRoles())));
            }

            // Get all granted authorities for this user.
            Collection<GrantedAuthority> grantedAuthorities = securityUserWrapper.getAuthorities();

            // Add relative security functions as per granted authorities, if any are present.
            if (CollectionUtils.isNotEmpty(grantedAuthorities))
            {
                userAuthorizations.setSecurityFunctions(
                    grantedAuthorities.stream().map(grantedAuthority -> new String(grantedAuthority.getAuthority())).collect(Collectors.toList()));
            }

            userAuthorizations.setNamespaceAuthorizations(new ArrayList<>(applicationUser.getNamespaceAuthorizations()));
        }

        return userAuthorizations;
    }

    /**
     * Filters a set of roles based on a list of role values specific for herd.
     *
     * @param roles A given set of roles
     *
     * @return Valid roles from the specified set of roles
     */
    private Set<String> getValidSecurityRoles(final Set<String> roles)
    {
        // Copy the set of specified roles to another set
        Set<String> incomingRoles = new HashSet<>(roles);

        // Copy the roles to a set for easier computation
        Set<SecurityRoleEntity> securityRoleEntities = new HashSet<>(securityRoleDao.getAllSecurityRoles());

        // Collect all security role codes from the entities
        Set<String> securityRoles = securityRoleEntities.stream().map(SecurityRoleEntity::getCode).collect(Collectors.toSet());

        // The Set of valid roles is the intersection of the two collections
        incomingRoles.retainAll(securityRoles);

        // Return valid roles
        return incomingRoles;
    }
}
