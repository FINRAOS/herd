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

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.UserAuthorizations;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.service.CurrentUserService;
import org.finra.herd.service.helper.SecurityRoleDaoHelper;

/**
 * The current user service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class CurrentUserServiceImpl implements CurrentUserService
{
    @Autowired
    private SecurityRoleDaoHelper securityRoleDaoHelper;

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
                userAuthorizations.setSecurityRoles(new ArrayList<>(securityRoleDaoHelper.getValidSecurityRoles(applicationUser.getRoles())));
            }

            userAuthorizations.setNamespaceAuthorizations(new ArrayList<>(applicationUser.getNamespaceAuthorizations()));
        }

        return userAuthorizations;
    }
}
