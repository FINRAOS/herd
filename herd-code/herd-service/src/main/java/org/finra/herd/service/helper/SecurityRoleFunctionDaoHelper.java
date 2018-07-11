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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.SecurityRoleFunctionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;

/**
 * Helper for security role to function mapping related operations which require DAO.
 */
@Component
public class SecurityRoleFunctionDaoHelper
{
    @Autowired
    private SecurityRoleFunctionDao securityRoleFunctionDao;

    /**
     * Gets a security role to function mapping by its key and makes sure that it exists.
     *
     * @param securityRoleFunctionKey the security role to function mapping key
     *
     * @return the security role to function mapping entity
     */
    public SecurityRoleFunctionEntity getSecurityRoleFunctionEntity(SecurityRoleFunctionKey securityRoleFunctionKey)
    {
        SecurityRoleFunctionEntity securityRoleFunctionEntity = securityRoleFunctionDao.getSecurityRoleFunctionByKey(securityRoleFunctionKey);

        if (securityRoleFunctionEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Security role to function mapping with \"%s\" security role name and \"%s\" security function name doesn't exist.",
                    securityRoleFunctionKey.getSecurityRoleName(), securityRoleFunctionKey.getSecurityFunctionName()));
        }

        return securityRoleFunctionEntity;
    }
}
