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

import org.finra.herd.dao.SecurityFunctionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.SecurityFunctionEntity;

/**
 * Helper for security function related operations which require DAO.
 */
@Component
public class SecurityFunctionDaoHelper
{
    @Autowired
    private SecurityFunctionDao securityFunctionDao;

    /**
     * Gets a security function entity and ensure it exists.
     *
     * @param securityFunctionName the security function name (case insensitive)
     *
     * @return the security function entity
     */
    public SecurityFunctionEntity getSecurityFunctionEntity(String securityFunctionName)
    {
        SecurityFunctionEntity securityFunctionEntity = securityFunctionDao.getSecurityFunctionByName(securityFunctionName);

        if (securityFunctionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Security function with name \"%s\" doesn't exist.", securityFunctionName));
        }

        return securityFunctionEntity;
    }
}
