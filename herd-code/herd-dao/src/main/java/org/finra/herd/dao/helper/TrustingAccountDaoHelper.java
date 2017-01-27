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
package org.finra.herd.dao.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.TrustingAccountDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.TrustingAccountEntity;

/**
 * Helper for trusting account related operations which require DAO.
 */
@Component
public class TrustingAccountDaoHelper
{
    @Autowired
    private TrustingAccountDao trustingAccountDao;

    /**
     * Gets an AWS trusting account by AWS account number and ensure it exists.
     *
     * @param accountId the AWS account number, without dashes (case-sensitive)
     *
     * @return the trusting account entity
     * @throws org.finra.herd.model.ObjectNotFoundException if the trusting account entity doesn't exist
     */
    public TrustingAccountEntity getTrustingAccountEntity(String accountId) throws ObjectNotFoundException
    {
        TrustingAccountEntity trustingAccountEntity = trustingAccountDao.getTrustingAccountById(accountId);

        if (trustingAccountEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Trusting AWS account with id \"%s\" doesn't exist.", accountId));
        }

        return trustingAccountEntity;
    }
}
