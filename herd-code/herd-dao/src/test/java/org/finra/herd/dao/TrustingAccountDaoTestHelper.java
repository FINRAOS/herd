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
package org.finra.herd.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.jpa.TrustingAccountEntity;

@Component
public class TrustingAccountDaoTestHelper
{
    @Autowired
    private TrustingAccountDao trustingAccountDao;

    /**
     * Creates and persists a new trusting account entity.
     *
     * @param accountId the trusting account number
     * @param roleArn the full ARN of the role for the cross-account access
     *
     * @return the newly created trusting account entity
     */
    public TrustingAccountEntity createTrustingAccountEntity(String accountId, String roleArn)
    {
        TrustingAccountEntity trustingAccountEntity = new TrustingAccountEntity();
        trustingAccountEntity.setId(accountId);
        trustingAccountEntity.setRoleArn(roleArn);
        return trustingAccountDao.saveAndRefresh(trustingAccountEntity);
    }
}
