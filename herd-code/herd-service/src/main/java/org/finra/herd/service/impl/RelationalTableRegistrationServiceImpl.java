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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.service.RelationalTableRegistrationService;
import org.finra.herd.service.helper.RelationalTableRegistrationDaoHelper;

/**
 * The relational table registration service implementation
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class RelationalTableRegistrationServiceImpl implements RelationalTableRegistrationService
{

    @Autowired
    RelationalTableRegistrationDaoHelper relationalTableRegistrationDaoHelper;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @NamespacePermission(fields = "#relationalTableRegistrationCreateRequest.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectData createRelationalTableRegistration(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest)
    {
        return relationalTableRegistrationDaoHelper.createRelationalTableRegistration(relationalTableRegistrationCreateRequest);
    }
}
