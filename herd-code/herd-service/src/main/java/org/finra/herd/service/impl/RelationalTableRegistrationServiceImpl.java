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

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.RelationalStorageAttributesDto;
import org.finra.herd.service.RelationalTableRegistrationHelperService;
import org.finra.herd.service.RelationalTableRegistrationService;

/**
 * An implementation of the relational table registration service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class RelationalTableRegistrationServiceImpl implements RelationalTableRegistrationService
{
    @Autowired
    private RelationalTableRegistrationHelperService relationalTableRegistrationHelperService;

    @PublishNotificationMessages
    @NamespacePermission(fields = "#relationalTableRegistrationCreateRequest.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public BusinessObjectData createRelationalTableRegistration(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest)
    {
        return createRelationalTableRegistrationImpl(relationalTableRegistrationCreateRequest);
    }

    /**
     * Creates a new relational table registration.
     *
     * @param relationalTableRegistrationCreateRequest the relational table registration create request
     *
     * @return the information for the newly created business object data
     */
    BusinessObjectData createRelationalTableRegistrationImpl(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest)
    {
        // Validate the relational table registration create request.
        relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(relationalTableRegistrationCreateRequest);

        // Get storage attributes required to perform relation table registration.
        // This method also validates database entities per specified relational table registration create request.
        RelationalStorageAttributesDto relationalStorageAttributesDto =
            relationalTableRegistrationHelperService.getRelationalStorageAttributes(relationalTableRegistrationCreateRequest);

        // Retrieve a list of actual schema columns for the specified relational table.
        // This method uses actual JDBC connection to retrieve a description of table columns.
        List<SchemaColumn> schemaColumns = relationalTableRegistrationHelperService
            .retrieveRelationalTableColumns(relationalStorageAttributesDto, relationalTableRegistrationCreateRequest.getRelationalSchemaName(),
                relationalTableRegistrationCreateRequest.getRelationalTableName());

        // Create a new relational table registration and return the information for the newly created business object data.
        return relationalTableRegistrationHelperService.registerRelationalTable(relationalTableRegistrationCreateRequest, schemaColumns);
    }
}
