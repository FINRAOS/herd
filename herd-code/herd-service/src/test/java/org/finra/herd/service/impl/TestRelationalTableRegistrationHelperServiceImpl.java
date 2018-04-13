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

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.RelationalStorageAttributesDto;
import org.finra.herd.model.dto.RelationalTableRegistrationDto;

@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
@Primary
public class TestRelationalTableRegistrationHelperServiceImpl extends RelationalTableRegistrationHelperServiceImpl
{
    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public RelationalStorageAttributesDto prepareForRelationalTableRegistration(
        RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest, Boolean appendToExistingBusinessObjectDefinition)
    {
        return prepareForRelationalTableRegistrationImpl(relationalTableRegistrationCreateRequest, appendToExistingBusinessObjectDefinition);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public RelationalTableRegistrationDto prepareForRelationalTableSchemaUpdate(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        return prepareForRelationalTableSchemaUpdateImpl(storageUnitKey);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectData registerRelationalTable(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest,
        List<SchemaColumn> schemaColumns, Boolean appendToExistingBusinessObjectDefinition)
    {
        return registerRelationalTableImpl(relationalTableRegistrationCreateRequest, schemaColumns, appendToExistingBusinessObjectDefinition);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public List<SchemaColumn> retrieveRelationalTableColumns(RelationalStorageAttributesDto relationalStorageAttributesDto, String relationalSchemaName,
        String relationalTableName)
    {
        return retrieveRelationalTableColumnsImpl(relationalStorageAttributesDto, relationalSchemaName, relationalTableName);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectData updateRelationalTableSchema(RelationalTableRegistrationDto relationalTableRegistrationDto, List<SchemaColumn> schemaColumns)
    {
        return updateRelationalTableSchemaImpl(relationalTableRegistrationDto, schemaColumns);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public void validateAndTrimRelationalTableRegistrationCreateRequest(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest)
    {
        validateAndTrimRelationalTableRegistrationCreateRequestImpl(relationalTableRegistrationCreateRequest);
    }
}
