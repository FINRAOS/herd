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
package org.finra.herd.service;

import java.util.List;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.RelationalStorageAttributesDto;
import org.finra.herd.model.dto.RelationalTableRegistrationDto;

/**
 * The helper service class for the relational table registration service.
 */
public interface RelationalTableRegistrationHelperService
{
    /**
     * Prepares for relational table registration by validating database entities per specified relational table registration create request. This method
     * returns storage attributes required to perform relation table registration.
     *
     * @param relationalTableRegistrationCreateRequest the relational table registration create request
     * @param appendToExistingBusinessObjectDefinition boolean flag that determines if the format should be appended to an existing business object definition
     *
     * @return the relational storage attributes DTO
     */
    RelationalStorageAttributesDto prepareForRelationalTableRegistration(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest,
        Boolean appendToExistingBusinessObjectDefinition);

    /**
     * Prepares for relational table schema update by validating database entities per specified storage unit key. This method returns a relational table
     * registration DTO which contains attributes required to retrieve the current relation table schema.
     *
     * @param storageUnitKey the storage unit key for the relational table registration
     *
     * @return the relational table registration DTO
     */
    RelationalTableRegistrationDto prepareForRelationalTableSchemaUpdate(BusinessObjectDataStorageUnitKey storageUnitKey);

    /**
     * Creates a new relational table registration. The relation table registration includes creation of the following entities: <ul> <li>a business object
     * definition</li> <li>a business object format with the specified schema columns</li> <li>a business object data</li> <li>a storage unit that links
     * together the business object data with the storage specified in the create request</li> </ul>
     *
     * @param relationalTableRegistrationCreateRequest the relational table registration create request
     * @param appendToExistingBusinessObjectDefinition boolean flag that determines if the format should be appended to an existing business object definition
     *
     * @return the information for the newly created business object data
     */
    BusinessObjectData registerRelationalTable(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest,
        List<SchemaColumn> schemaColumns, Boolean appendToExistingBusinessObjectDefinition);

    /**
     * Retrieves a list of actual schema columns for the specified relational table. This method uses actual JDBC connection to retrieve a description of table
     * columns.
     *
     * @param relationalStorageAttributesDto the relational storage attributes DTO
     * @param relationalSchemaName the name of the relational database schema
     * @param relationalTableName the name of the relational table
     *
     * @return the list of schema columns for the specified relational table
     */
    List<SchemaColumn> retrieveRelationalTableColumns(RelationalStorageAttributesDto relationalStorageAttributesDto, String relationalSchemaName,
        String relationalTableName);

    /**
     * Updates relational table schema for an already existing relational table registration. The relation table schema update includes creation of the
     * following entities: <ul> <li>a new version of the business object format with updated schema as extracted from the specified relational table in the
     * specified storage of RELATIONAL storage platform type</li> <li>a business object data that is associated with the new business object format version</li>
     * <li>a storage unit that links together the newly created business object data with the storage</li> </ul>
     *
     * @param relationalTableRegistrationDto the relational table registration DTO
     * @param schemaColumns the new relational table schema
     *
     * @return the information for the business object data created for the updated relational table registration
     */
    BusinessObjectData updateRelationalTableSchema(RelationalTableRegistrationDto relationalTableRegistrationDto, List<SchemaColumn> schemaColumns);

    /**
     * Validates a relational table registration create request. This method also trims the request parameters.
     *
     * @param relationalTableRegistrationCreateRequest the relational table registration create request
     */
    void validateAndTrimRelationalTableRegistrationCreateRequest(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest);
}
