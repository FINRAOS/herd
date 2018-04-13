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

/**
 * The relational table registration service.
 */
public interface RelationalTableRegistrationService
{
    /**
     * Creates a new relational table registration. The relation table registration includes creation of the following entities: <ul> <li>a business object
     * definition</li> <li>a business object format with schema extracted from the specified relational table in the specified storage of RELATIONAL storage
     * platform type</li> <li>a business object data</li> <li>a storage unit that links together the business object data with the storage</li> </ul>
     *
     * @param relationalTableRegistrationCreateRequest the relational table registration create request
     * @param appendToExistingBusinessObjectDefinition boolean flag that determines if the format should be appended to an existing business object definition
     *
     * @return the information for the newly created business object data
     */
    BusinessObjectData createRelationalTableRegistration(RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest,
        Boolean appendToExistingBusinessObjectDefinition);

    /**
     * Returns latest versions of all relational tables registered in the system.
     *
     * @return the list of relational table registrations
     */
    List<BusinessObjectDataStorageUnitKey> getRelationalTableRegistrationsForSchemaUpdate();

    /**
     * Updates relational table schema, if changes are detected, for an already existing relational table registration. The relation table schema update
     * includes creation of the following entities: <ul> <li>a new version of the business object format with updated schema as extracted from the specified
     * relational table in the specified storage of RELATIONAL storage platform type</li> <li>a business object data that is associated with the new business
     * object format version</li> <li>a storage unit that links together the newly created business object data with the storage</li> </ul>
     *
     * @param storageUnitKey the storage unit key for relational table registration
     *
     * @return the information for the newly created business object data, if schema was updated; null otherwise
     */
    BusinessObjectData processRelationalTableRegistrationForSchemaUpdate(BusinessObjectDataStorageUnitKey storageUnitKey);
}
