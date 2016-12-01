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

import java.util.Set;

import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptiveInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionUpdateRequest;
import org.finra.herd.model.dto.BusinessObjectDefinitionSampleFileUpdateDto;

/**
 * The business object definition service.
 */
public interface BusinessObjectDefinitionService
{
    public BusinessObjectDefinition createBusinessObjectDefinition(BusinessObjectDefinitionCreateRequest businessObjectDefinitionCreateRequest);

    public BusinessObjectDefinition updateBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionUpdateRequest businessObjectDefinitionUpdateRequest);

    public BusinessObjectDefinition updateBusinessObjectDefinitionDescriptiveInformation(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionDescriptiveInformationUpdateRequest businessObjectDefinitionDescriptiveInformationUpdateRequest);

    public BusinessObjectDefinition getBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey);

    public BusinessObjectDefinition deleteBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey);

    public BusinessObjectDefinitionKeys getBusinessObjectDefinitions();

    public BusinessObjectDefinitionKeys getBusinessObjectDefinitions(String namespaceCode);

    public BusinessObjectDefinitionSearchResponse searchBusinessObjectDefinitions(BusinessObjectDefinitionSearchRequest businessObjectDefinitionSearchRequest,
        Set<String> fields);
    
    public void updatedBusinessObjectDefinitionEntitySampleFile(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionSampleFileUpdateDto businessObjectDefinitionSampleFileUpdateDto);
}
