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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpert;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKeys;

/**
 * The business object definition subject matter expert service.
 */
public interface BusinessObjectDefinitionSubjectMatterExpertService
{
    /**
     * Creates a new business object definition subject matter expert.
     *
     * @param request the information needed to create a business object definition subject matter expert
     *
     * @return the newly created business object definition subject matter expert
     */
    public BusinessObjectDefinitionSubjectMatterExpert createBusinessObjectDefinitionSubjectMatterExpert(
        BusinessObjectDefinitionSubjectMatterExpertCreateRequest request);

    /**
     * Deletes an existing business object definition subject matter expert by key.
     *
     * @param key the business object definition subject matter expert key
     *
     * @return the business object definition subject matter expert that got deleted
     */
    public BusinessObjectDefinitionSubjectMatterExpert deleteBusinessObjectDefinitionSubjectMatterExpert(BusinessObjectDefinitionSubjectMatterExpertKey key);

    /**
     * Gets a list of keys for all existing business object definition subject matter experts for the specified business object definition.
     *
     * @param businessObjectDefinitionKey the key of the business object definition
     *
     * @return the list of business object definition subject matter expert keys
     */
    public BusinessObjectDefinitionSubjectMatterExpertKeys getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(
        BusinessObjectDefinitionKey businessObjectDefinitionKey);
}
