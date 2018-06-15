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

import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;

public interface BusinessObjectDefinitionDescriptionSuggestionStatusDao extends BaseJpaDao
{
    /**
     * Gets a business object definition description suggestion status by its code.
     *
     * @param code the business object definition description suggestion status code (case-insensitive)
     *
     * @return the business object definition description suggestion status for the specified code
     */
    BusinessObjectDefinitionDescriptionSuggestionStatusEntity getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(String code);
}
