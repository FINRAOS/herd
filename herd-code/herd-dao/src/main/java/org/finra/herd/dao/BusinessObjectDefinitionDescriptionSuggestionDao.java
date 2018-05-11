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

import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

public interface BusinessObjectDefinitionDescriptionSuggestionDao extends BaseJpaDao
{
    /**
     * Gets a business object definition description suggestion by business object definition entity and user id.
     *
     * @param businessObjectDefinitionEntity the business object definition entity associated with the description suggestion
     * @param userId the user id associated with the description suggestion
     *
     * @return the business object definition description suggestion for the specified key
     */
    BusinessObjectDefinitionDescriptionSuggestionEntity getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, String userId);
}
