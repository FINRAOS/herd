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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionSubjectMatterExpertEntity;

public interface BusinessObjectDefinitionSubjectMatterExpertDao extends BaseJpaDao
{
    /**
     * Gets a business object definition subject matter expert by business object definition entity and subject matter expert's user id.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param userId the user id of the subject matter expert (case-insensitive)
     *
     * @return the business object definition subject matter expert entity
     */
    public BusinessObjectDefinitionSubjectMatterExpertEntity getBusinessObjectDefinitionSubjectMatterExpert(
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, String userId);

    /**
     * Gets a business object definition subject matter expert by it's key.
     *
     * @param key the key of the business object definition subject matter expert (case-insensitive)
     *
     * @return the business object definition subject matter expert entity
     */
    public BusinessObjectDefinitionSubjectMatterExpertEntity getBusinessObjectDefinitionSubjectMatterExpertByKey(
        BusinessObjectDefinitionSubjectMatterExpertKey key);
}
