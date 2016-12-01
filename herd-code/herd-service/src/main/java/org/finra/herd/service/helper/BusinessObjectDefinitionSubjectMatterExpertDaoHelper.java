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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.BusinessObjectDefinitionSubjectMatterExpertDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionSubjectMatterExpertEntity;

/**
 * Helper for business object definition subject matter expert related operations which require DAO.
 */
@Component
public class BusinessObjectDefinitionSubjectMatterExpertDaoHelper
{
    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private BusinessObjectDefinitionSubjectMatterExpertDao businessObjectDefinitionSubjectMatterExpertDao;

    /**
     * Gets a business object definition subject matter expert entity on the key and makes sure that it exists.
     *
     * @param key the business object definition subject matter expert key
     *
     * @return the business object definition subject matter expert entity
     */
    public BusinessObjectDefinitionSubjectMatterExpertEntity getBusinessObjectDefinitionSubjectMatterExpertEntity(
        BusinessObjectDefinitionSubjectMatterExpertKey key)
    {
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key);

        if (businessObjectDefinitionSubjectMatterExpertEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Subject matter expert with user id \"%s\" does not exist for business object definition {%s}.", key.getUserId(),
                    businessObjectDefinitionHelper
                        .businessObjectDefinitionKeyToString(new BusinessObjectDefinitionKey(key.getNamespace(), key.getBusinessObjectDefinitionName()))));
        }

        return businessObjectDefinitionSubjectMatterExpertEntity;
    }
}
