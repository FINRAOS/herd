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

import org.finra.herd.dao.CustomDdlDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.jpa.CustomDdlEntity;

/**
 * Helper for custom DDL related operations which require DAO.
 */
@Component
public class CustomDdlDaoHelper
{
    @Autowired
    private CustomDdlDao customDdlDao;

    /**
     * Gets a custom DDL entity based on the key and makes sure that it exists.
     *
     * @param customDdlKey the custom DDL key
     *
     * @return the custom DDL entity
     * @throws ObjectNotFoundException if the business object format entity doesn't exist
     */
    public CustomDdlEntity getCustomDdlEntity(CustomDdlKey customDdlKey) throws ObjectNotFoundException
    {
        CustomDdlEntity customDdlEntity = customDdlDao.getCustomDdlByKey(customDdlKey);

        if (customDdlEntity == null)
        {
            throw new ObjectNotFoundException(String.format(
                "Custom DDL with name \"%s\" does not exist for business object format with namespace \"%s\", business object definition name \"%s\", " +
                    "format usage \"%s\", format file type \"%s\", and format version \"%d\".", customDdlKey.getCustomDdlName(), customDdlKey.getNamespace(),
                customDdlKey.getBusinessObjectDefinitionName(), customDdlKey.getBusinessObjectFormatUsage(), customDdlKey.getBusinessObjectFormatFileType(),
                customDdlKey.getBusinessObjectFormatVersion()));
        }

        return customDdlEntity;
    }
}
