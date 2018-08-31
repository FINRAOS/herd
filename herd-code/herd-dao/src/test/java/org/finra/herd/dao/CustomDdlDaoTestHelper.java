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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;

@Component
public class CustomDdlDaoTestHelper
{
    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatDaoTestHelper businessObjectFormatDaoTestHelper;

    @Autowired
    private CustomDdlDao customDdlDao;

    /**
     * Creates and persists a new custom DDL entity.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param customDdlName the custom DDL name
     * @param ddl the custom DDL context
     *
     * @return the newly created custom DDL entity
     */
    public CustomDdlEntity createCustomDdlEntity(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String customDdlName, String ddl)
    {
        // Create a business object format entity if it does not exist.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion));
        if (businessObjectFormatEntity == null)
        {
            businessObjectFormatEntity = businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                    businessObjectFormatVersion, AbstractDaoTest.FORMAT_DESCRIPTION, AbstractDaoTest.FORMAT_DOCUMENT_SCHEMA,
                    AbstractDaoTest.LATEST_VERSION_FLAG_SET, AbstractDaoTest.PARTITION_KEY);
        }

        return createCustomDdlEntity(businessObjectFormatEntity, customDdlName, ddl);
    }

    /**
     * Creates and persists a new custom DDL entity.
     *
     * @return the newly created custom DDL entity
     */
    public CustomDdlEntity createCustomDdlEntity(BusinessObjectFormatEntity businessObjectFormatEntity, String customDdlName, String ddl)
    {
        CustomDdlEntity customDdlEntity = new CustomDdlEntity();

        customDdlEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        customDdlEntity.setCustomDdlName(customDdlName);
        customDdlEntity.setDdl(ddl);

        return customDdlDao.saveAndRefresh(customDdlEntity);
    }
}
