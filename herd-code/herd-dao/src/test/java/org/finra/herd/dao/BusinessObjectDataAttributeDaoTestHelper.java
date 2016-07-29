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

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;

@Component
public class BusinessObjectDataAttributeDaoTestHelper
{
    @Autowired
    private BusinessObjectDataAttributeDao businessObjectDataAttributeDao;

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataDaoTestHelper businessObjectDataDaoTestHelper;

    /**
     * Creates and persists a new business object data attribute entity.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param businessObjectDataPartitionValue the business object data primary partition value
     * @param businessObjectDataSubPartitionValues the list of business object data sub-partition values
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param businessObjectDataAttributeValue the business object data attribute value
     *
     * @return the newly created business object data attribute entity.
     */
    public BusinessObjectDataAttributeEntity createBusinessObjectDataAttributeEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String businessObjectDataPartitionValue,
        List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion, String businessObjectDataAttributeName,
        String businessObjectDataAttributeValue)
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues, businessObjectDataVersion);

        return createBusinessObjectDataAttributeEntity(businessObjectDataKey, businessObjectDataAttributeName, businessObjectDataAttributeValue);
    }

    /**
     * Creates and persists a new business object data attribute entity.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param businessObjectDataAttributeValue the business object data attribute value
     *
     * @return the newly created business object data attribute entity.
     */
    public BusinessObjectDataAttributeEntity createBusinessObjectDataAttributeEntity(BusinessObjectDataKey businessObjectDataKey,
        String businessObjectDataAttributeName, String businessObjectDataAttributeValue)
    {
        // Create a business object data entity if it does not exist.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);
        if (businessObjectDataEntity == null)
        {
            businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(businessObjectDataKey, AbstractDaoTest.LATEST_VERSION_FLAG_SET, AbstractDaoTest.BDATA_STATUS);
        }

        return createBusinessObjectDataAttributeEntity(businessObjectDataEntity, businessObjectDataAttributeName, businessObjectDataAttributeValue);
    }

    /**
     * Creates and persists a new business object data attribute entity.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param businessObjectDataAttributeValue the business object data attribute value
     *
     * @return the newly created business object data attribute entity.
     */
    public BusinessObjectDataAttributeEntity createBusinessObjectDataAttributeEntity(BusinessObjectDataEntity businessObjectDataEntity,
        String businessObjectDataAttributeName, String businessObjectDataAttributeValue)
    {
        // Create a new business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity.setBusinessObjectData(businessObjectDataEntity);
        businessObjectDataAttributeEntity.setName(businessObjectDataAttributeName);
        businessObjectDataAttributeEntity.setValue(businessObjectDataAttributeValue);

        // Update the parent entity.
        businessObjectDataEntity.getAttributes().add(businessObjectDataAttributeEntity);
        businessObjectDataAttributeDao.saveAndRefresh(businessObjectDataEntity);

        return businessObjectDataAttributeEntity;
    }
}
