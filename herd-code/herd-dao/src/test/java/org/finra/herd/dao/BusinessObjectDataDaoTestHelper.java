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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusHistoryEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;

@Component
public class BusinessObjectDataDaoTestHelper
{
    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataStatusDao businessObjectDataStatusDao;

    @Autowired
    private BusinessObjectDataStatusDaoTestHelper businessObjectDataStatusDaoTestHelper;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatDaoTestHelper businessObjectFormatDaoTestHelper;

    /**
     * Removes time portion of a given {@link XMLGregorianCalendar} instance.
     *
     * @param xmlGregorianCalendar the given {@link XMLGregorianCalendar} instant, not null
     * @return a new {@link XMLGregorianCalendar} instance with the time portion removed
     */
    public static XMLGregorianCalendar resetToMidnight(final XMLGregorianCalendar xmlGregorianCalendar) throws DatatypeConfigurationException
    {
        XMLGregorianCalendar gregorianCalendarResetToMidnight =
            DatatypeFactory.newInstance().newXMLGregorianCalendar(xmlGregorianCalendar.toGregorianCalendar());

        // reset time fields
        gregorianCalendarResetToMidnight.setHour(0);
        gregorianCalendarResetToMidnight.setMinute(0);
        gregorianCalendarResetToMidnight.setSecond(0);
        gregorianCalendarResetToMidnight.setMillisecond(0);

        return gregorianCalendarResetToMidnight;
    }

    /**
     * Resets business object data entity "created on" field value back by the specified number of days.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param offsetInDays the number of days to reset the business object data "created on" field value
     */
    public void ageBusinessObjectData(BusinessObjectDataEntity businessObjectDataEntity, long offsetInDays)
    {
        // Apply the offset in days to business object data "created on" value.
        businessObjectDataEntity
            .setCreatedOn(new Timestamp(businessObjectDataEntity.getCreatedOn().getTime() - offsetInDays * 86400000L));    // 24L * 60L * 60L * 1000L
        businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String businessObjectDataPartitionValue, Integer businessObjectDataVersion,
        Boolean businessObjectDataLatestVersion, String businessObjectDataStatusCode)
    {
        return createBusinessObjectDataEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
            businessObjectFormatVersion, businessObjectDataPartitionValue, AbstractDaoTest.NO_SUBPARTITION_VALUES, businessObjectDataVersion,
            businessObjectDataLatestVersion, businessObjectDataStatusCode);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectDataKey businessObjectDataKey, Boolean businessObjectDataLatestVersion,
        String businessObjectDataStatusCode)
    {
        return createBusinessObjectDataEntity(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues(),
            businessObjectDataKey.getBusinessObjectDataVersion(), businessObjectDataLatestVersion, businessObjectDataStatusCode);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String businessObjectDataPartitionValue,
        List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion, Boolean businessObjectDataLatestVersion,
        String businessObjectDataStatusCode, List<BusinessObjectDataEntity> businessObjectDataParents)
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
                    AbstractDaoTest.FORMAT_DOCUMENT_SCHEMA_URL, true, AbstractDaoTest.PARTITION_KEY);
        }

        // Create a business object data status entity if it does not exist.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(businessObjectDataStatusCode);
        if (businessObjectDataStatusEntity == null)
        {
            businessObjectDataStatusEntity = businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(businessObjectDataStatusCode);
        }

        return createBusinessObjectDataEntity(businessObjectFormatEntity, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues,
            businessObjectDataVersion, businessObjectDataLatestVersion, businessObjectDataStatusEntity, businessObjectDataParents);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String businessObjectDataPartitionValue,
        List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion, Boolean businessObjectDataLatestVersion,
        String businessObjectDataStatusCode)
    {
        return createBusinessObjectDataEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
            businessObjectFormatVersion, AbstractDaoTest.LATEST_VERSION_FLAG_SET, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues,
            businessObjectDataVersion, businessObjectDataLatestVersion, businessObjectDataStatusCode);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, Boolean businessObjectFormatLatestVersion,
        String businessObjectDataPartitionValue, List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion,
        Boolean businessObjectDataLatestVersion, String businessObjectDataStatusCode)
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
                    AbstractDaoTest.FORMAT_DOCUMENT_SCHEMA_URL, businessObjectFormatLatestVersion, AbstractDaoTest.PARTITION_KEY);
        }

        // Create a business object data status entity if it does not exist.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(businessObjectDataStatusCode);
        if (businessObjectDataStatusEntity == null)
        {
            businessObjectDataStatusEntity = businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(businessObjectDataStatusCode);
        }

        return createBusinessObjectDataEntity(businessObjectFormatEntity, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues,
            businessObjectDataVersion, businessObjectDataLatestVersion, businessObjectDataStatusEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectFormatEntity businessObjectFormatEntity,
        String businessObjectDataPartitionValue, Integer businessObjectDataVersion, Boolean businessObjectDataLatestVersion,
        String businessObjectDataStatusCode)
    {
        // Create a business object data status entity if it does not exist.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(businessObjectDataStatusCode);
        if (businessObjectDataStatusEntity == null)
        {
            businessObjectDataStatusEntity = businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(businessObjectDataStatusCode);
        }

        return createBusinessObjectDataEntity(businessObjectFormatEntity, businessObjectDataPartitionValue, AbstractDaoTest.NO_SUBPARTITION_VALUES,
            businessObjectDataVersion, businessObjectDataLatestVersion, businessObjectDataStatusEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectFormatEntity businessObjectFormatEntity,
        String businessObjectDataPartitionValue, Integer businessObjectDataVersion, Boolean businessObjectDataLatestVersion,
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity)
    {
        return createBusinessObjectDataEntity(businessObjectFormatEntity, businessObjectDataPartitionValue, AbstractDaoTest.NO_SUBPARTITION_VALUES,
            businessObjectDataVersion, businessObjectDataLatestVersion, businessObjectDataStatusEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectFormatEntity businessObjectFormatEntity,
        String businessObjectDataPartitionValue, List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion,
        Boolean businessObjectDataLatestVersion, String businessObjectDataStatusCode)
    {
        // Create a business object data status entity if it does not exist.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(businessObjectDataStatusCode);
        if (businessObjectDataStatusEntity == null)
        {
            businessObjectDataStatusEntity = businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(businessObjectDataStatusCode);
        }

        return createBusinessObjectDataEntity(businessObjectFormatEntity, businessObjectDataPartitionValue, businessObjectDataSubPartitionValues,
            businessObjectDataVersion, businessObjectDataLatestVersion, businessObjectDataStatusEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectFormatEntity businessObjectFormatEntity,
        String businessObjectDataPartitionValue, List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion,
        Boolean businessObjectDataLatestVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity)
    {
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setVersion(businessObjectDataVersion);
        businessObjectDataEntity.setPartitionValue(businessObjectDataPartitionValue);
        if (businessObjectDataSubPartitionValues != null)
        {
            businessObjectDataEntity.setPartitionValue2(businessObjectDataSubPartitionValues.size() > 0 ? businessObjectDataSubPartitionValues.get(0) : null);
            businessObjectDataEntity.setPartitionValue3(businessObjectDataSubPartitionValues.size() > 1 ? businessObjectDataSubPartitionValues.get(1) : null);
            businessObjectDataEntity.setPartitionValue4(businessObjectDataSubPartitionValues.size() > 2 ? businessObjectDataSubPartitionValues.get(2) : null);
            businessObjectDataEntity.setPartitionValue5(businessObjectDataSubPartitionValues.size() > 3 ? businessObjectDataSubPartitionValues.get(3) : null);
        }
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setLatestVersion(businessObjectDataLatestVersion);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

        // Add an entry to the business object data status history table.
        BusinessObjectDataStatusHistoryEntity businessObjectDataStatusHistoryEntity = new BusinessObjectDataStatusHistoryEntity();
        businessObjectDataStatusHistoryEntity.setBusinessObjectData(businessObjectDataEntity);
        businessObjectDataStatusHistoryEntity.setStatus(businessObjectDataStatusEntity);
        List<BusinessObjectDataStatusHistoryEntity> businessObjectDataStatusHistoryEntities = new ArrayList<>();
        businessObjectDataStatusHistoryEntities.add(businessObjectDataStatusHistoryEntity);
        businessObjectDataEntity.setHistoricalStatuses(businessObjectDataStatusHistoryEntities);

        return businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectFormatEntity businessObjectFormatEntity,
        String businessObjectDataPartitionValue, List<String> businessObjectDataSubPartitionValues, Integer businessObjectDataVersion,
        Boolean businessObjectDataLatestVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity,
        List<BusinessObjectDataEntity> businessObjectDataParents)
    {
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setVersion(businessObjectDataVersion);
        businessObjectDataEntity.setPartitionValue(businessObjectDataPartitionValue);
        if (businessObjectDataSubPartitionValues != null)
        {
            businessObjectDataEntity.setPartitionValue2(businessObjectDataSubPartitionValues.size() > 0 ? businessObjectDataSubPartitionValues.get(0) : null);
            businessObjectDataEntity.setPartitionValue3(businessObjectDataSubPartitionValues.size() > 1 ? businessObjectDataSubPartitionValues.get(1) : null);
            businessObjectDataEntity.setPartitionValue4(businessObjectDataSubPartitionValues.size() > 2 ? businessObjectDataSubPartitionValues.get(2) : null);
            businessObjectDataEntity.setPartitionValue5(businessObjectDataSubPartitionValues.size() > 3 ? businessObjectDataSubPartitionValues.get(3) : null);
        }
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setLatestVersion(businessObjectDataLatestVersion);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        businessObjectDataEntity.setBusinessObjectDataParents(businessObjectDataParents);

        // Add an entry to the business object data status history table.
        BusinessObjectDataStatusHistoryEntity businessObjectDataStatusHistoryEntity = new BusinessObjectDataStatusHistoryEntity();
        businessObjectDataStatusHistoryEntity.setBusinessObjectData(businessObjectDataEntity);
        businessObjectDataStatusHistoryEntity.setStatus(businessObjectDataStatusEntity);
        List<BusinessObjectDataStatusHistoryEntity> businessObjectDataStatusHistoryEntities = new ArrayList<>();
        businessObjectDataStatusHistoryEntities.add(businessObjectDataStatusHistoryEntity);
        businessObjectDataEntity.setHistoricalStatuses(businessObjectDataStatusHistoryEntities);

        return businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);
    }

    /**
     * Creates and persists a new business object data entity.
     *
     * @return the newly created business object data entity.
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntity()
    {
        return createBusinessObjectDataEntity(businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(false),
            new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK).format(System.currentTimeMillis()), AbstractDaoTest.SUBPARTITION_VALUES,
            AbstractDaoTest.INITIAL_DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID);
    }

    /**
     * Create a list of business object data search results per specified list of business object data entities.
     *
     * @param businessObjectDataEntities the list of business object data entities
     *
     * @return the list of business object data search results per specified list of business object data entities
     */
    public List<BusinessObjectData> getExpectedSearchResults(List<BusinessObjectDataEntity> businessObjectDataEntities)
    {
        List<BusinessObjectData> searchResults = new ArrayList<>();

        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            // Create a business object data information that is expected to be returned by the search endpoint for this business object data entity.
            BusinessObjectData businessObjectData = new BusinessObjectData();
            businessObjectData.setId(businessObjectDataEntity.getId());
            businessObjectData.setNamespace(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode());
            businessObjectData.setBusinessObjectDefinitionName(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName());
            businessObjectData.setBusinessObjectFormatUsage(businessObjectDataEntity.getBusinessObjectFormat().getUsage());
            businessObjectData.setBusinessObjectFormatFileType(businessObjectDataEntity.getBusinessObjectFormat().getFileTypeCode());
            businessObjectData.setBusinessObjectFormatVersion(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
            businessObjectData.setPartitionKey(businessObjectDataEntity.getBusinessObjectFormat().getPartitionKey());
            businessObjectData.setPartitionValue(businessObjectDataEntity.getPartitionValue());
            businessObjectData.setSubPartitionValues(getSubPartitionValues(businessObjectDataEntity));
            businessObjectData.setVersion(businessObjectDataEntity.getVersion());
            businessObjectData.setLatestVersion(businessObjectDataEntity.getLatestVersion());
            businessObjectData.setStatus(businessObjectDataEntity.getStatusCode());

            // Add business object data information to the list.
            searchResults.add(businessObjectData);
        }

        return searchResults;
    }

    /**
     * Updates business object status code
     *
     * @param businessObjectDataEntity business object data entity
     * @param businessObjectDataStatusCode business object status code
     *
     * @return updated business object data entity
     */
    public BusinessObjectDataEntity updateBusinessObjectDataEntityStatus(BusinessObjectDataEntity businessObjectDataEntity,
        String businessObjectDataStatusCode)
    {
        // Create a business object data status entity if it does not exist.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(businessObjectDataStatusCode);
        if (businessObjectDataStatusEntity == null)
        {
            businessObjectDataStatusEntity = businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(businessObjectDataStatusCode);
        }

        BusinessObjectDataStatusHistoryEntity businessObjectDataStatusHistoryEntity = new BusinessObjectDataStatusHistoryEntity();
        businessObjectDataStatusHistoryEntity.setBusinessObjectData(businessObjectDataEntity);
        businessObjectDataStatusHistoryEntity.setStatus(businessObjectDataStatusEntity);

        businessObjectDataEntity.getHistoricalStatuses().add(businessObjectDataStatusHistoryEntity);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        return businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);
    }

    /**
     * Gets the sub-partition values for the specified business object data entity.
     *
     * @param businessObjectDataEntity the business object data entity.
     *
     * @return the list of sub-partition values.
     */
    private List<String> getSubPartitionValues(BusinessObjectDataEntity businessObjectDataEntity)
    {
        List<String> subPartitionValues = new ArrayList<>();

        List<String> rawSubPartitionValues = new ArrayList<>();
        rawSubPartitionValues.add(businessObjectDataEntity.getPartitionValue2());
        rawSubPartitionValues.add(businessObjectDataEntity.getPartitionValue3());
        rawSubPartitionValues.add(businessObjectDataEntity.getPartitionValue4());
        rawSubPartitionValues.add(businessObjectDataEntity.getPartitionValue5());

        for (String rawSubPartitionValue : rawSubPartitionValues)
        {
            if (rawSubPartitionValue != null)
            {
                subPartitionValues.add(rawSubPartitionValue);
            }
            else
            {
                break;
            }
        }

        return subPartitionValues;
    }
}
