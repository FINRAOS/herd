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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusChangeEvent;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.LatestAfterPartitionValue;
import org.finra.herd.model.api.xml.LatestBeforePartitionValue;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.RegistrationDateRangeFilter;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusHistoryEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.service.BusinessObjectDataService;

/**
 * A helper class for BusinessObjectDataService related code.
 */
@Component
public class BusinessObjectDataHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private StorageUnitHelper storageUnitHelper;

    /**
     * Asserts that the status of the given data is equal to the given expected value.
     *
     * @param expectedBusinessObjectDataStatusCode - the expected status
     * @param businessObjectDataEntity - the data entity
     *
     * @throws IllegalArgumentException when status does not equal
     */
    public void assertBusinessObjectDataStatusEquals(String expectedBusinessObjectDataStatusCode, BusinessObjectDataEntity businessObjectDataEntity)
        throws IllegalArgumentException
    {
        String businessObjectDataStatusCode = businessObjectDataEntity.getStatus().getCode();
        Assert.isTrue(expectedBusinessObjectDataStatusCode.equals(businessObjectDataStatusCode), String
            .format("Business object data status \"%s\" does not match the expected status \"%s\" for the business object data {%s}.",
                businessObjectDataStatusCode, expectedBusinessObjectDataStatusCode, businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
    }

    /**
     * Returns a string representation of the alternate key values for the business object data entity.
     *
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the string representation of the alternate key values for the business object data entity
     */
    public String businessObjectDataEntityAltKeyToString(BusinessObjectDataEntity businessObjectDataEntity)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectDataEntity.getBusinessObjectFormat();

        return businessObjectDataKeyToString(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
            businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion(),
            businessObjectDataEntity.getPartitionValue(), getSubPartitionValues(businessObjectDataEntity), businessObjectDataEntity.getVersion());
    }

    /**
     * Returns a string representation of the business object data key.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the string representation of the business object data key
     */
    public String businessObjectDataKeyToString(BusinessObjectDataKey businessObjectDataKey)
    {
        if (businessObjectDataKey == null)
        {
            return null;
        }

        return businessObjectDataKeyToString(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues(),
            businessObjectDataKey.getBusinessObjectDataVersion());
    }

    /**
     * Returns a string representation of the business object data key.
     *
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name.
     * @param businessObjectFormatUsage the business object format usage.
     * @param businessObjectFormatFileType the business object format file type.
     * @param businessObjectFormatVersion the business object formation version.
     * @param businessObjectDataPartitionValue the business object data partition value.
     * @param businessObjectDataSubPartitionValues the business object data subpartition values.
     * @param businessObjectDataVersion the business object data version.
     *
     * @return the string representation of the business object data key.
     */
    public String businessObjectDataKeyToString(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, int businessObjectFormatVersion, String businessObjectDataPartitionValue,
        List<String> businessObjectDataSubPartitionValues, int businessObjectDataVersion)
    {
        return String.format(
            "namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                "businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s\", " +
                "businessObjectDataVersion: %d", namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
            businessObjectFormatVersion, businessObjectDataPartitionValue,
            org.springframework.util.CollectionUtils.isEmpty(businessObjectDataSubPartitionValues) ? "" :
                StringUtils.join(businessObjectDataSubPartitionValues, ","), businessObjectDataVersion);
    }

    /**
     * Validates the upload single initiation request. This method also trims the request parameters.
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param uuid the UUID
     * @param businessObjectDataStatus the status of the business object data
     * @param attributes the list of attributes
     * @param storageEntity the storage entity
     * @param storageDirectoryPath the storage directory path
     * @param storageFilePath the storage file path
     * @param storageFileSizeBytes the storage file size in bytes
     * @param storageFileRowCount the storage file row count
     *
     * @return the business object data create request.
     */
    public BusinessObjectDataCreateRequest createBusinessObjectDataCreateRequest(BusinessObjectFormatEntity businessObjectFormatEntity, String uuid,
        String businessObjectDataStatus, List<Attribute> attributes, StorageEntity storageEntity, String storageDirectoryPath, String storageFilePath,
        Long storageFileSizeBytes, Long storageFileRowCount)
    {
        BusinessObjectDataCreateRequest request = new BusinessObjectDataCreateRequest();

        // Set the values required for the business object data partition key.
        request.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        request.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        request.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        request.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        request.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());
        request.setPartitionKey(businessObjectFormatEntity.getPartitionKey());
        request.setPartitionValue(uuid);

        // Set the business object data status.
        request.setStatus(businessObjectDataStatus);

        // Add a storage unit.
        StorageUnitCreateRequest storageUnitCreateRequest = new StorageUnitCreateRequest();
        request.setStorageUnits(Arrays.asList(storageUnitCreateRequest));
        storageUnitCreateRequest.setStorageName(storageEntity.getName());
        storageUnitCreateRequest.setStorageDirectory(new StorageDirectory(storageDirectoryPath));
        storageUnitCreateRequest.setStorageFiles(Arrays.asList(new StorageFile(storageFilePath, storageFileSizeBytes, storageFileRowCount)));

        // Set the attributes if any are specified.
        request.setAttributes(attributes);

        // Since we are using UUID as our partition value, set the flag to only allow business object data initial version creation.
        request.setCreateNewVersion(false);

        return request;
    }

    /**
     * Creates the business object data from the persisted entity.
     *
     * @param businessObjectDataEntity the newly persisted business object data entity.
     *
     * @return the business object data.
     */
    public BusinessObjectData createBusinessObjectDataFromEntity(BusinessObjectDataEntity businessObjectDataEntity)
    {
        return createBusinessObjectDataFromEntity(businessObjectDataEntity, false, false);
    }

    /**
     * Creates the business object data from the persisted entity.
     *
     * @param businessObjectDataEntity the newly persisted business object data entity.
     * @param includeBusinessObjectDataStatusHistory specifies to include business object data status history in the response
     * @param includeStorageUnitStatusHistory specifies to include storage unit status history for each storage unit in the response
     *
     * @return the business object data.
     */
    public BusinessObjectData createBusinessObjectDataFromEntity(BusinessObjectDataEntity businessObjectDataEntity,
        Boolean includeBusinessObjectDataStatusHistory, Boolean includeStorageUnitStatusHistory)
    {
        // Make the business object format associated with this data easily accessible.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectDataEntity.getBusinessObjectFormat();

        // Create the core business object data information.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectData.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        businessObjectData.setId(businessObjectDataEntity.getId());
        businessObjectData.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        businessObjectData.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        businessObjectData.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());
        businessObjectData.setPartitionKey(businessObjectDataEntity.getBusinessObjectFormat().getPartitionKey());
        businessObjectData.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        businessObjectData.setSubPartitionValues(getSubPartitionValues(businessObjectDataEntity));
        businessObjectData.setStatus(businessObjectDataEntity.getStatus().getCode());
        businessObjectData.setVersion(businessObjectDataEntity.getVersion());
        businessObjectData.setLatestVersion(businessObjectDataEntity.getLatestVersion());

        // Add in the storage units.
        businessObjectData
            .setStorageUnits(storageUnitHelper.createStorageUnitsFromEntities(businessObjectDataEntity.getStorageUnits(), includeStorageUnitStatusHistory));

        // Add in the attributes.
        List<Attribute> attributes = new ArrayList<>();
        businessObjectData.setAttributes(attributes);

        for (BusinessObjectDataAttributeEntity attributeEntity : businessObjectDataEntity.getAttributes())
        {
            Attribute attribute = new Attribute();
            attributes.add(attribute);
            attribute.setName(attributeEntity.getName());
            attribute.setValue(attributeEntity.getValue());
        }

        // Add in the parents - sorted.
        List<BusinessObjectDataKey> businessObjectDataKeys = new ArrayList<>();
        businessObjectData.setBusinessObjectDataParents(businessObjectDataKeys);
        for (BusinessObjectDataEntity parent : businessObjectDataEntity.getBusinessObjectDataParents())
        {
            businessObjectDataKeys.add(createBusinessObjectDataKeyFromEntity(parent));
        }
        Collections.sort(businessObjectDataKeys, new BusinessObjectDataKeyComparator());

        // Add in the children - sorted.
        businessObjectDataKeys = new ArrayList<>();
        businessObjectData.setBusinessObjectDataChildren(businessObjectDataKeys);
        for (BusinessObjectDataEntity parent : businessObjectDataEntity.getBusinessObjectDataChildren())
        {
            businessObjectDataKeys.add(createBusinessObjectDataKeyFromEntity(parent));
        }
        Collections.sort(businessObjectDataKeys, new BusinessObjectDataKeyComparator());

        // If specified, add business object data status history.
        if (BooleanUtils.isTrue(includeBusinessObjectDataStatusHistory))
        {
            List<BusinessObjectDataStatusChangeEvent> businessObjectDataStatusChangeEvents = new ArrayList<>();
            businessObjectData.setBusinessObjectDataStatusHistory(businessObjectDataStatusChangeEvents);
            for (BusinessObjectDataStatusHistoryEntity businessObjectDataStatusHistoryEntity : businessObjectDataEntity.getHistoricalStatuses())
            {
                businessObjectDataStatusChangeEvents.add(new BusinessObjectDataStatusChangeEvent(businessObjectDataStatusHistoryEntity.getStatus().getCode(),
                    HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDataStatusHistoryEntity.getCreatedOn()),
                    businessObjectDataStatusHistoryEntity.getCreatedBy()));
            }
        }

        // If specified, add business object data retention information.
        if (businessObjectDataEntity.getRetentionExpiration() != null)
        {
            businessObjectData.setRetentionExpirationDate(HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDataEntity.getRetentionExpiration()));
        }

        return businessObjectData;
    }

    /**
     * Creates a business object data key from a business object data DTO.
     *
     * @param businessObjectData the business object data DTO.
     *
     * @return the business object data key.
     */
    public BusinessObjectDataKey createBusinessObjectDataKey(BusinessObjectData businessObjectData)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(businessObjectData.getNamespace());
        businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectData.getBusinessObjectDefinitionName());
        businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectData.getBusinessObjectFormatUsage());
        businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectData.getBusinessObjectFormatFileType());
        businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectData.getBusinessObjectFormatVersion());
        businessObjectDataKey.setPartitionValue(businessObjectData.getPartitionValue());
        businessObjectDataKey.setSubPartitionValues(businessObjectData.getSubPartitionValues());
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectData.getVersion());
        return businessObjectDataKey;
    }

    /**
     * Creates a business object data key from a business object data entity.
     *
     * @param businessObjectDataEntity the business object data entity.
     *
     * @return the business object data key.
     */
    public BusinessObjectDataKey createBusinessObjectDataKeyFromEntity(BusinessObjectDataEntity businessObjectDataEntity)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName());
        businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectDataEntity.getBusinessObjectFormat().getUsage());
        businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode());
        businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
        businessObjectDataKey.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        businessObjectDataKey.setSubPartitionValues(getSubPartitionValues(businessObjectDataEntity));
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());
        return businessObjectDataKey;
    }

    /**
     * Creates a business object data key from a storage unit key.
     *
     * @param storageUnitKey the storage unit key
     *
     * @return the business object data key.
     */
    public BusinessObjectDataKey createBusinessObjectDataKeyFromStorageUnitKey(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        return new BusinessObjectDataKey(storageUnitKey.getNamespace(), storageUnitKey.getBusinessObjectDefinitionName(),
            storageUnitKey.getBusinessObjectFormatUsage(), storageUnitKey.getBusinessObjectFormatFileType(), storageUnitKey.getBusinessObjectFormatVersion(),
            storageUnitKey.getPartitionValue(), storageUnitKey.getSubPartitionValues(), storageUnitKey.getBusinessObjectDataVersion());
    }

    /**
     * Returns a business object data key for the business object data entity.
     *
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the business object data key for the business object data entity
     */
    public BusinessObjectDataKey getBusinessObjectDataKey(BusinessObjectDataEntity businessObjectDataEntity)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();

        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectDataEntity.getBusinessObjectFormat();
        businessObjectDataKey.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());
        businessObjectDataKey.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        businessObjectDataKey.setSubPartitionValues(getSubPartitionValues(businessObjectDataEntity));
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());

        return businessObjectDataKey;
    }

    /**
     * Returns a business object data key for the business object data.
     *
     * @param businessObjectData the business object data
     *
     * @return the business object data key for the business object data
     */
    public BusinessObjectDataKey getBusinessObjectDataKey(BusinessObjectData businessObjectData)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();

        businessObjectDataKey.setNamespace(businessObjectData.getNamespace());
        businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectData.getBusinessObjectDefinitionName());
        businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectData.getBusinessObjectFormatUsage());
        businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectData.getBusinessObjectFormatFileType());
        businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectData.getBusinessObjectFormatVersion());
        businessObjectDataKey.setPartitionValue(businessObjectData.getPartitionValue());
        businessObjectDataKey.setSubPartitionValues(businessObjectData.getSubPartitionValues());
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectData.getVersion());

        return businessObjectDataKey;
    }

    /**
     * Gets a date in a date format from a string format or null if one wasn't specified. The format of the date should match
     * HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.
     *
     * @param dateString the date as a string
     *
     * @return the date as a date or null if one wasn't specified or the conversion fails
     */
    public Date getDateFromString(String dateString)
    {
        Date resultDate = null;

        // For strict date parsing, process the date string only if it has the required length.
        if (dateString.length() == AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.length())
        {
            // Try to convert the date string to a Date.
            try
            {
                // Use strict parsing to ensure our date is more definitive.
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK, Locale.US);
                simpleDateFormat.setLenient(false);
                resultDate = simpleDateFormat.parse(dateString);
            }
            catch (ParseException e)
            {
                // This assignment is here to pass PMD checks.
                resultDate = null;
            }
        }

        return resultDate;
    }

    /**
     * Returns a partition filter that the specified business object data key would match to. The filter is build as per specified sample partition filter.
     *
     * @param businessObjectDataKey the business object data key
     * @param samplePartitionFilter the sample partition filter
     *
     * @return the partition filter
     */
    public List<String> getPartitionFilter(BusinessObjectDataKey businessObjectDataKey, List<String> samplePartitionFilter)
    {
        List<String> resultPartitionFilter = new ArrayList<>();

        resultPartitionFilter.add(samplePartitionFilter.get(0) != null ? businessObjectDataKey.getPartitionValue() : null);

        for (int i = 0; i < BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            resultPartitionFilter.add(samplePartitionFilter.get(i + 1) != null ? businessObjectDataKey.getSubPartitionValues().get(i) : null);
        }

        return resultPartitionFilter;
    }

    /**
     * Returns primary partition or subpartition value specified by the partition column position.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param partitionColumnPosition the position of the partition column (one-based numbering)
     *
     * @return the value of the partition identified by the partition column position
     */
    public String getPartitionValue(BusinessObjectDataEntity businessObjectDataEntity, int partitionColumnPosition)
    {
        String partitionValue = null;

        if (partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
        {
            partitionValue = businessObjectDataEntity.getPartitionValue();
        }
        else
        {
            List<String> subPartitionValues = getSubPartitionValues(businessObjectDataEntity);

            if (partitionColumnPosition > BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION && partitionColumnPosition < subPartitionValues.size() + 2)
            {
                // Please note that the value of the second partition column is located at index 0.
                partitionValue = subPartitionValues.get(partitionColumnPosition - 2);
            }
        }

        return partitionValue;
    }

    /**
     * Returns a list of primary and sub-partition values per specified business object data.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the list of primary and sub-partition values
     */
    public List<String> getPrimaryAndSubPartitionValues(BusinessObjectDataKey businessObjectDataKey)
    {
        List<String> partitionValues = new ArrayList<>();
        partitionValues.add(businessObjectDataKey.getPartitionValue());
        partitionValues.addAll(businessObjectDataKey.getSubPartitionValues());
        return partitionValues;
    }

    /**
     * Returns a list of primary and sub-partition values per specified business object data.
     *
     * @param businessObjectData the business object data
     *
     * @return the list of primary and sub-partition values
     */
    public List<String> getPrimaryAndSubPartitionValues(BusinessObjectData businessObjectData)
    {
        return getPrimaryAndSubPartitionValues(getBusinessObjectDataKey(businessObjectData));
    }

    /**
     * Returns a list of primary and sub-partition values per specified business object data entity.
     *
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the list of primary and sub-partition values
     */
    public List<String> getPrimaryAndSubPartitionValues(BusinessObjectDataEntity businessObjectDataEntity)
    {
        return getPrimaryAndSubPartitionValues(getBusinessObjectDataKey(businessObjectDataEntity));
    }

    /**
     * Gets a storage unit by storage name (case insensitive).
     *
     * @param businessObjectData the business object data
     * @param storageName the storage name
     *
     * @return the storage unit
     * @throws IllegalStateException if business object data has no storage unit with the specified storage name
     */
    public StorageUnit getStorageUnitByStorageName(BusinessObjectData businessObjectData, String storageName) throws IllegalStateException
    {
        StorageUnit resultStorageUnit = null;

        // Find a storage unit that belongs to the specified storage.
        for (StorageUnit storageUnit : businessObjectData.getStorageUnits())
        {
            if (storageUnit.getStorage().getName().equalsIgnoreCase(storageName))
            {
                resultStorageUnit = storageUnit;
                break;
            }
        }

        // Validate that we found a storage unit that belongs to the specified storage.
        if (resultStorageUnit == null)
        {
            throw new IllegalStateException(String.format("Business object data has no storage unit with storage name \"%s\".", storageName));
        }

        return resultStorageUnit;
    }

    /**
     * Gets the sub-partition values for the specified business object data entity.
     *
     * @param businessObjectDataEntity the business object data entity.
     *
     * @return the list of sub-partition values.
     */
    public List<String> getSubPartitionValues(BusinessObjectDataEntity businessObjectDataEntity)
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

    /**
     * Validates the business object data key. This method also trims the key parameters.
     *
     * @param key the business object data key
     * @param businessObjectFormatVersionRequired specifies if the business object format version is required or not
     * @param businessObjectDataVersionRequired specifies if the business object data version is required or not
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectDataKey(BusinessObjectDataKey key, boolean businessObjectFormatVersionRequired, boolean businessObjectDataVersionRequired)
        throws IllegalArgumentException
    {
        // Validate and remove leading and trailing spaces.
        Assert.notNull(key, "A business object data key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));
        key.setBusinessObjectFormatUsage(alternateKeyHelper.validateStringParameter("business object format usage", key.getBusinessObjectFormatUsage()));
        key.setBusinessObjectFormatFileType(
            alternateKeyHelper.validateStringParameter("business object format file type", key.getBusinessObjectFormatFileType()));
        if (businessObjectFormatVersionRequired)
        {
            Assert.notNull(key.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        }
        key.setPartitionValue(alternateKeyHelper.validateStringParameter("partition value", key.getPartitionValue()));
        validateSubPartitionValues(key.getSubPartitionValues());
        if (businessObjectDataVersionRequired)
        {
            Assert.notNull(key.getBusinessObjectDataVersion(), "A business object data version must be specified.");
        }
    }

    /**
     * Validates a list of partition value filters or a standalone partition filter. This method makes sure that a partition value filter contains exactly one
     * partition value range or a non-empty partition value list. This method also makes sure that there is no more than one partition value range specified
     * across all partition value filters.
     *
     * @param partitionValueFilters the list of partition value filters to validate
     * @param standalonePartitionValueFilter the standalone partition value filter to validate
     * @param allowPartitionValueTokens specifies whether the partition value filter is allowed to contain partition value tokens
     */
    public void validatePartitionValueFilters(List<PartitionValueFilter> partitionValueFilters, PartitionValueFilter standalonePartitionValueFilter,
        boolean allowPartitionValueTokens)
    {
        // Make sure that request does not contain both a list of partition value filters and a standalone partition value filter.
        Assert.isTrue(partitionValueFilters == null || standalonePartitionValueFilter == null,
            "A list of partition value filters and a standalone partition value filter cannot be both specified.");

        List<PartitionValueFilter> partitionValueFiltersToValidate = new ArrayList<>();

        if (partitionValueFilters != null)
        {
            partitionValueFiltersToValidate.addAll(partitionValueFilters);
        }

        if (standalonePartitionValueFilter != null)
        {
            partitionValueFiltersToValidate.add(standalonePartitionValueFilter);
        }

        // Make sure that at least one partition value filter is specified.
        Assert.notEmpty(partitionValueFiltersToValidate, "At least one partition value filter must be specified.");

        // Validate and trim partition value filters.
        int partitionValueRangesCount = 0;
        for (PartitionValueFilter partitionValueFilter : partitionValueFiltersToValidate)
        {
            // Partition key is required when request contains a partition value filter list.
            if (partitionValueFilters != null)
            {
                Assert.hasText(partitionValueFilter.getPartitionKey(), "A partition key must be specified.");
            }

            // Trim partition key value.
            if (StringUtils.isNotBlank(partitionValueFilter.getPartitionKey()))
            {
                partitionValueFilter.setPartitionKey(partitionValueFilter.getPartitionKey().trim());
            }

            PartitionValueRange partitionValueRange = partitionValueFilter.getPartitionValueRange();
            List<String> partitionValues = partitionValueFilter.getPartitionValues();
            LatestBeforePartitionValue latestBeforePartitionValue = partitionValueFilter.getLatestBeforePartitionValue();
            LatestAfterPartitionValue latestAfterPartitionValue = partitionValueFilter.getLatestAfterPartitionValue();

            // Validate that we have exactly one partition filter option specified.
            List<Boolean> partitionFilterOptions =
                Arrays.asList(partitionValueRange != null, partitionValues != null, latestBeforePartitionValue != null, latestAfterPartitionValue != null);
            Assert.isTrue(Collections.frequency(partitionFilterOptions, Boolean.TRUE) == 1, "Exactly one partition value filter option must be specified.");

            if (partitionValueRange != null)
            {
                // A "partition value range" filter option is specified.

                // Only one partition value range is allowed across all partition value filters.
                partitionValueRangesCount++;
                Assert.isTrue(partitionValueRangesCount < 2, "Cannot specify more than one partition value range.");

                // Validate start partition value for the partition value range.
                Assert.hasText(partitionValueRange.getStartPartitionValue(), "A start partition value for the partition value range must be specified.");
                partitionValueRange.setStartPartitionValue(partitionValueRange.getStartPartitionValue().trim());

                // Validate end partition value for the partition value range.
                Assert.hasText(partitionValueRange.getEndPartitionValue(), "An end partition value for the partition value range must be specified.");
                partitionValueRange.setEndPartitionValue(partitionValueRange.getEndPartitionValue().trim());

                // Validate that partition value tokens are not specified as start and end partition values.
                // This check is required, regardless if partition value tokens are allowed or not.
                Assert.isTrue(!partitionValueRange.getStartPartitionValue().equals(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN) &&
                        !partitionValueRange.getStartPartitionValue().equals(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN) &&
                        !partitionValueRange.getEndPartitionValue().equals(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN) &&
                        !partitionValueRange.getEndPartitionValue().equals(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN),
                    "A partition value token cannot be specified with a partition value range.");

                // Using string compare, validate that start partition value is less than or equal to end partition value.
                Assert.isTrue(partitionValueRange.getStartPartitionValue().compareTo(partitionValueRange.getEndPartitionValue()) <= 0, String
                    .format("The start partition value \"%s\" cannot be greater than the end partition value \"%s\".",
                        partitionValueRange.getStartPartitionValue(), partitionValueRange.getEndPartitionValue()));
            }
            else if (partitionValues != null)
            {
                // A "partition value list" filter option is specified.

                // Validate that the list contains at least one partition value.
                Assert.isTrue(!partitionValues.isEmpty(), "At least one partition value must be specified.");

                for (int i = 0; i < partitionValues.size(); i++)
                {
                    String partitionValue = partitionValues.get(i);
                    Assert.hasText(partitionValue, "A partition value must be specified.");
                    partitionValue = partitionValue.trim();

                    // When partition value tokens are not allowed, validate that they are not specified as one of partition values.
                    if (!allowPartitionValueTokens)
                    {
                        Assert.isTrue(!partitionValue.equals(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN) &&
                                !partitionValue.equals(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN),
                            "A partition value token cannot be specified as one of partition values.");
                    }

                    partitionValues.set(i, partitionValue);
                }
            }
            else if (latestBeforePartitionValue != null)
            {
                // A "latest before partition value" filter option is specified.
                Assert.hasText(latestBeforePartitionValue.getPartitionValue(), "A partition value must be specified.");
                latestBeforePartitionValue.setPartitionValue(latestBeforePartitionValue.getPartitionValue().trim());
            }
            else
            {
                // A "latest after partition value" filter option is specified.
                Assert.hasText(latestAfterPartitionValue.getPartitionValue(), "A partition value must be specified.");
                latestAfterPartitionValue.setPartitionValue(latestAfterPartitionValue.getPartitionValue().trim());
            }
        }
    }

    /**
     * Validates a registration date range filter. This method makes sure that a registration date range filter contains start date or end date or both.
     *
     * @param registrationDateRangeFilter the registration date range
     */
    public void validateRegistrationDateRangeFilter(RegistrationDateRangeFilter registrationDateRangeFilter)
    {
        Assert.isTrue(registrationDateRangeFilter.getStartRegistrationDate() != null || registrationDateRangeFilter.getEndRegistrationDate() != null,
            "Either start registration date or end registration date must be specified.");

        if (registrationDateRangeFilter.getStartRegistrationDate() != null && registrationDateRangeFilter.getEndRegistrationDate() != null)
        {
            Assert.isTrue(registrationDateRangeFilter.getStartRegistrationDate().compare(registrationDateRangeFilter.getEndRegistrationDate()) <= 0, String
                .format("The start registration date \"%s\" cannot be greater than the end registration date \"%s\".",
                    registrationDateRangeFilter.getStartRegistrationDate(), registrationDateRangeFilter.getEndRegistrationDate()));
        }
    }

    /**
     * Validates a list of sub-partition values. This method also trims the sub-partition values.
     *
     * @param subPartitionValues the list of sub-partition values
     *
     * @throws IllegalArgumentException if a sub-partition value is missing or not valid
     */
    public void validateSubPartitionValues(List<String> subPartitionValues) throws IllegalArgumentException
    {
        int subPartitionValuesCount = CollectionUtils.size(subPartitionValues);

        Assert.isTrue(subPartitionValuesCount <= BusinessObjectDataEntity.MAX_SUBPARTITIONS,
            String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS));

        for (int i = 0; i < subPartitionValuesCount; i++)
        {
            subPartitionValues.set(i, alternateKeyHelper.validateStringParameter("subpartition value", subPartitionValues.get(i)));
        }
    }
}
