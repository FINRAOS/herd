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
package org.finra.dm.service.helper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.propertyeditors.CustomBooleanEditor;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.dao.DmDao;
import org.finra.dm.dao.helper.DmStringHelper;
import org.finra.dm.model.MethodNotAllowedException;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKey;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.dm.model.api.xml.BusinessObjectFormatKey;
import org.finra.dm.model.api.xml.CustomDdlKey;
import org.finra.dm.model.api.xml.EmrClusterDefinitionKey;
import org.finra.dm.model.api.xml.PartitionKeyGroupKey;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.dto.StorageAlternateKeyDto;
import org.finra.dm.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.ConfigurationEntity;
import org.finra.dm.model.jpa.CustomDdlEntity;
import org.finra.dm.model.jpa.DataProviderEntity;
import org.finra.dm.model.jpa.EmrClusterDefinitionEntity;
import org.finra.dm.model.jpa.FileTypeEntity;
import org.finra.dm.model.jpa.JmsMessageEntity;
import org.finra.dm.model.jpa.JobDefinitionEntity;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.jpa.NotificationEventTypeEntity;
import org.finra.dm.model.jpa.PartitionKeyGroupEntity;
import org.finra.dm.model.jpa.StorageAttributeEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StorageFileEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;

/**
 * A helper class for DmDao related data management code.
 */
@Component
public class DmDaoHelper
{
    @Autowired
    private DmDao dmDao;

    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private DmStringHelper dmStringHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Gets a namespace entity and ensure it exists.
     *
     * @param namespace the namespace (case insensitive)
     *
     * @return the namespace entity
     * @throws ObjectNotFoundException if the namespace entity doesn't exist
     */
    public NamespaceEntity getNamespaceEntity(String namespace) throws ObjectNotFoundException
    {
        NamespaceEntity namespaceEntity = dmDao.getNamespaceByCd(namespace);

        if (namespaceEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Namespace \"%s\" doesn't exist.", namespace));
        }

        return namespaceEntity;
    }

    /**
     * Gets a data provider entity and ensure it exists.
     *
     * @param dataProviderName the data provider name (case insensitive)
     *
     * @return the data provider entity
     * @throws ObjectNotFoundException if the data provider entity doesn't exist
     */
    public DataProviderEntity getDataProviderEntity(String dataProviderName) throws ObjectNotFoundException
    {
        DataProviderEntity dataProviderEntity = dmDao.getDataProviderByName(dataProviderName);

        if (dataProviderEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Data provider with name \"%s\" doesn't exist.", dataProviderName));
        }

        return dataProviderEntity;
    }

    /**
     * Retrieves a namespace code for a specified legacy business object definition.
     *
     * @param businessObjectDefinitionName the business object definition name (case-insensitive)
     *
     * @return the namespace code for the specified legacy business object definition
     * @throws ObjectNotFoundException if the legacy business object definition entity doesn't exist
     */
    public String getNamespaceCode(String businessObjectDefinitionName) throws ObjectNotFoundException
    {
        return getLegacyBusinessObjectDefinitionEntity(businessObjectDefinitionName).getNamespace().getCode();
    }

    /**
     * Retrieves a legacy business object definition by it's name and ensure it exists.
     *
     * @param businessObjectDefinitionName the business object definition name (case-insensitive)
     *
     * @return the legacy business object definition entity for the specified name
     * @throws ObjectNotFoundException if the business object definition entity doesn't exist
     */
    public BusinessObjectDefinitionEntity getLegacyBusinessObjectDefinitionEntity(String businessObjectDefinitionName) throws ObjectNotFoundException
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = dmDao.getLegacyBusinessObjectDefinitionByName(businessObjectDefinitionName);

        if (businessObjectDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Legacy business object definition with name \"%s\" doesn't exist.", businessObjectDefinitionName));
        }

        return businessObjectDefinitionEntity;
    }

    /**
     * Retrieves a business object definition entity by it's key and ensure it exists.
     *
     * @param businessObjectDefinitionKey the business object definition name (case-insensitive)
     *
     * @return the business object definition entity
     * @throws ObjectNotFoundException if the business object definition entity doesn't exist
     */
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionEntity(BusinessObjectDefinitionKey businessObjectDefinitionKey)
        throws ObjectNotFoundException
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = dmDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);

        if (businessObjectDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                businessObjectDefinitionKey.getBusinessObjectDefinitionName(), businessObjectDefinitionKey.getNamespace()));
        }

        return businessObjectDefinitionEntity;
    }

    /**
     * Gets the file type entity and ensure it exists.
     *
     * @param fileType the file type (case insensitive)
     *
     * @return the file type entity
     * @throws ObjectNotFoundException if the file type entity doesn't exist
     */
    public FileTypeEntity getFileTypeEntity(String fileType) throws ObjectNotFoundException
    {
        FileTypeEntity fileTypeEntity = dmDao.getFileTypeByCode(fileType);

        if (fileTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("File type with code \"%s\" doesn't exist.", fileType));
        }

        return fileTypeEntity;
    }

    /**
     * Gets the partition key group entity and ensure it exists.
     *
     * @param partitionKeyGroupKey the partition key group key
     *
     * @return the partition key group entity
     * @throws ObjectNotFoundException if the partition key group entity doesn't exist
     */
    public PartitionKeyGroupEntity getPartitionKeyGroupEntity(PartitionKeyGroupKey partitionKeyGroupKey) throws ObjectNotFoundException
    {
        return getPartitionKeyGroupEntity(partitionKeyGroupKey.getPartitionKeyGroupName());
    }

    /**
     * Gets the partition key group entity and ensure it exists.
     *
     * @param partitionKeyGroupName the partition key group name (case insensitive)
     *
     * @return the partition key group entity
     * @throws ObjectNotFoundException if the partition key group entity doesn't exist
     */
    public PartitionKeyGroupEntity getPartitionKeyGroupEntity(String partitionKeyGroupName) throws ObjectNotFoundException
    {
        PartitionKeyGroupEntity partitionKeyGroupEntity = dmDao.getPartitionKeyGroupByName(partitionKeyGroupName);

        if (partitionKeyGroupEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Partition key group \"%s\" doesn't exist.", partitionKeyGroupName));
        }

        return partitionKeyGroupEntity;
    }

    /**
     * Gets a business object format entity based on the alternate key and makes sure that it exists. If a format version isn't specified in the business object
     * format alternate key, the latest available format version will be used.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the business object format entity
     * @throws ObjectNotFoundException if the business object format entity doesn't exist
     */
    public BusinessObjectFormatEntity getBusinessObjectFormatEntity(BusinessObjectFormatKey businessObjectFormatKey) throws ObjectNotFoundException
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = dmDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey);

        if (businessObjectFormatEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object format with namespace \"%s\", business object definition name \"%s\", " +
                "format usage \"%s\", format file type \"%s\", and format version \"%d\" doesn't exist.", businessObjectFormatKey.getNamespace(),
                businessObjectFormatKey.getBusinessObjectDefinitionName(), businessObjectFormatKey.getBusinessObjectFormatUsage(),
                businessObjectFormatKey.getBusinessObjectFormatFileType(), businessObjectFormatKey.getBusinessObjectFormatVersion()));
        }

        return businessObjectFormatEntity;
    }

    /**
     * Creates a business object format key from specified business object format entity.
     *
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the business object format key
     */
    public BusinessObjectFormatKey getBusinessObjectFormatKey(BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();

        businessObjectFormatKey.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectFormatKey.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        businessObjectFormatKey.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        businessObjectFormatKey.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        businessObjectFormatKey.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());

        return businessObjectFormatKey;
    }

    /**
     * Returns a string representation of the alternate key values for the business object format.
     *
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the string representation of the alternate key values for the business object format entity
     */
    public String businessObjectFormatEntityAltKeyToString(BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        return String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
            "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d",
            businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
            businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion());
    }

    /**
     * Determines if the specified business object data attribute is a required attribute or not.
     *
     * @param businessObjectAttributeName the name of the business object attribute
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return true if this business object data attribute is a required attribute, otherwise false
     */
    public boolean isBusinessObjectDataAttributeRequired(String businessObjectAttributeName, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        boolean required = false;

        for (BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity : businessObjectFormatEntity.getAttributeDefinitions())
        {
            if (businessObjectAttributeName.equalsIgnoreCase(attributeDefinitionEntity.getName()))
            {
                required = true;
                break;
            }
        }

        return required;
    }

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
        CustomDdlEntity customDdlEntity = dmDao.getCustomDdlByKey(customDdlKey);

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

    /**
     * Gets a business object status entity and ensure it exists.
     *
     * @param code the code (case insensitive)
     *
     * @return the Business Object Data Status Entity
     * @throws ObjectNotFoundException if the status entity doesn't exist
     */
    public BusinessObjectDataStatusEntity getBusinessObjectDataStatusEntity(String code) throws ObjectNotFoundException
    {
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = dmDao.getBusinessObjectDataStatusByCode(code);

        if (businessObjectDataStatusEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object data status \"%s\" doesn't exist.", code));
        }

        return businessObjectDataStatusEntity;
    }

    /**
     * Gets business object data based on the key information.
     *
     * @param businessObjectDataKey the business object data key.
     *
     * @return the business object data.
     */
    public BusinessObjectDataEntity getBusinessObjectDataEntity(BusinessObjectDataKey businessObjectDataKey)
    {
        // Retrieve the business object data entity regardless of its status.
        return getBusinessObjectDataEntityByKeyAndStatus(businessObjectDataKey, null);
    }

    /**
     * Retrieves business object data by it's key. If a format version isn't specified, the latest available format version (for this partition value) will be
     * used. If a business object data version isn't specified, the latest data version based on the specified business object data status is returned. When
     * both business object data version and business object data status are not specified, the latest data version for each set of partition values will be
     * used regardless of the status.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified.
     *
     * @return the business object data
     */
    public BusinessObjectDataEntity getBusinessObjectDataEntityByKeyAndStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        // Get the business object data based on the specified parameters.
        BusinessObjectDataEntity businessObjectDataEntity = dmDao.getBusinessObjectDataByAltKeyAndStatus(businessObjectDataKey, businessObjectDataStatus);

        // Make sure that business object data exists.
        if (businessObjectDataEntity == null)
        {
            throw new ObjectNotFoundException(
                String.format("Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", " +
                    "businessObjectDataSubPartitionValues: \"%s\", businessObjectDataVersion: %d, businessObjectDataStatus: \"%s\"} doesn't exist.",
                    businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                    businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                    businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
                    CollectionUtils.isEmpty(businessObjectDataKey.getSubPartitionValues()) ? "" :
                        StringUtils.join(businessObjectDataKey.getSubPartitionValues(), ","), businessObjectDataKey.getBusinessObjectDataVersion(),
                    businessObjectDataStatus));
        }

        // Return the retrieved business object data entity.
        return businessObjectDataEntity;
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

        return dmHelper.businessObjectDataKeyToString(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
            businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion(),
            businessObjectDataEntity.getPartitionValue(), dmHelper.getSubPartitionValues(businessObjectDataEntity), businessObjectDataEntity.getVersion());
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
        businessObjectDataKey.setSubPartitionValues(dmHelper.getSubPartitionValues(businessObjectDataEntity));
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());

        return businessObjectDataKey;
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
            List<String> subPartitionValues = dmHelper.getSubPartitionValues(businessObjectDataEntity);

            if (partitionColumnPosition > BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION && partitionColumnPosition < subPartitionValues.size() + 2)
            {
                // Please note that the value of the second partition column is located at index 0.
                partitionValue = subPartitionValues.get(partitionColumnPosition - 2);
            }
        }

        return partitionValue;
    }

    /**
     * Returns a partition filter that the specified business object data entity would match to.  The filter is build as per specified sample partition filter.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param samplePartitionFilter the sample partition filter
     *
     * @return the partition filter
     */
    public List<String> getPartitionFilter(BusinessObjectDataEntity businessObjectDataEntity, List<String> samplePartitionFilter)
    {
        BusinessObjectDataKey businessObjectDataKey = getBusinessObjectDataKey(businessObjectDataEntity);

        List<String> resultPartitionFilter = new ArrayList<>();

        resultPartitionFilter.add(samplePartitionFilter.get(0) != null ? businessObjectDataKey.getPartitionValue() : null);

        for (int i = 0; i < BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            resultPartitionFilter.add(samplePartitionFilter.get(i + 1) != null ? businessObjectDataKey.getSubPartitionValues().get(i) : null);
        }

        return resultPartitionFilter;
    }

    /**
     * Gets a business object data attribute entity on the key and makes sure that it exists.
     *
     * @param businessObjectDataAttributeKey the business object data attribute key
     *
     * @return the business object data attribute entity
     * @throws ObjectNotFoundException if the business object data or the business object data attribute don't exist
     */
    public BusinessObjectDataAttributeEntity getBusinessObjectDataAttributeEntity(BusinessObjectDataAttributeKey businessObjectDataAttributeKey)
        throws ObjectNotFoundException
    {
        // Get the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = getBusinessObjectDataEntity(
            new BusinessObjectDataKey(businessObjectDataAttributeKey.getNamespace(), businessObjectDataAttributeKey.getBusinessObjectDefinitionName(),
                businessObjectDataAttributeKey.getBusinessObjectFormatUsage(), businessObjectDataAttributeKey.getBusinessObjectFormatFileType(),
                businessObjectDataAttributeKey.getBusinessObjectFormatVersion(), businessObjectDataAttributeKey.getPartitionValue(),
                businessObjectDataAttributeKey.getSubPartitionValues(), businessObjectDataAttributeKey.getBusinessObjectDataVersion()));

        // Load all existing business object data attribute entities into a map for quick access using lowercase attribute names.
        Map<String, BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntityMap =
            getBusinessObjectDataAttributeEntityMap(businessObjectDataEntity.getAttributes());

        // Get the relative entity using the attribute name in lowercase.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            businessObjectDataAttributeEntityMap.get(businessObjectDataAttributeKey.getBusinessObjectDataAttributeName().toLowerCase());
        if (businessObjectDataAttributeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Attribute with name \"%s\" does not exist for business object data {%s}.",
                businessObjectDataAttributeKey.getBusinessObjectDataAttributeName(), businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        return businessObjectDataAttributeEntity;
    }

    /**
     * Creates a map that maps business object data attribute names in lowercase to the relative business object data attribute entities.
     *
     * @param businessObjectDataAttributeEntities the collection of business object data attribute entities to be loaded into the map
     *
     * @return the map that maps business object data attribute names in lowercase to the relative business object data attribute entities
     */
    public Map<String, BusinessObjectDataAttributeEntity> getBusinessObjectDataAttributeEntityMap(
        Collection<BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntities)
    {
        Map<String, BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntityMap = new HashMap<>();

        for (BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity : businessObjectDataAttributeEntities)
        {
            businessObjectDataAttributeEntityMap.put(businessObjectDataAttributeEntity.getName().toLowerCase(), businessObjectDataAttributeEntity);
        }

        return businessObjectDataAttributeEntityMap;
    }

    /**
     * Gets a storage entity based on the alternate key and makes sure that it exists.
     *
     * @param storageAlternateKey the storage entity alternate key
     *
     * @return the storage entity
     * @throws ObjectNotFoundException if the storage entity doesn't exist
     */
    public StorageEntity getStorageEntity(StorageAlternateKeyDto storageAlternateKey) throws ObjectNotFoundException
    {
        return getStorageEntity(storageAlternateKey.getStorageName());
    }

    /**
     * Gets a storage entity based on the storage name and makes sure that it exists.
     *
     * @param storageName the storage name (case insensitive)
     *
     * @return the storage entity
     * @throws ObjectNotFoundException if the storage entity doesn't exist
     */
    public StorageEntity getStorageEntity(String storageName) throws ObjectNotFoundException
    {
        StorageEntity storageEntity = dmDao.getStorageByName(storageName);

        if (storageEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage with name \"%s\" doesn't exist.", storageName));
        }

        return storageEntity;
    }

    /**
     * Gets the notification event type entity and ensure it exists.
     *
     * @param code the notification event type code (case insensitive)
     *
     * @return the notification event type entity
     * @throws ObjectNotFoundException if the entity doesn't exist
     */
    public NotificationEventTypeEntity getNotificationEventTypeEntity(String code) throws ObjectNotFoundException
    {
        NotificationEventTypeEntity notificationEventTypeEntity = dmDao.getNotificationEventTypeByCode(code);

        if (notificationEventTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Notification event type with code \"%s\" doesn't exist.", code));
        }

        return notificationEventTypeEntity;
    }

    /**
     * Gets a business object data notification registration entity based on the key and makes sure that it exists.
     *
     * @param key the business object data notification registration key
     *
     * @return the business object data notification registration entity
     * @throws ObjectNotFoundException if the entity doesn't exist
     */
    public BusinessObjectDataNotificationRegistrationEntity getBusinessObjectDataNotificationEntity(BusinessObjectDataNotificationRegistrationKey key)
        throws ObjectNotFoundException
    {
        BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity =
            dmDao.getBusinessObjectDataNotificationByAltKey(key);

        if (businessObjectDataNotificationRegistrationEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Business object data notification registration with name \"%s\" does not exist for \"%s\" namespace.", key.getNotificationName(),
                    key.getNamespace()));
        }

        return businessObjectDataNotificationRegistrationEntity;
    }

    /**
     * Retrieve and ensures that a job definition entity exists.
     *
     * @param namespace the namespace (case insensitive)
     * @param jobName the job name (case insensitive)
     *
     * @return the job definition entity
     * @throws ObjectNotFoundException if the storage entity doesn't exist
     */
    public JobDefinitionEntity getJobDefinitionEntity(String namespace, String jobName) throws ObjectNotFoundException
    {
        JobDefinitionEntity jobDefinitionEntity = dmDao.getJobDefinitionByAltKey(namespace, jobName);

        if (jobDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Job definition with namespace \"%s\" and job name \"%s\" doesn't exist.", namespace, jobName));
        }

        return jobDefinitionEntity;
    }

    /**
     * Gets attribute value by name from the storage entity while specifying whether the attribute is required and whether the attribute value is required.
     *
     * @param attributeName the attribute name (case insensitive)
     * @param storageEntity the storage entity
     * @param attributeRequired specifies whether the attribute is mandatory (i.e. whether it has a value or not).
     * @param attributeValueRequiredIfExists specifies whether the attribute value is mandatory (i.e. the attribute must exist and its value must also contain a
     * value).
     *
     * @return the attribute value from the attribute with the attribute name.
     * @throws IllegalStateException if the attribute is mandatory and this storage contains no attribute with this attribute name or the value is blank.This
     * will produce a 500 HTTP status code error. If storage attributes are able to be updated by a REST invocation in the future, we might want to consider
     * making this a 400 instead since the user has the ability to fix the issue on their own.
     */
    public String getStorageAttributeValueByName(String attributeName, StorageEntity storageEntity, boolean attributeRequired,
        boolean attributeValueRequiredIfExists) throws IllegalStateException
    {
        boolean attributeExists = false;
        String attributeValue = null;

        for (StorageAttributeEntity attributeEntity : storageEntity.getAttributes())
        {
            if (attributeEntity.getName().equalsIgnoreCase(attributeName))
            {
                attributeExists = true;
                attributeValue = attributeEntity.getValue();
                break;
            }
        }

        // If the attribute must exist and doesn't, throw an exception.
        if (attributeRequired && !attributeExists)
        {
            throw new IllegalStateException(String.format("Attribute \"%s\" for \"%s\" storage must be configured.", attributeName, storageEntity.getName()));
        }

        // If the attribute is configured, but has a blank value, throw an exception.
        if (attributeExists && attributeValueRequiredIfExists && StringUtils.isBlank(attributeValue))
        {
            throw new IllegalStateException(
                String.format("Attribute \"%s\" for \"%s\" storage must have a value that is not blank.", attributeName, storageEntity.getName()));
        }

        return attributeValue;
    }

    /**
     * Gets attribute value by name from the storage entity while specifying whether the attribute value is required (i.e. it must exist and must have a value
     * configured). This is a convenience method when a value must be returned (i.e. the attribute must be configured and have a value present) or is totally
     * optional (i.e. the attribute can not exist or it can exist and have a blank value).
     *
     * @param attributeName the attribute name (case insensitive)
     * @param storageEntity the storage entity
     * @param attributeValueRequired specifies whether the attribute value is mandatory.
     *
     * @return the attribute value from the attribute with the attribute name.
     * @throws IllegalArgumentException if the attribute is mandatory and this storage contains no attribute with this attribute name
     */
    public String getStorageAttributeValueByName(String attributeName, StorageEntity storageEntity, boolean attributeValueRequired)
        throws IllegalArgumentException
    {
        return getStorageAttributeValueByName(attributeName, storageEntity, attributeValueRequired, attributeValueRequired);
    }

    /**
     * Gets attribute value by name from the storage entity and returns it as a boolean. Most types of boolean strings are supported (e.g. true/false, on/off,
     * yes/no, etc.).
     *
     * @param attributeName the attribute name (case insensitive)
     * @param storageEntity the storage entity
     * @param attributeRequired specifies whether the attribute is mandatory (i.e. whether it has a value or not).
     * @param attributeValueRequiredIfExists specifies whether the attribute value is mandatory (i.e. the attribute must exist and its value must also contain a
     * value).
     *
     * @return the attribute value from the attribute with the attribute name as a boolean. If no value is configured and the attribute isn't required, then
     *         false is returned.
     * @throws IllegalStateException if an invalid storage attribute boolean value was configured.
     */
    public boolean getBooleanStorageAttributeValueByName(String attributeName, StorageEntity storageEntity, boolean attributeRequired,
        boolean attributeValueRequiredIfExists) throws IllegalStateException
    {
        // Get the boolean string value.
        // The required flag is being passed so an exception will be thrown if it is required and isn't present.
        String booleanStringValue = getStorageAttributeValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);

        // If it isn't required, then treat a blank value as "false".
        if (StringUtils.isBlank(booleanStringValue))
        {
            return false;
        }

        // Use custom boolean editor without allowed empty strings to convert the value of the argument to a boolean value.
        CustomBooleanEditor customBooleanEditor = new CustomBooleanEditor(attributeRequired);
        try
        {
            customBooleanEditor.setAsText(booleanStringValue);
        }
        catch (IllegalArgumentException e)
        {
            // This will produce a 500 HTTP status code error. If storage attributes are able to be updated by a REST invocation in the future,
            // we might want to consider making this a 400 instead since the user has the ability to fix the issue on their own.
            throw new IllegalStateException(String
                .format("Attribute \"%s\" for \"%s\" storage has an invalid boolean value: \"%s\".", attributeName, storageEntity.getName(),
                    booleanStringValue), e);
        }

        // Return the boolean value.
        return (Boolean) customBooleanEditor.getValue();
    }

    /**
     * Retrieves a storage unit entity for the business object data in the specified storage and make sure it exists.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param storageName the storage name
     *
     * @return the storage unit entity
     * @throws ObjectNotFoundException if the storage unit couldn't be found.
     */
    public StorageUnitEntity getStorageUnitEntity(BusinessObjectDataEntity businessObjectDataEntity, String storageName) throws ObjectNotFoundException
    {
        StorageUnitEntity resultStorageUnitEntity = null;

        for (StorageUnitEntity storageUnitEntity : businessObjectDataEntity.getStorageUnits())
        {
            if (storageUnitEntity.getStorage().getName().equalsIgnoreCase(storageName))
            {
                resultStorageUnitEntity = storageUnitEntity;
            }
        }

        if (resultStorageUnitEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", storageName,
                businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        return resultStorageUnitEntity;
    }

    /**
     * Retrieves a list of storage file paths.
     *
     * @param storageUnitEntity the storage unit entity
     *
     * @return the of storage file paths
     */
    public List<String> getStorageFilePaths(StorageUnitEntity storageUnitEntity)
    {
        List<String> storageFilePaths = new ArrayList<>();

        for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
        {
            storageFilePaths.add(storageFileEntity.getPath());
        }

        return storageFilePaths;
    }

    /**
     * Retrieves storage file by storage name and file path.
     *
     * @param storageName the storage name
     * @param filePath the file path
     *
     * @return the storage file
     * @throws ObjectNotFoundException if the storage file doesn't exist
     * @throws IllegalArgumentException if more than one storage file matching the file path exist in the storage
     */
    public StorageFileEntity getStorageFileEntity(String storageName, String filePath) throws ObjectNotFoundException, IllegalArgumentException
    {
        StorageFileEntity storageFileEntity = dmDao.getStorageFileByStorageNameAndFilePath(storageName, filePath);

        if (storageFileEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage file \"%s\" doesn't exist in \"%s\" storage.", filePath, storageName));
        }

        return storageFileEntity;
    }

    /**
     * Validates a list of storage files registered with specified business object data at specified storage. This method makes sure that all storage files
     * match the expected s3 key prefix value.
     *
     * @param storageFilePaths the list of storage file paths to be validated
     * @param s3KeyPrefix the S3 key prefix that storage file paths are expected to start with
     * @param businessObjectDataEntity the business object data entity
     * @param storageName the name of the storage that storage files are stored in
     *
     * @throws IllegalArgumentException if a storage file doesn't match the expected S3 key prefix.
     */
    public void validateStorageFiles(List<String> storageFilePaths, String s3KeyPrefix, BusinessObjectDataEntity businessObjectDataEntity, String storageName)
        throws IllegalArgumentException
    {
        for (String storageFilePath : storageFilePaths)
        {
            Assert.isTrue(storageFilePath.startsWith(s3KeyPrefix), String
                .format("Storage file \"%s\" registered with business object data {%s} in \"%s\" storage does not match the expected S3 key prefix \"%s\".",
                    storageFilePath, businessObjectDataEntityAltKeyToString(businessObjectDataEntity), storageName, s3KeyPrefix));
        }
    }

    /**
     * Gets an EMR cluster definition entity based on the key and makes sure that it exists.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @return the EMR cluster definition entity
     * @throws ObjectNotFoundException if the EMR cluster definition entity doesn't exist
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionEntity(EmrClusterDefinitionKey emrClusterDefinitionKey) throws ObjectNotFoundException
    {
        return getEmrClusterDefinitionEntity(emrClusterDefinitionKey.getNamespace(), emrClusterDefinitionKey.getEmrClusterDefinitionName());
    }

    /**
     * Gets an EMR cluster definition entity based on the namespace and cluster definition name and makes sure that it exists.
     *
     * @param namespace the namespace
     * @param clusterDefinitionName the cluster definition name
     *
     * @return the EMR cluster definition entity
     * @throws ObjectNotFoundException if the EMR cluster definition entity doesn't exist
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionEntity(String namespace, String clusterDefinitionName) throws ObjectNotFoundException
    {
        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = dmDao.getEmrClusterDefinitionByAltKey(namespace, clusterDefinitionName);
        if (emrClusterDefinitionEntity == null)
        {
            throw new ObjectNotFoundException("EMR cluster definition with name \"" + clusterDefinitionName + "\" doesn't exist for namespace \"" +
                namespace + "\".");
        }
        return emrClusterDefinitionEntity;
    }

    /**
     * Checks if the method name is not allowed against the configuration.
     *
     * @param methodName the method name
     *
     * @throws MethodNotAllowedException if requested method is not allowed.
     */
    public void checkNotAllowedMethod(String methodName) throws MethodNotAllowedException
    {
        boolean needToBlock = false;
        ConfigurationEntity configurationEntity = dmDao.getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_DM_ENDPOINTS.getKey());
        if (configurationEntity != null && StringUtils.isNotBlank(configurationEntity.getValueClob()))
        {
            List<String> methodsToBeBlocked = dmStringHelper.splitStringWithDefaultDelimiter(configurationEntity.getValueClob());
            needToBlock = methodsToBeBlocked.contains(methodName);
        }

        if (needToBlock)
        {
            throw new MethodNotAllowedException("The requested method is not allowed.");
        }
    }

    /**
     * Returns a new instance of S3FileTransferRequestParamsDto populated with all parameters, required to access an S3 bucket.
     *
     * @param storageEntity the storage entity that contains attributes to access an S3 bucket
     *
     * @return the S3FileTransferRequestParamsDto instance that can be used to access S3 bucket
     */
    public S3FileTransferRequestParamsDto getS3BucketAccessParams(StorageEntity storageEntity)
    {
        // Get S3 bucket specific configuration settings.
        // Please note that since those values are required we pass a "true" flag.
        String s3BucketName =
            getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);

        return getS3BucketAccessParams(s3BucketName);
    }

    /**
     * Returns a new instance of S3FileTransferRequestParamsDto populated with all parameters, required to access an S3 bucket.
     *
     * @param s3BucketName the S3 bucket name.
     *
     * @return the S3FileTransferRequestParamsDto instance that can be used to access S3 bucket.
     */
    public S3FileTransferRequestParamsDto getS3BucketAccessParams(String s3BucketName)
    {
        S3FileTransferRequestParamsDto params = getS3FileTransferRequestParamsDto();

        params.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
        params.setS3BucketName(s3BucketName);

        return params;
    }

    /**
     * Returns a new {@link S3FileTransferRequestParamsDto} with proxy host and port populated from the configuration.
     *
     * @return {@link S3FileTransferRequestParamsDto} with proxy host and port.
     */
    public S3FileTransferRequestParamsDto getS3FileTransferRequestParamsDto()
    {
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();

        // Get HTTP proxy configuration settings.
        String httpProxyHost = configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_HOST);
        Integer httpProxyPort = configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_PORT, Integer.class);

        params.setHttpProxyHost(httpProxyHost);
        params.setHttpProxyPort(httpProxyPort);
        return params;
    }

    /**
     * Adds the JMS message to the database queue.
     *
     * @param jmsQueueName the JMS queue name
     * @param messageText the message text
     *
     * @return the JMS message entity
     */
    public JmsMessageEntity addJmsMessageToDatabaseQueue(String jmsQueueName, String messageText)
    {
        JmsMessageEntity jmsMessageEntity = new JmsMessageEntity();
        jmsMessageEntity.setJmsQueueName(jmsQueueName);
        jmsMessageEntity.setMessageText(messageText);
        jmsMessageEntity = dmDao.saveAndRefresh(jmsMessageEntity);

        // Set to schedule JMS publishing job. 
        ScheduleJmsPublishingJobAdvice.setScheduleJmsPublishingJob();

        return jmsMessageEntity;
    }
}
