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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.propertyeditors.CustomBooleanEditor;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StorageAlternateKeyDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageAttributeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * Helper for storage related operations which require DAO.
 */
@Component
public class StorageDaoHelper
{
    @Autowired
    private HerdDaoHelper herdDaoHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdDao herdDao;

    /**
     * Gets a storage attribute value by name. The value must be a valid integer, otherwise a validation error is thrown.
     *
     * @param attributeName the attribute name (case insensitive)
     * @param storageEntity the storage entity
     * @param attributeRequired specifies whether the attribute is mandatory (i.e. whether it has a value or not).
     * @param attributeValueRequiredIfExists specifies whether the attribute value is mandatory (i.e. the attribute must exist and its value must also contain a
     * value).
     *
     * @return the attribute value from the attribute with the attribute name.
     * @throws IllegalStateException if the attribute is mandatory and this storage contains no attribute with this attribute name or the value is blank. This
     * will produce a 500 HTTP status code error. If storage attributes are able to be updated by a REST invocation in the future, we might want to consider
     * making this a 400 instead since the user has the ability to fix the issue on their own. Also throws when the value exists, but is not a valid integer.
     */
    public Integer getStorageAttributeIntegerValueByName(String attributeName, StorageEntity storageEntity, boolean attributeRequired,
        boolean attributeValueRequiredIfExists) throws IllegalStateException
    {
        Integer intValue = null;
        String value = getStorageAttributeValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
        if (value != null)
        {
            if (!StringUtils.isNumeric(value))
            {
                throw new IllegalStateException("Storage attribute \"" + attributeName + "\" must be a valid integer. Actual value is \"" + value + "\"");
            }
            intValue = Integer.parseInt(value.trim());
        }
        return intValue;
    }

    /**
     * Gets a storage attribute value by name. If the attribute does not exist, returns the default value. If the attribute exists, and is defined, the value
     * MUST be an integer, otherwise a exception is thrown.
     *
     * @param attributeName The name of attribute
     * @param storageEntity The storage entity
     * @param defaultValue The default value
     *
     * @return The integer value
     */
    public Integer getStorageAttributeIntegerValueByName(String attributeName, StorageEntity storageEntity, Integer defaultValue)
    {
        Integer value = getStorageAttributeIntegerValueByName(attributeName, storageEntity, false, false);
        if (value == null)
        {
            value = defaultValue;
        }
        return value;
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

        S3FileTransferRequestParamsDto params = getS3FileTransferRequestParamsDto();

        params.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
        params.setS3BucketName(s3BucketName);

        return params;
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
        StorageEntity storageEntity = herdDao.getStorageByName(storageName);

        if (storageEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage with name \"%s\" doesn't exist.", storageName));
        }

        return storageEntity;
    }

    /**
     * Gets the storage policy rule type entity and ensure it exists.
     *
     * @param code the storage policy rule type code (case insensitive)
     *
     * @return the storage policy rule type entity
     * @throws ObjectNotFoundException if the entity doesn't exist
     */
    public StoragePolicyRuleTypeEntity getStoragePolicyRuleTypeEntity(String code) throws ObjectNotFoundException
    {
        StoragePolicyRuleTypeEntity storagePolicyRuleTypeEntity = herdDao.getStoragePolicyRuleTypeByCode(code);

        if (storagePolicyRuleTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage policy rule type with code \"%s\" doesn't exist.", code));
        }

        return storagePolicyRuleTypeEntity;
    }

    /**
     * Gets a storage policy entity based on the key and makes sure that it exists.
     *
     * @param key the storage policy key
     *
     * @return the storage policy entity
     * @throws ObjectNotFoundException if the entity doesn't exist
     */
    public StoragePolicyEntity getStoragePolicyEntity(StoragePolicyKey key) throws ObjectNotFoundException
    {
        StoragePolicyEntity storagePolicyEntity = herdDao.getStoragePolicyByAltKey(key);

        if (storagePolicyEntity == null)
        {
            throw new ObjectNotFoundException(
                String.format("Storage policy with name \"%s\" does not exist for \"%s\" namespace.", key.getStoragePolicyName(), key.getNamespace()));
        }

        return storagePolicyEntity;
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
        StorageFileEntity storageFileEntity = herdDao.getStorageFileByStorageNameAndFilePath(storageName, filePath);

        if (storageFileEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage file \"%s\" doesn't exist in \"%s\" storage.", filePath, storageName));
        }

        return storageFileEntity;
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
     * Validates a list of storage files registered with specified business object data at specified storage. This method makes sure that all storage files
     * match the expected s3 key prefix value.
     *
     * @param storageFilePaths the storage file paths to be validated
     * @param s3KeyPrefix the S3 key prefix that storage file paths are expected to start with
     * @param businessObjectDataEntity the business object data entity
     * @param storageName the name of the storage that storage files are stored in
     *
     * @throws IllegalArgumentException if a storage file doesn't match the expected S3 key prefix.
     */
    public void validateStorageFiles(Collection<String> storageFilePaths, String s3KeyPrefix, BusinessObjectDataEntity businessObjectDataEntity,
        String storageName) throws IllegalArgumentException
    {
        for (String storageFilePath : storageFilePaths)
        {
            Assert.isTrue(storageFilePath.startsWith(s3KeyPrefix), String
                .format("Storage file \"%s\" registered with business object data {%s} in \"%s\" storage does not match the expected S3 key prefix \"%s\".",
                    storageFilePath, herdDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity), storageName, s3KeyPrefix));
        }
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
                herdDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        return resultStorageUnitEntity;
    }

    /**
     * Gets a storage unit status entity by the code and ensure it exists.
     *
     * @param code the storage unit status code (case insensitive)
     *
     * @return the storage unit status entity
     * @throws ObjectNotFoundException if the status entity doesn't exist
     */
    public StorageUnitStatusEntity getStorageUnitStatusEntity(String code) throws ObjectNotFoundException
    {
        StorageUnitStatusEntity storageUnitStatusEntity = herdDao.getStorageUnitStatusByCode(code);

        if (storageUnitStatusEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Storage unit status \"%s\" doesn't exist.", code));
        }

        return storageUnitStatusEntity;
    }
}
