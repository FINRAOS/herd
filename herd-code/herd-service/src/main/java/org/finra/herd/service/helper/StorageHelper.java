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

import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.propertyeditors.CustomBooleanEditor;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.StsDao;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.StorageAttributeEntity;
import org.finra.herd.model.jpa.StorageEntity;

/**
 * A helper class for Storage related code.
 */
@Component
public class StorageHelper
{
    @Autowired
    protected StsDao stsDao;

    @Autowired
    private ConfigurationHelper configurationHelper;

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
     * Returns a new {@link org.finra.herd.model.dto.S3FileCopyRequestParamsDto} with proxy host and port populated from the configuration.
     *
     * @return {@link org.finra.herd.model.dto.S3FileCopyRequestParamsDto} with proxy host and port.
     */
    public S3FileCopyRequestParamsDto getS3FileCopyRequestParamsDto()
    {
        S3FileCopyRequestParamsDto params = new S3FileCopyRequestParamsDto();

        // Update the parameters with proxy host and port retrieved from the configuration.
        setProxyHostAndPort(params);

        return params;
    }

    /**
     * Returns a new {@link S3FileTransferRequestParamsDto} with proxy host and port populated from the configuration.
     *
     * @return the {@link S3FileTransferRequestParamsDto} object
     */
    public S3FileTransferRequestParamsDto getS3FileTransferRequestParamsDto()
    {
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();

        // Update the parameters with proxy host and port retrieved from the configuration.
        setProxyHostAndPort(params);

        return params;
    }

    /**
     * Returns a new {@link S3FileTransferRequestParamsDto} with temporary credentials as per specified AWS role and session name.
     *
     * @param roleArn the ARN of the role
     * @param sessionName the session name
     *
     * @return the {@link S3FileTransferRequestParamsDto} object
     */
    public S3FileTransferRequestParamsDto getS3FileTransferRequestParamsDtoByRole(String roleArn, String sessionName)
    {
        // Get the S3 file transfer request parameters DTO with proxy host and port populated from the configuration.
        S3FileTransferRequestParamsDto params = getS3FileTransferRequestParamsDto();

        // Assume the specified role. Set the duration of the role session to 3600 seconds (1 hour).
        Credentials credentials = stsDao.getTemporarySecurityCredentials(params, sessionName, roleArn, 3600, null);

        // Update the AWS parameters DTO with the temporary credentials.
        params.setAwsAccessKeyId(credentials.getAccessKeyId());
        params.setAwsSecretKey(credentials.getSecretAccessKey());
        params.setSessionToken(credentials.getSessionToken());

        return params;
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
     * Gets attribute value by name from the storage object instance.
     *
     * @param attributeName the attribute name (case insensitive)
     * @param storage the storage
     * @param required specifies whether the attribute value is mandatory
     *
     * @return the attribute value from the attribute with the attribute name, or {@code null} if this list contains no attribute with this attribute name
     */
    public String getStorageAttributeValueByName(String attributeName, Storage storage, Boolean required)
    {
        String attributeValue = null;

        for (Attribute attribute : storage.getAttributes())
        {
            if (attribute.getName().equalsIgnoreCase(attributeName))
            {
                attributeValue = attribute.getValue();
                break;
            }
        }

        if (required && StringUtils.isBlank(attributeValue))
        {
            throw new IllegalStateException(String.format("Attribute \"%s\" for \"%s\" storage must be configured.", attributeName, storage.getName()));
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
     * @param attributeValueRequired specifies whether the attribute value is mandatory
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
     * Gets the storage's bucket name. Throws if not defined.
     *
     * @param storageEntity the storage entity
     *
     * @return the S3 bucket name
     */
    public String getStorageBucketName(StorageEntity storageEntity)
    {
        return getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);
    }

    /**
     * Gets the storage's KMS key ID. Throws if not defined.
     *
     * @param storageEntity the storage entity
     *
     * @return the KMS key ID
     */
    public String getStorageKmsKeyId(StorageEntity storageEntity)
    {
        return getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KMS_KEY_ID), storageEntity, true);
    }

    /**
     * Updates an {@link S3FileTransferRequestParamsDto} instance with proxy host and port retrieved from the configuration.
     *
     * @param params the S3FileTransferRequestParamsDto
     */
    private void setProxyHostAndPort(S3FileTransferRequestParamsDto params)
    {
        // Get HTTP proxy configuration settings from the configuration.
        String httpProxyHost = configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_HOST);
        Integer httpProxyPort = configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_PORT, Integer.class);

        // Update the DTO with the HTTP proxy configuration settings.
        params.setHttpProxyHost(httpProxyHost);
        params.setHttpProxyPort(httpProxyPort);
    }
}
