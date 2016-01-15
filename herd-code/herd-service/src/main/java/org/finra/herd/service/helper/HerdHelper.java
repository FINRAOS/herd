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

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;
import org.finra.herd.model.api.xml.InstanceDefinition;
import org.finra.herd.model.api.xml.MasterInstanceDefinition;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NodeTag;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;

/**
 * A helper class for general herd code.
 */
// TODO: This class is too generic and should be split up into smaller more meaningful classes. When this is done, we can remove the PMD suppress warning below.
@SuppressWarnings({"PMD.ExcessivePublicCount", "PMD.TooManyMethods"})
@Component
public class HerdHelper
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdStringHelper herdStringHelper;

    /**
     * Gets a date in a date format from a string format or null if one wasn't specified. The format of the date should match
     * HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.
     *
     * @param dateString the date as a string.
     *
     * @return the date as a date or null if one wasn't specified
     * @throws IllegalArgumentException if the date isn't the correct length or isn't in the correct format
     */
    public Date getDateFromString(String dateString) throws IllegalArgumentException
    {
        String dateStringLocal = dateString;

        // Default the return port value to null.
        Date resultDate = null;

        if (StringUtils.isNotBlank(dateStringLocal))
        {
            // Trim the date string.
            dateStringLocal = dateStringLocal.trim();

            // Verify that the date string has the required length.
            Assert.isTrue(dateStringLocal.length() == HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.length(), String
                .format("A date value \"%s\" must contain %d characters and be in \"%s\" format.", dateStringLocal,
                    HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.length(), HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.toUpperCase()));

            // Convert the date from a String to a Date.
            try
            {
                // Use strict parsing to ensure our date is more definitive.
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK, Locale.US);
                simpleDateFormat.setLenient(false);
                resultDate = simpleDateFormat.parse(dateStringLocal);
            }
            catch (Exception ex)
            {
                throw new IllegalArgumentException(
                    String.format("A date value \"%s\" must be in \"%s\" format.", dateStringLocal, HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.toUpperCase()), ex);
            }
        }

        return resultDate;
    }

    /**
     * Gets an HTTP proxy host value.
     *
     * @param environmentHttpProxyHost the environment specific HTTP proxy host value
     * @param overrideHttpProxyHost the optional override value for HTTP proxy host
     *
     * @return the HTTP proxy host value or null if one wasn't specified
     */
    public String getHttpProxyHost(String environmentHttpProxyHost, String overrideHttpProxyHost)
    {
        // Default the return host value to null.
        String httpProxyHost = null;

        if (overrideHttpProxyHost != null)
        {
            httpProxyHost = overrideHttpProxyHost;
        }
        else if (StringUtils.isNotBlank(environmentHttpProxyHost))
        {
            httpProxyHost = environmentHttpProxyHost;
        }

        return httpProxyHost;
    }

    /**
     * Gets an HTTP proxy port value.
     *
     * @param environmentHttpProxyPortString the environment specific HTTP proxy port value as a String
     * @param overrideHttpProxyPort the optional override value for HTTP proxy port
     *
     * @return the HTTP proxy port as an Integer or null if one wasn't specified
     * @throws IllegalStateException if the specified HTTP proxy port isn't a valid integer
     */
    public Integer getHttpProxyPort(String environmentHttpProxyPortString, Integer overrideHttpProxyPort) throws IllegalStateException
    {
        // Default the return port value to null.
        Integer httpProxyPort = null;

        if (overrideHttpProxyPort != null)
        {
            httpProxyPort = overrideHttpProxyPort;
        }
        else
        {
            if (StringUtils.isNotBlank(environmentHttpProxyPortString))
            {
                try
                {
                    httpProxyPort = Integer.valueOf(environmentHttpProxyPortString.trim());
                }
                catch (Exception ex)
                {
                    throw new IllegalStateException(String.format("Configured HTTP proxy port value \"%s\" is not an integer.", environmentHttpProxyPortString),
                        ex);
                }
            }
        }

        return httpProxyPort;
    }

    /**
     * Converts the specified string into tht S3 key prefix format. This implies making the string lower case and converting underscores to dashes.
     *
     * @param string the string to convert
     *
     * @return the string in S3 format
     */
    public String s3KeyPrefixFormat(String string)
    {
        return string.toLowerCase().replace('_', '-');
    }

    /**
     * Validates that the query string parameters aren't duplicated for a list of expected parameters.
     *
     * @param parameterMap the query string parameter map.
     * @param parametersToCheck the query string parameters to check.
     *
     * @throws IllegalArgumentException if any duplicates were found.
     */
    public void validateNoDuplicateQueryStringParams(Map<String, String[]> parameterMap, String... parametersToCheck) throws IllegalArgumentException
    {
        List<String> parametersToCheckList = Arrays.asList(parametersToCheck);
        for (Map.Entry<String, String[]> mapEntry : parameterMap.entrySet())
        {
            if ((parametersToCheckList.contains(mapEntry.getKey())) && (mapEntry.getValue().length != 1))
            {
                throw new IllegalArgumentException("Found " + mapEntry.getValue().length + " occurrences of query string parameter \"" + mapEntry.getKey() +
                    "\", but 1 expected. Values found: \"" + StringUtils.join(mapEntry.getValue(), ", ") + "\".");
            }
        }
    }

    /**
     * Returns Activiti Id constructed according to the template defined.
     *
     * @param namespaceCd the namespace code value
     * @param jobName the job name value
     *
     * @return the Activiti Id
     */
    public String buildActivitiIdString(String namespaceCd, String jobName)
    {
        // Populate a map with the tokens mapped to actual database values.
        Map<String, String> pathToTokenValueMap = new HashMap<>();
        pathToTokenValueMap.put(getNamespaceToken(), namespaceCd);
        pathToTokenValueMap.put(getJobNameToken(), jobName);

        // The Activiti Id will start as the template.
        String activitiId = getActivitiJobDefinitionTemplate();

        // Substitute the tokens with the actual database values.
        for (Map.Entry<String, String> mapEntry : pathToTokenValueMap.entrySet())
        {
            activitiId = activitiId.replaceAll(mapEntry.getKey(), mapEntry.getValue());
        }

        // Return the final Activiti Id.
        return activitiId;
    }

    /**
     * Gets the Activiti job definition template.
     *
     * @return the Activiti job definition template.
     */
    public String getActivitiJobDefinitionTemplate()
    {
        // Set the default Activiti Id tokenized template.
        // ~namespace~.~jobName~
        String defaultActivitiIdTemplate = getNamespaceToken() + "." + getJobNameToken();

        // Get the Activiti Id template from the environment, but use the default if one isn't configured.
        // This gives us the ability to customize/change the format post deployment.
        String template = configurationHelper.getProperty(ConfigurationValue.ACTIVITI_JOB_DEFINITION_ID_TEMPLATE);

        if (template == null)
        {
            template = defaultActivitiIdTemplate;
        }

        return template;
    }

    /**
     * Gets the namespace token.
     *
     * @return the namespace token.
     */
    public String getNamespaceToken()
    {
        // Set the token delimiter based on the environment configuration.
        String tokenDelimiter = configurationHelper.getProperty(ConfigurationValue.TEMPLATE_TOKEN_DELIMITER);

        // Setup the individual token names (using the configured delimiter).
        return tokenDelimiter + "namespace" + tokenDelimiter;
    }

    /**
     * Gets the job name token.
     *
     * @return the job name token.
     */
    public String getJobNameToken()
    {
        // Set the token delimiter based on the environment configuration.
        String tokenDelimiter = configurationHelper.getProperty(ConfigurationValue.TEMPLATE_TOKEN_DELIMITER);

        // Setup the individual token names (using the configured delimiter).
        return tokenDelimiter + "jobName" + tokenDelimiter;
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
        if (!CollectionUtils.isEmpty(businessObjectData.getStorageUnits()))
        {
            for (StorageUnit storageUnit : businessObjectData.getStorageUnits())
            {
                if (storageUnit.getStorage().getName().equalsIgnoreCase(storageName))
                {
                    resultStorageUnit = storageUnit;
                    break;
                }
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
     * Validate a list of S3 files per storage unit information.
     *
     * @param storageUnit the storage unit that contains S3 files to be validated
     * @param actualS3Files the list of the actual S3 files
     * @param s3KeyPrefix the S3 key prefix that was prepended to the S3 file paths, when they were uploaded to S3
     */
    public void validateS3Files(StorageUnit storageUnit, List<String> actualS3Files, String s3KeyPrefix)
    {
        validateS3Files(storageUnit.getStorage().getName(), storageUnit.getStorageFiles(), actualS3Files, s3KeyPrefix);
    }

    /**
     * Validate a list of S3 files per storage unit information.
     *
     * @param storageName the storage name
     * @param storageFiles the list of storage files
     * @param actualS3Files the list of the actual S3 files
     * @param s3KeyPrefix the S3 key prefix that was prepended to the S3 file paths, when they were uploaded to S3
     */
    public void validateS3Files(String storageName, List<StorageFile> storageFiles, List<String> actualS3Files, String s3KeyPrefix)
    {
        // Validate that all files match the expected S3 key prefix and build a list of registered S3 files.
        List<String> registeredS3Files = new ArrayList<>();
        if (!CollectionUtils.isEmpty(storageFiles))
        {
            for (StorageFile storageFile : storageFiles)
            {
                Assert.isTrue(storageFile.getFilePath().startsWith(s3KeyPrefix), String
                    .format("Storage file S3 key prefix \"%s\" does not match the expected S3 key prefix \"%s\".", storageFile.getFilePath(), s3KeyPrefix));
                registeredS3Files.add(storageFile.getFilePath());
            }
        }

        // Validate that all files exist in S3 managed bucket.
        if (!actualS3Files.containsAll(registeredS3Files))
        {
            registeredS3Files.removeAll(actualS3Files);
            throw new IllegalStateException(String.format("Registered file \"%s\" does not exist in \"%s\" storage.", registeredS3Files.get(0), storageName));
        }

        // Validate that no other files in S3 managed bucket have the same S3 key prefix.
        if (!registeredS3Files.containsAll(actualS3Files))
        {
            actualS3Files.removeAll(registeredS3Files);
            throw new IllegalStateException(
                String.format("Found S3 file \"%s\" in \"%s\" storage not registered with this business object data.", actualS3Files.get(0), storageName));
        }
    }

    /**
     * Validate downloaded S3 files per storage unit information.
     *
     * @param baseDirectory the local parent directory path, relative to which the files are expected to be located
     * @param s3KeyPrefix the S3 key prefix that was prepended to the S3 file paths, when they were uploaded to S3
     * @param storageUnit the storage unit that contains a list of storage files to be validated
     *
     * @throws IllegalStateException if files are not valid
     */
    public void validateDownloadedS3Files(String baseDirectory, String s3KeyPrefix, StorageUnit storageUnit) throws IllegalStateException
    {
        validateDownloadedS3Files(baseDirectory, s3KeyPrefix, storageUnit.getStorageFiles());
    }

    /**
     * Validate downloaded S3 files per specified list of storage files.
     *
     * @param baseDirectory the local parent directory path, relative to which the files are expected to be located
     * @param s3KeyPrefix the S3 key prefix that was prepended to the S3 file paths, when they were uploaded to S3
     * @param storageFiles the list of storage files
     *
     * @throws IllegalStateException if files are not valid
     */
    public void validateDownloadedS3Files(String baseDirectory, String s3KeyPrefix, List<StorageFile> storageFiles) throws IllegalStateException
    {
        // Build a target local directory path, which is the parent directory plus the S3 key prefix.
        File targetLocalDirectory = Paths.get(baseDirectory, s3KeyPrefix).toFile();

        // Get a list of all files within the target local directory and its subdirectories.
        Collection<File> actualLocalFiles = FileUtils.listFiles(targetLocalDirectory, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);

        // Validate the total file count.
        int storageFilesCount = CollectionUtils.isEmpty(storageFiles) ? 0 : storageFiles.size();
        if (storageFilesCount != actualLocalFiles.size())
        {
            throw new IllegalStateException(String
                .format("Number of downloaded files does not match the storage unit information (expected %d files, actual %d files).", storageFiles.size(),
                    actualLocalFiles.size()));
        }

        // Validate each downloaded file.
        if (storageFilesCount > 0)
        {
            for (StorageFile storageFile : storageFiles)
            {
                // Create a "real file" that points to the actual file on the file system.
                File localFile = Paths.get(baseDirectory, storageFile.getFilePath()).toFile();

                // Verify that the file exists.
                if (!localFile.isFile())
                {
                    throw new IllegalStateException(String.format("Downloaded \"%s\" file doesn't exist.", localFile));
                }

                // Validate the file size.
                if (localFile.length() != storageFile.getFileSizeBytes())
                {
                    throw new IllegalStateException(String
                        .format("Size of the downloaded \"%s\" S3 file does not match the expected value (expected %d bytes, actual %d bytes).",
                            localFile.getPath(), storageFile.getFileSizeBytes(), localFile.length()));
                }
            }
        }
    }

    /**
     * Validates the attributes.
     *
     * @param attributes the attributes to validate. Null shouldn't be specified.
     *
     * @throws IllegalArgumentException if any invalid attributes were found.
     */
    public void validateAttributes(List<Attribute> attributes) throws IllegalArgumentException
    {
        // Validate attributes if they are specified.
        if (!CollectionUtils.isEmpty(attributes))
        {
            Map<String, String> attributeNameValidationMap = new HashMap<>();
            for (Attribute attribute : attributes)
            {
                Assert.hasText(attribute.getName(), "An attribute name must be specified.");
                attribute.setName(attribute.getName().trim());

                // Ensure the attribute key isn't a duplicate by using a map with a "lowercase" name as the key for case insensitivity.
                String validationMapKey = attribute.getName().toLowerCase();
                if (attributeNameValidationMap.containsKey(validationMapKey))
                {
                    throw new IllegalArgumentException("Duplicate attribute name found: " + attribute.getName());
                }
                attributeNameValidationMap.put(validationMapKey, attribute.getValue());
            }
        }
    }

    /**
     * Builds the string delimited by default delimiter for the input String values.
     *
     * @param inputStrings List of string literals
     *
     * @return the String delimited with the default delimiter
     */
    public String buildStringWithDefaultDelimiter(List<String> inputStrings)
    {
        return StringUtils.join(inputStrings, configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER));
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
        return String
            .format("namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                "businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s\", " +
                "businessObjectDataVersion: %d", namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, businessObjectDataPartitionValue,
                CollectionUtils.isEmpty(businessObjectDataSubPartitionValues) ? "" : StringUtils.join(businessObjectDataSubPartitionValues, ","),
                businessObjectDataVersion);
    }

    /**
     * Validates a namespace key. This method also trims the key parameters.
     *
     * @param namespaceKey the namespace key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateNamespaceKey(NamespaceKey namespaceKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(namespaceKey, "A namespace key must be specified.");
        Assert.hasText(namespaceKey.getNamespaceCode(), "A namespace code must be specified.");

        // Remove leading and trailing spaces.
        namespaceKey.setNamespaceCode(namespaceKey.getNamespaceCode().trim());
    }

    /**
     * Validates the business object definition key. This method also trims the key parameters.
     *
     * @param businessObjectDefinitionKey the business object definition key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectDefinitionKey(BusinessObjectDefinitionKey businessObjectDefinitionKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(businessObjectDefinitionKey, "A business object definition key must be specified.");
        Assert.hasText(businessObjectDefinitionKey.getNamespace(), "A namespace must be specified.");
        Assert.hasText(businessObjectDefinitionKey.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");

        // Remove leading and trailing spaces.
        businessObjectDefinitionKey.setNamespace(businessObjectDefinitionKey.getNamespace().trim());
        businessObjectDefinitionKey.setBusinessObjectDefinitionName(businessObjectDefinitionKey.getBusinessObjectDefinitionName().trim());
    }

    /**
     * Validates the partition key group key. This method also trims the key parameters.
     *
     * @param partitionKeyGroupKey the partition key group key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validatePartitionKeyGroupKey(PartitionKeyGroupKey partitionKeyGroupKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(partitionKeyGroupKey, "A partition key group key must be specified.");
        Assert.hasText(partitionKeyGroupKey.getPartitionKeyGroupName(), "A partition key group name must be specified.");

        // Remove leading and trailing spaces.
        partitionKeyGroupKey.setPartitionKeyGroupName(partitionKeyGroupKey.getPartitionKeyGroupName().trim());
    }

    /**
     * Validates the expected partition value key. This method also trims the key parameters.
     *
     * @param expectedPartitionValueKey the expected partition value key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateExpectedPartitionValueKey(ExpectedPartitionValueKey expectedPartitionValueKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(expectedPartitionValueKey, "An expected partition value key must be specified.");
        Assert.hasText(expectedPartitionValueKey.getPartitionKeyGroupName(), "A partition key group name must be specified.");
        Assert.hasText(expectedPartitionValueKey.getExpectedPartitionValue(), "An expected partition value must be specified.");

        // Remove leading and trailing spaces.
        expectedPartitionValueKey.setPartitionKeyGroupName(expectedPartitionValueKey.getPartitionKeyGroupName().trim());
        expectedPartitionValueKey.setExpectedPartitionValue(expectedPartitionValueKey.getExpectedPartitionValue().trim());
    }

    /**
     * Validates the business object format key. This method also trims the key parameters.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectFormatKey(BusinessObjectFormatKey businessObjectFormatKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(businessObjectFormatKey, "A business object format key must be specified.");
        Assert.hasText(businessObjectFormatKey.getNamespace(), "A namespace must be specified.");
        Assert.hasText(businessObjectFormatKey.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        Assert.hasText(businessObjectFormatKey.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        Assert.hasText(businessObjectFormatKey.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        Assert.notNull(businessObjectFormatKey.getBusinessObjectFormatVersion(), "A business object format version must be specified.");

        // Remove leading and trailing spaces.
        businessObjectFormatKey.setNamespace(businessObjectFormatKey.getNamespace().trim());
        businessObjectFormatKey.setBusinessObjectDefinitionName(businessObjectFormatKey.getBusinessObjectDefinitionName().trim());
        businessObjectFormatKey.setBusinessObjectFormatUsage(businessObjectFormatKey.getBusinessObjectFormatUsage().trim());
        businessObjectFormatKey.setBusinessObjectFormatFileType(businessObjectFormatKey.getBusinessObjectFormatFileType().trim());
    }

    /**
     * Validates the custom DDL key. This method also trims the key parameters.
     *
     * @param customDdlKey the custom DDL key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateCustomDdlKey(CustomDdlKey customDdlKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(customDdlKey, "A custom DDL key must be specified.");
        Assert.hasText(customDdlKey.getNamespace(), "A namespace must be specified.");
        Assert.hasText(customDdlKey.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        Assert.hasText(customDdlKey.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        Assert.hasText(customDdlKey.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        Assert.notNull(customDdlKey.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        Assert.hasText(customDdlKey.getCustomDdlName(), "A custom DDL name must be specified.");

        // Remove leading and trailing spaces.
        customDdlKey.setNamespace(customDdlKey.getNamespace().trim());
        customDdlKey.setBusinessObjectDefinitionName(customDdlKey.getBusinessObjectDefinitionName().trim());
        customDdlKey.setBusinessObjectFormatUsage(customDdlKey.getBusinessObjectFormatUsage().trim());
        customDdlKey.setBusinessObjectFormatFileType(customDdlKey.getBusinessObjectFormatFileType().trim());
        customDdlKey.setCustomDdlName(customDdlKey.getCustomDdlName().trim());
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
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatVersionRequired specifies if the business object format version is required or not
     * @param businessObjectDataVersionRequired specifies if the business object data version is required or not
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectDataKey(BusinessObjectDataKey businessObjectDataKey, boolean businessObjectFormatVersionRequired,
        boolean businessObjectDataVersionRequired) throws IllegalArgumentException
    {
        // Validate and remove leading and trailing spaces.
        Assert.notNull(businessObjectDataKey, "A business object data key must be specified.");

        Assert.hasText(businessObjectDataKey.getNamespace(), "A namespace must be specified.");
        businessObjectDataKey.setNamespace(businessObjectDataKey.getNamespace().trim());
        Assert.hasText(businessObjectDataKey.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectDataKey.getBusinessObjectDefinitionName().trim());
        Assert.hasText(businessObjectDataKey.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectDataKey.getBusinessObjectFormatUsage().trim());
        Assert.hasText(businessObjectDataKey.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectDataKey.getBusinessObjectFormatFileType().trim());

        if (businessObjectFormatVersionRequired)
        {
            Assert.notNull(businessObjectDataKey.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        }

        Assert.hasText(businessObjectDataKey.getPartitionValue(), "A partition value must be specified.");
        businessObjectDataKey.setPartitionValue(businessObjectDataKey.getPartitionValue().trim());

        int subPartitionValuesCount = getCollectionSize(businessObjectDataKey.getSubPartitionValues());
        Assert.isTrue(subPartitionValuesCount <= BusinessObjectDataEntity.MAX_SUBPARTITIONS,
            String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS));

        for (int i = 0; i < subPartitionValuesCount; i++)
        {
            Assert.hasText(businessObjectDataKey.getSubPartitionValues().get(i), "A subpartition value must be specified.");
            businessObjectDataKey.getSubPartitionValues().set(i, businessObjectDataKey.getSubPartitionValues().get(i).trim());
        }

        if (businessObjectDataVersionRequired)
        {
            Assert.notNull(businessObjectDataKey.getBusinessObjectDataVersion(), "A business object data version must be specified.");
        }
    }

    /**
     * Validates the business object data attribute key. This method also trims the key parameters.
     *
     * @param businessObjectDataAttributeKey the business object data attribute key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectDataAttributeKey(BusinessObjectDataAttributeKey businessObjectDataAttributeKey) throws IllegalArgumentException
    {
        // Validate and remove leading and trailing spaces.
        Assert.notNull(businessObjectDataAttributeKey, "A business object data key must be specified.");
        Assert.hasText(businessObjectDataAttributeKey.getNamespace(), "A namespace must be specified.");
        businessObjectDataAttributeKey.setNamespace(businessObjectDataAttributeKey.getNamespace().trim());
        Assert.hasText(businessObjectDataAttributeKey.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        businessObjectDataAttributeKey.setBusinessObjectDefinitionName(businessObjectDataAttributeKey.getBusinessObjectDefinitionName().trim());
        Assert.hasText(businessObjectDataAttributeKey.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        businessObjectDataAttributeKey.setBusinessObjectFormatUsage(businessObjectDataAttributeKey.getBusinessObjectFormatUsage().trim());
        Assert.hasText(businessObjectDataAttributeKey.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        businessObjectDataAttributeKey.setBusinessObjectFormatFileType(businessObjectDataAttributeKey.getBusinessObjectFormatFileType().trim());
        Assert.notNull(businessObjectDataAttributeKey.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        Assert.hasText(businessObjectDataAttributeKey.getPartitionValue(), "A partition value must be specified.");
        businessObjectDataAttributeKey.setPartitionValue(businessObjectDataAttributeKey.getPartitionValue().trim());

        int subPartitionValuesCount = getCollectionSize(businessObjectDataAttributeKey.getSubPartitionValues());
        Assert.isTrue(subPartitionValuesCount <= BusinessObjectDataEntity.MAX_SUBPARTITIONS,
            String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS));

        for (int i = 0; i < subPartitionValuesCount; i++)
        {
            Assert.hasText(businessObjectDataAttributeKey.getSubPartitionValues().get(i), "A subpartition value must be specified.");
            businessObjectDataAttributeKey.getSubPartitionValues().set(i, businessObjectDataAttributeKey.getSubPartitionValues().get(i).trim());
        }

        Assert.notNull(businessObjectDataAttributeKey.getBusinessObjectDataVersion(), "A business object data version must be specified.");
        Assert.hasText(businessObjectDataAttributeKey.getBusinessObjectDataAttributeName(), "A business object data attribute name must be specified.");
        businessObjectDataAttributeKey.setBusinessObjectDataAttributeName(businessObjectDataAttributeKey.getBusinessObjectDataAttributeName().trim());
    }

    /**
     * Validates the business object data notification registration key. This method also trims the key parameters.
     *
     * @param businessObjectDataNotificationKey the business object data notification registration key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectDataNotificationRegistrationKey(NotificationRegistrationKey businessObjectDataNotificationKey)
        throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(businessObjectDataNotificationKey, "A business object data notification registration key must be specified.");
        Assert.hasText(businessObjectDataNotificationKey.getNamespace(), "A namespace must be specified.");
        Assert.hasText(businessObjectDataNotificationKey.getNotificationName(), "A notification name must be specified.");

        // Remove leading and trailing spaces.
        businessObjectDataNotificationKey.setNamespace(businessObjectDataNotificationKey.getNamespace().trim());
        businessObjectDataNotificationKey.setNotificationName(businessObjectDataNotificationKey.getNotificationName().trim());
    }

    /**
     * Validates the EMR cluster definition key. This method also trims the key parameters.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateEmrClusterDefinitionKey(EmrClusterDefinitionKey emrClusterDefinitionKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(emrClusterDefinitionKey, "A partition key group key must be specified.");
        Assert.hasText(emrClusterDefinitionKey.getNamespace(), "A namespace must be specified.");
        Assert.hasText(emrClusterDefinitionKey.getEmrClusterDefinitionName(), "An EMR cluster definition name must be specified.");

        // Remove leading and trailing spaces.
        emrClusterDefinitionKey.setNamespace(emrClusterDefinitionKey.getNamespace().trim());
        emrClusterDefinitionKey.setEmrClusterDefinitionName(emrClusterDefinitionKey.getEmrClusterDefinitionName().trim());
    }

    /**
     * Validates an EMR cluster definition configuration.
     *
     * @param emrClusterDefinition the EMR cluster definition configuration
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateEmrClusterDefinitionConfiguration(EmrClusterDefinition emrClusterDefinition) throws IllegalArgumentException
    {
        Assert.notNull(emrClusterDefinition, "An EMR cluster definition configuration must be specified.");

        Assert.isTrue(StringUtils.isNotBlank(emrClusterDefinition.getSubnetId()), "Subnet ID must be specified");
        for (String token : emrClusterDefinition.getSubnetId().split(","))
        {
            Assert.isTrue(StringUtils.isNotBlank(token), "No blank is allowed in the list of subnet IDs");
        }

        Assert.notNull(emrClusterDefinition.getInstanceDefinitions(), "Instance definitions must be specified.");

        // Check master instances.
        Assert.notNull(emrClusterDefinition.getInstanceDefinitions().getMasterInstances(), "Master instances must be specified.");
        validateMasterInstanceDefinition(emrClusterDefinition.getInstanceDefinitions().getMasterInstances());

        // Check core instances.
        Assert.notNull(emrClusterDefinition.getInstanceDefinitions().getCoreInstances(), "Core instances must be specified.");
        validateInstanceDefinition("core", emrClusterDefinition.getInstanceDefinitions().getCoreInstances());

        // Check task instances
        if (emrClusterDefinition.getInstanceDefinitions().getTaskInstances() != null)
        {
            validateInstanceDefinition("task", emrClusterDefinition.getInstanceDefinitions().getTaskInstances());
        }

        // Check that total number of instances does not exceed the max allowed.
        int maxEmrInstanceCount = configurationHelper.getProperty(ConfigurationValue.MAX_EMR_INSTANCES_COUNT, Integer.class);
        if (maxEmrInstanceCount > 0)
        {
            int instancesRequested = emrClusterDefinition.getInstanceDefinitions().getMasterInstances().getInstanceCount() +
                emrClusterDefinition.getInstanceDefinitions().getCoreInstances().getInstanceCount();
            if (emrClusterDefinition.getInstanceDefinitions().getTaskInstances() != null)
            {
                instancesRequested += emrClusterDefinition.getInstanceDefinitions().getTaskInstances().getInstanceCount();
            }

            Assert.isTrue((maxEmrInstanceCount >= instancesRequested), "Total number of instances requested can not exceed : " + maxEmrInstanceCount);
        }

        // Validate node tags including checking for required tags and detecting any duplicate node tag names in case sensitive manner.
        Assert.notEmpty(emrClusterDefinition.getNodeTags(), "Node tags must be specified.");
        HashSet<String> nodeTagNameValidationSet = new HashSet<>();
        for (NodeTag nodeTag : emrClusterDefinition.getNodeTags())
        {
            Assert.hasText(nodeTag.getTagName(), "A node tag name must be specified.");
            Assert.hasText(nodeTag.getTagValue(), "A node tag value must be specified.");
            Assert.isTrue(!nodeTagNameValidationSet.contains(nodeTag.getTagName()), String.format("Duplicate node tag \"%s\" is found.", nodeTag.getTagName()));
            nodeTagNameValidationSet.add(nodeTag.getTagName());
        }

        // Validate the mandatory AWS tags are there
        for (String mandatoryTag : herdStringHelper.splitStringWithDefaultDelimiter(configurationHelper.getProperty(ConfigurationValue.MANDATORY_AWS_TAGS)))
        {
            Assert.isTrue(nodeTagNameValidationSet.contains(mandatoryTag), String.format("Mandatory AWS tag not specified: \"%s\"", mandatoryTag));
        }
    }

    /**
     * Converts the given master instance definition to a generic instance definition and delegates to validateInstanceDefinition(). Generates an appropriate
     * error message using the name "master".
     *
     * @param masterInstanceDefinition the master instance definition to validate
     *
     * @throws IllegalArgumentException when any validation error occurs
     */
    private void validateMasterInstanceDefinition(MasterInstanceDefinition masterInstanceDefinition)
    {
        InstanceDefinition instanceDefinition = new InstanceDefinition();
        instanceDefinition.setInstanceCount(masterInstanceDefinition.getInstanceCount());
        instanceDefinition.setInstanceMaxSearchPrice(masterInstanceDefinition.getInstanceMaxSearchPrice());
        instanceDefinition.setInstanceOnDemandThreshold(masterInstanceDefinition.getInstanceOnDemandThreshold());
        instanceDefinition.setInstanceSpotPrice(masterInstanceDefinition.getInstanceSpotPrice());
        instanceDefinition.setInstanceType(masterInstanceDefinition.getInstanceType());
        validateInstanceDefinition("master", instanceDefinition);
    }

    /**
     * Validates the given instance definition. Generates an appropriate error message using the given name. The name specified is one of "master", "core", or
     * "task".
     *
     * @param name name of instance group
     * @param instanceDefinition the instance definition to validate
     *
     * @throws IllegalArgumentException when any validation error occurs
     */
    private void validateInstanceDefinition(String name, InstanceDefinition instanceDefinition)
    {
        String capitalizedName = StringUtils.capitalize(name);

        Assert.isTrue(instanceDefinition.getInstanceCount() >= 1, "At least 1 " + name + " instance must be specified.");
        Assert.hasText(instanceDefinition.getInstanceType(), "An instance type for " + name + " instances must be specified.");

        if (instanceDefinition.getInstanceSpotPrice() != null)
        {
            Assert.isNull(instanceDefinition.getInstanceMaxSearchPrice(),
                capitalizedName + " instance max search price must not be specified when instance spot price is specified.");

            Assert.isTrue(instanceDefinition.getInstanceSpotPrice().compareTo(BigDecimal.ZERO) > 0,
                capitalizedName + " instance spot price must be greater than 0");
        }

        if (instanceDefinition.getInstanceMaxSearchPrice() != null)
        {
            Assert.isNull(instanceDefinition.getInstanceSpotPrice(),
                capitalizedName + " instance spot price must not be specified when max search price is specified.");

            Assert.isTrue(instanceDefinition.getInstanceMaxSearchPrice().compareTo(BigDecimal.ZERO) > 0,
                capitalizedName + " instance max search price must be greater than 0");

            if (instanceDefinition.getInstanceOnDemandThreshold() != null)
            {
                Assert.isTrue(instanceDefinition.getInstanceOnDemandThreshold().compareTo(BigDecimal.ZERO) > 0,
                    capitalizedName + " instance on-demand threshold must be greater than 0");
            }
        }
        else
        {
            Assert.isNull(instanceDefinition.getInstanceOnDemandThreshold(),
                capitalizedName + " instance on-demand threshold must not be specified when instance max search price is not specified.");
        }
    }

    /**
     * Returns the number of elements in this collection or 0 if the supplied Collection is {@code null}.
     *
     * @return the number of elements in this collection
     */
    public int getCollectionSize(Collection<?> collection)
    {
        return (collection == null ? 0 : collection.size());
    }

    /**
     * Replaces all null values in the specified list with empty strings.
     *
     * @param list the list of strings
     */
    public void replaceAllNullsWithEmptyString(List<String> list)
    {
        for (int i = 0; i < list.size(); i++)
        {
            if (list.get(i) == null)
            {
                list.set(i, "");
            }
        }
    }

    /**
     * Validates that parameter names are there and that there are no duplicate parameter names in case insensitive manner. This method also trims parameter
     * names.
     *
     * @param parameters the list of parameters to be validated
     */
    public void validateParameters(List<Parameter> parameters)
    {
        if (!CollectionUtils.isEmpty(parameters))
        {
            Set<String> parameterNameValidationSet = new HashSet<>();
            for (Parameter parameter : parameters)
            {
                // Validate and trim the parameter name.
                Assert.hasText(parameter.getName(), "A parameter name must be specified.");
                parameter.setName(parameter.getName().trim());

                // Ensure the parameter name isn't a duplicate by using a set with a "lowercase" name as the key for case insensitivity.
                String lowercaseParameterName = parameter.getName().toLowerCase();
                Assert.isTrue(!parameterNameValidationSet.contains(lowercaseParameterName), "Duplicate parameter name found: " + parameter.getName());
                parameterNameValidationSet.add(lowercaseParameterName);
            }
        }
    }

    /**
     * Gets the parameter value if found or defaults to the relative configuration setting value.
     *
     * @param parameters the map of parameters
     * @param configurationValue the configuration value
     *
     * @return the parameter value if found, the relative configuration setting value otherwise
     */
    public String getParameterValue(Map<String, String> parameters, ConfigurationValue configurationValue)
    {
        String parameterName = configurationValue.getKey().toLowerCase();
        String parameterValue;

        if (parameters.containsKey(parameterName))
        {
            parameterValue = parameters.get(parameterName);
        }
        else
        {
            parameterValue = configurationHelper.getProperty(configurationValue);
        }

        return parameterValue;
    }

    /**
     * Parses the parameter value as a signed decimal integer.
     *
     * @param parameter the string value to be parsed
     *
     * @return the integer value represented by the parameter value in decimal
     * @throws IllegalArgumentException if the parameter value does not contain a parsable integer
     */
    public int getParameterValueAsInteger(Parameter parameter) throws IllegalArgumentException
    {
        return getParameterValueAsInteger(parameter.getName(), parameter.getValue());
    }

    /**
     * Gets the parameter value if found or defaults to the relative configuration setting value. The parameter value is parsed as a signed decimal integer.
     *
     * @param parameters the map of parameters
     * @param configurationValue the configuration value
     *
     * @return the integer value represented by the parameter value in decimal
     * @throws IllegalArgumentException if the parameter value does not contain a parsable integer
     */
    public int getParameterValueAsInteger(Map<String, String> parameters, ConfigurationValue configurationValue) throws IllegalArgumentException
    {
        return getParameterValueAsInteger(configurationValue.getKey(), getParameterValue(parameters, configurationValue));
    }

    /**
     * Parses the parameter value as a signed decimal integer.
     *
     * @param parameterName the parameter name
     * @param parameterValue the string parameter value to be parsed
     *
     * @return the integer value represented by the parameter value in decimal
     * @throws IllegalArgumentException if the parameter value does not contain a parsable integer
     */
    private int getParameterValueAsInteger(String parameterName, String parameterValue) throws IllegalArgumentException
    {
        try
        {
            return Integer.parseInt(parameterValue);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException(String.format("Parameter \"%s\" specifies a non-integer value \"%s\".", parameterName, parameterValue), e);
        }
    }

    /**
     * Masks the value of the given parameter if the parameter is a password. If the parameter has the name which contains the string "password"
     * case-insensitive, the value will be replaced with ****.
     *
     * @param parameter {@link Parameter} to mask
     */
    public void maskPassword(Parameter parameter)
    {
        if (parameter.getName() != null)
        {
            String name = parameter.getName().toUpperCase();
            if (name.contains("PASSWORD"))
            {
                parameter.setValue("****");
            }
        }
    }
}
