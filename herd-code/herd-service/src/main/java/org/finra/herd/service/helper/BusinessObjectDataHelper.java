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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusHistoryEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity;
import org.finra.herd.model.jpa.StorageAttributeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.BusinessObjectDataService;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.SqsNotificationEventService;

/**
 * A helper class for BusinessObjectDataService related code.
 */
@Component
public class BusinessObjectDataHelper
{
    private static final String ENVIRONMENT_TOKEN_CODE = "environment";
    private static final String NAMESPACE_TOKEN_CODE = "namespace";
    private static final String DATA_PROVIDER_NAME_TOKEN_CODE = "dataProviderName";
    private static final String BUSINESS_OBJECT_DEFINITION_NAME_TOKEN_CODE = "businessObjectDefinitionName";
    private static final String BUSINESS_OBJECT_FORMAT_USAGE_TOKEN_CODE = "businessObjectFormatUsage";
    private static final String BUSINESS_OBJECT_FORMAT_FILE_TYPE_TOKEN_CODE = "businessObjectFormatFileType";
    private static final String BUSINESS_OBJECT_FORMAT_VERSION_TOKEN_CODE = "businessObjectFormatVersion";
    private static final String BUSINESS_OBJECT_FORMAT_PARTITION_KEY_TOKEN_CODE = "businessObjectFormatPartitionKey";
    private static final String BUSINESS_OBJECT_DATA_PARTITION_VALUE_TOKEN_CODE = "businessObjectDataPartitionValue";
    private static final String BUSINESS_OBJECT_DATA_SUBPARTITION_VALUE_TOKEN_CODE = "businessObjectDataSubPartitionValue";
    private static final String BUSINESS_OBJECT_DATA_VERSION_TOKEN_CODE = "businessObjectDataVersion";

    private static final List<String> NULL_VALUE_LIST = Arrays.asList((String) null);

    @Autowired
    private HerdHelper herdHelper;

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdDaoHelper herdDaoHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private SqsNotificationEventService sqsNotificationEventService;

    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    /**
     * Returns the S3 object key prefix based on the given format and data. This S3 key prefix is the standard S3 object key prefix that should be used for any
     * herd-managed S3 buckets. If the {@link ConfigurationValue#S3_KEY_PREFIX_TEMPLATE} is not configured, the default format will be used.
     * <p/>
     * The default format is ~namespace~/~dataProviderName~/~businessObjectFormatUsage~/~businessObjectFormatFileType~/~businessObjectDefinitionName~
     * /frmt-v~businessObjectFormatVersion~/data-v~businessObjectDataVersion~/~businessObjectFormatPartitionKey~=~businessObjectDataPartitionValue~
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param businessObjectDataKey the business object data key
     *
     * @return The S3 object key prefix
     */
    public String buildS3KeyPrefix(BusinessObjectFormatEntity businessObjectFormatEntity, BusinessObjectDataKey businessObjectDataKey)
    {
        BusinessObjectFormat businessObjectFormat = null;
        // Validate that subpartition are specified in format.
        if (!CollectionUtils.isEmpty(businessObjectDataKey.getSubPartitionValues()))
        {
            // Get business object format model object to directly access schema columns and partitions.
            businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

            // Validate that we have subpartitions specified in the business object format schema.
            Assert.notNull(businessObjectFormat.getSchema(), String
                .format("Schema must be defined when using subpartition values for business object format {%s}.",
                    herdDaoHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));

            Assert.isTrue(businessObjectFormat.getSchema().getPartitions().size() > businessObjectDataKey.getSubPartitionValues().size(),
                String.format("Number of subpartition values specified for the business object data is greater than or equal to " +
                    "the number of partition columns defined in the schema for the associated business object format." +
                    "Business object data: {%s}", herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Set the token delimiter based on the environment configuration.
        String tokenDelimiter = configurationHelper.getProperty(ConfigurationValue.TEMPLATE_TOKEN_DELIMITER);

        // Setup the individual token names (using the configured delimiter).
        String namespaceToken = tokenDelimiter + NAMESPACE_TOKEN_CODE + tokenDelimiter;
        String dataProviderNameToken = tokenDelimiter + DATA_PROVIDER_NAME_TOKEN_CODE + tokenDelimiter;
        String businessObjectDefinitionNameToken = tokenDelimiter + BUSINESS_OBJECT_DEFINITION_NAME_TOKEN_CODE + tokenDelimiter;
        String businessObjectFormatUsageToken = tokenDelimiter + BUSINESS_OBJECT_FORMAT_USAGE_TOKEN_CODE + tokenDelimiter;
        String businessObjectFormatFileTypeToken = tokenDelimiter + BUSINESS_OBJECT_FORMAT_FILE_TYPE_TOKEN_CODE + tokenDelimiter;
        String businessObjectFormatVersionToken = tokenDelimiter + BUSINESS_OBJECT_FORMAT_VERSION_TOKEN_CODE + tokenDelimiter;
        String businessObjectFormatPartitionKeyToken = tokenDelimiter + BUSINESS_OBJECT_FORMAT_PARTITION_KEY_TOKEN_CODE + tokenDelimiter;
        String businessObjectDataPartitionValueToken = tokenDelimiter + BUSINESS_OBJECT_DATA_PARTITION_VALUE_TOKEN_CODE + tokenDelimiter;
        List<String> subPartitionValueTokens = new ArrayList<>();
        int subPartitionValuesCount = herdHelper.getCollectionSize(businessObjectDataKey.getSubPartitionValues());
        for (int i = 0; i < subPartitionValuesCount; i++)
        {
            subPartitionValueTokens.add(tokenDelimiter + BUSINESS_OBJECT_DATA_SUBPARTITION_VALUE_TOKEN_CODE + i + tokenDelimiter);
        }
        String businessObjectDataVersionToken = tokenDelimiter + BUSINESS_OBJECT_DATA_VERSION_TOKEN_CODE + tokenDelimiter;

        // Set the default S3 key prefix tokenized template.
        //    ~namespace~/~dataProviderName~/~businessObjectFormatUsage~/~businessObjectFormatFileType~/~businessObjectDefinitionName~/
        //    frmt-v~businessObjectFormatVersion~/data-v~businessObjectDataVersion~/~businessObjectFormatPartitionKey~=~businessObjectDataPartitionValue~
        String defaultS3KeyPrefixTemplate = namespaceToken + "/" + dataProviderNameToken + "/" + businessObjectFormatUsageToken + "/" +
            businessObjectFormatFileTypeToken + "/" + businessObjectDefinitionNameToken + "/frmt-v" + businessObjectFormatVersionToken +
            "/data-v" + businessObjectDataVersionToken + "/" + businessObjectFormatPartitionKeyToken + "=" + businessObjectDataPartitionValueToken;

        // Get the S3 key prefix template from the environment, but use the default if one isn't configured.
        // This gives us the ability to customize/change the format post deployment.
        String s3KeyPrefixTemplate = configurationHelper.getProperty(ConfigurationValue.S3_KEY_PREFIX_TEMPLATE);

        if (s3KeyPrefixTemplate == null)
        {
            s3KeyPrefixTemplate = defaultS3KeyPrefixTemplate;
        }

        StringBuilder s3KeyPrefixTemplateStringBuilder = new StringBuilder(s3KeyPrefixTemplate);

        for (int i = 0; i < subPartitionValueTokens.size(); i++)
        {   // TODO: Add some guards against null pointer exceptions when businessObjectFormat or getSchema or getPartitions could be null.
            s3KeyPrefixTemplateStringBuilder.append('/')
                .append(herdHelper.s3KeyPrefixFormat(businessObjectFormat.getSchema().getPartitions().get(i + 1).getName())).append('=').
                append(businessObjectDataKey.getSubPartitionValues().get(i));
        }

        return buildS3KeyPrefixHelper(s3KeyPrefixTemplateStringBuilder.toString(), tokenDelimiter, businessObjectFormatEntity, businessObjectDataKey);
    }

    /**
     * Returns S3 key prefix constructed for the file upload.
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param businessObjectDataKey the business object data key
     *
     * @return the S3 key prefix
     */
    public String buildFileUploadS3KeyPrefix(BusinessObjectFormatEntity businessObjectFormatEntity, BusinessObjectDataKey businessObjectDataKey)
    {
        // Set the token delimiter based on the environment configuration.
        String tokenDelimiter = configurationHelper.getProperty(ConfigurationValue.TEMPLATE_TOKEN_DELIMITER);

        // Setup the individual token names (using the configured delimiter).
        String environmentToken = tokenDelimiter + ENVIRONMENT_TOKEN_CODE + tokenDelimiter;
        String namespaceToken = tokenDelimiter + NAMESPACE_TOKEN_CODE + tokenDelimiter;
        String businessObjectDataPartitionValueToken = tokenDelimiter + BUSINESS_OBJECT_DATA_PARTITION_VALUE_TOKEN_CODE + tokenDelimiter;

        // Set the default S3 key prefix tokenized template for the file upload.
        //    ~environment~/~namespace~/~businessObjectDataPartitionValue~
        String defaultS3KeyPrefixTemplate = environmentToken + "/" + namespaceToken + "/" + businessObjectDataPartitionValueToken;

        // Get the file upload specific S3 key prefix template from the environment, but use the default if one isn't configured.
        // This gives us the ability to customize/change the format post deployment.
        String s3KeyPrefixTemplate = configurationHelper.getProperty(ConfigurationValue.FILE_UPLOAD_S3_KEY_PREFIX_TEMPLATE);

        if (s3KeyPrefixTemplate == null)
        {
            s3KeyPrefixTemplate = defaultS3KeyPrefixTemplate;
        }

        return buildS3KeyPrefixHelper(s3KeyPrefixTemplate, tokenDelimiter, businessObjectFormatEntity, businessObjectDataKey);
    }

    /**
     * Returns S3 key prefix constructed per specified s3KeyPrefixTemplate.
     *
     * @param s3KeyPrefixTemplate the S3 key prefix tokenized template
     * @param tokenDelimiter the token delimiter used in the S3 key prefix template
     * @param businessObjectFormatEntity the business object format entity
     * @param businessObjectDataKey the business object data key
     *
     * @return the S3 key prefix
     */
    private String buildS3KeyPrefixHelper(String s3KeyPrefixTemplate, String tokenDelimiter, BusinessObjectFormatEntity businessObjectFormatEntity,
        BusinessObjectDataKey businessObjectDataKey)
    {
        // Setup the individual token names (using the specified delimiter).
        String environmentToken = tokenDelimiter + ENVIRONMENT_TOKEN_CODE + tokenDelimiter;
        String namespaceToken = tokenDelimiter + NAMESPACE_TOKEN_CODE + tokenDelimiter;
        String dataProviderNameToken = tokenDelimiter + DATA_PROVIDER_NAME_TOKEN_CODE + tokenDelimiter;
        String businessObjectDefinitionNameToken = tokenDelimiter + BUSINESS_OBJECT_DEFINITION_NAME_TOKEN_CODE + tokenDelimiter;
        String businessObjectFormatUsageToken = tokenDelimiter + BUSINESS_OBJECT_FORMAT_USAGE_TOKEN_CODE + tokenDelimiter;
        String businessObjectFormatFileTypeToken = tokenDelimiter + BUSINESS_OBJECT_FORMAT_FILE_TYPE_TOKEN_CODE + tokenDelimiter;
        String businessObjectFormatVersionToken = tokenDelimiter + BUSINESS_OBJECT_FORMAT_VERSION_TOKEN_CODE + tokenDelimiter;
        String businessObjectFormatPartitionKeyToken = tokenDelimiter + BUSINESS_OBJECT_FORMAT_PARTITION_KEY_TOKEN_CODE + tokenDelimiter;
        String businessObjectDataPartitionValueToken = tokenDelimiter + BUSINESS_OBJECT_DATA_PARTITION_VALUE_TOKEN_CODE + tokenDelimiter;
        List<String> subPartitionValueTokens = new ArrayList<>();
        int subPartitionValuesCount = herdHelper.getCollectionSize(businessObjectDataKey.getSubPartitionValues());
        for (int i = 0; i < subPartitionValuesCount; i++)
        {
            subPartitionValueTokens.add(tokenDelimiter + BUSINESS_OBJECT_DATA_SUBPARTITION_VALUE_TOKEN_CODE + i + tokenDelimiter);
        }
        String businessObjectDataVersionToken = tokenDelimiter + BUSINESS_OBJECT_DATA_VERSION_TOKEN_CODE + tokenDelimiter;

        // Set the token delimiter based on the environment configuration.
        String environmentName = configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT);

        // Populate a map with the tokens mapped to actual database values.
        Map<String, String> pathToTokenValueMap = new HashMap<>();
        pathToTokenValueMap.put(environmentToken, herdHelper.s3KeyPrefixFormat(environmentName));
        pathToTokenValueMap
            .put(namespaceToken, herdHelper.s3KeyPrefixFormat(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode()));
        pathToTokenValueMap
            .put(dataProviderNameToken, herdHelper.s3KeyPrefixFormat(businessObjectFormatEntity.getBusinessObjectDefinition().getDataProvider().getName()));
        pathToTokenValueMap
            .put(businessObjectDefinitionNameToken, herdHelper.s3KeyPrefixFormat(businessObjectFormatEntity.getBusinessObjectDefinition().getName()));
        pathToTokenValueMap.put(businessObjectFormatUsageToken, herdHelper.s3KeyPrefixFormat(businessObjectFormatEntity.getUsage()));
        pathToTokenValueMap.put(businessObjectFormatFileTypeToken, herdHelper.s3KeyPrefixFormat(businessObjectFormatEntity.getFileType().getCode()));
        pathToTokenValueMap.put(businessObjectFormatVersionToken, String.valueOf(businessObjectFormatEntity.getBusinessObjectFormatVersion()));
        pathToTokenValueMap.put(businessObjectFormatPartitionKeyToken, herdHelper.s3KeyPrefixFormat(businessObjectFormatEntity.getPartitionKey()));
        pathToTokenValueMap.put(businessObjectDataPartitionValueToken, businessObjectDataKey.getPartitionValue());

        for (int i = 0; i < subPartitionValueTokens.size(); i++)
        {
            pathToTokenValueMap.put(subPartitionValueTokens.get(i), businessObjectDataKey.getSubPartitionValues().get(i));
        }

        pathToTokenValueMap.put(businessObjectDataVersionToken, String.valueOf(businessObjectDataKey.getBusinessObjectDataVersion()));

        // Substitute the tokens with the actual database values.
        String s3KeyPrefix = s3KeyPrefixTemplate;
        for (Map.Entry<String, String> mapEntry : pathToTokenValueMap.entrySet())
        {
            s3KeyPrefix = s3KeyPrefix.replaceAll(mapEntry.getKey(), mapEntry.getValue());
        }

        return s3KeyPrefix;
    }

    /**
     * Builds a list of partition values from the partition value filter. The partition range takes precedence over the list of partition values in the filter.
     * If a range is specified the list of values will come from the expected partition values table for values within the specified range. If the list is
     * specified, duplicates will be removed. In both cases, the list will be ordered ascending.
     *
     * @param partitionValueFilter the partition value filter that was validated to have exactly one partition value filter option
     * @param partitionKey the partition key
     * @param partitionColumnPosition the partition column position (one-based numbering)
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectDataVersion the business object data version
     * @param storageNames the list of storage names
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the unique and sorted partition value list
     */
    public List<String> getPartitionValues(PartitionValueFilter partitionValueFilter, String partitionKey, int partitionColumnPosition,
        BusinessObjectFormatKey businessObjectFormatKey, Integer businessObjectDataVersion, List<String> storageNames,
        BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        List<String> partitionValues = new ArrayList<>();

        if (partitionValueFilter.getPartitionValueRange() != null)
        {
            // A "partition value range" filter option is specified.
            partitionValues = processPartitionValueRangeFilterOption(partitionValueFilter.getPartitionValueRange(), businessObjectFormatEntity);
        }
        else if (partitionValueFilter.getPartitionValues() != null)
        {
            // A "partition value list" filter option is specified.
            partitionValues =
                processPartitionValueListFilterOption(partitionValueFilter.getPartitionValues(), partitionKey, partitionColumnPosition, businessObjectFormatKey,
                    businessObjectDataVersion, storageNames);
        }
        else if (partitionValueFilter.getLatestBeforePartitionValue() != null)
        {
            // A "latest before partition value" filter option is specified.

            // Retrieve the maximum partition value before (inclusive) the specified partition value.
            // If a business object data version isn't specified, the latest VALID business object data version will be used.
            String maxPartitionValue = herdDao
                .getBusinessObjectDataMaxPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion,
                    BusinessObjectDataStatusEntity.VALID, storageNames, partitionValueFilter.getLatestBeforePartitionValue().getPartitionValue(), null);
            if (maxPartitionValue != null)
            {
                partitionValues.add(maxPartitionValue);
            }
            else
            {
                throw new ObjectNotFoundException(
                    getLatestPartitionValueNotFoundErrorMessage("before", partitionValueFilter.getLatestBeforePartitionValue().getPartitionValue(),
                        partitionKey, businessObjectFormatKey, businessObjectDataVersion, storageNames));
            }
        }
        else
        {
            // A "latest after partition value" filter option is specified.

            // Retrieve the maximum partition value before (inclusive) the specified partition value.
            // If a business object data version isn't specified, the latest VALID business object data version will be used.
            String maxPartitionValue = herdDao
                .getBusinessObjectDataMaxPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion,
                    BusinessObjectDataStatusEntity.VALID, storageNames, null, partitionValueFilter.getLatestAfterPartitionValue().getPartitionValue());
            if (maxPartitionValue != null)
            {
                partitionValues.add(maxPartitionValue);
            }
            else
            {
                throw new ObjectNotFoundException(
                    getLatestPartitionValueNotFoundErrorMessage("after", partitionValueFilter.getLatestAfterPartitionValue().getPartitionValue(), partitionKey,
                        businessObjectFormatKey, businessObjectDataVersion, storageNames));
            }
        }

        // If a max partition values limit has been set, check the limit and throw an exception if the limit has been exceeded.
        Integer availabilityDdlMaxPartitionValues = configurationHelper.getProperty(ConfigurationValue.AVAILABILITY_DDL_MAX_PARTITION_VALUES, Integer.class);
        if ((availabilityDdlMaxPartitionValues != null) && (partitionValues.size() > availabilityDdlMaxPartitionValues))
        {
            throw new IllegalArgumentException(
                "The number of partition values (" + partitionValues.size() + ") exceeds the system limit of " + availabilityDdlMaxPartitionValues + ".");
        }

        // Return the partition values.
        return partitionValues;
    }

    /**
     * Builds a list of partition values from a "partition value range" partition value filter option. The list of partition values will come from the expected
     * partition values table for values within the specified range. The list will be ordered ascending.
     *
     * @param partitionValueRange the partition value range
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the unique and sorted partition value list
     */
    private List<String> processPartitionValueRangeFilterOption(PartitionValueRange partitionValueRange, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        List<String> resultPartitionValues = new ArrayList<>();

        Assert.notNull(businessObjectFormatEntity.getPartitionKeyGroup(), String
            .format("A partition key group, which is required to use partition value ranges, is not specified for the business object format {%s}.",
                herdDaoHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));

        List<ExpectedPartitionValueEntity> expectedPartitionValueEntities = herdDao
            .getExpectedPartitionValuesByGroupAndRange(businessObjectFormatEntity.getPartitionKeyGroup().getPartitionKeyGroupName(), partitionValueRange);

        // Populate the partition values returned from the range query.
        for (ExpectedPartitionValueEntity expectedPartitionValueEntity : expectedPartitionValueEntities)
        {
            String partitionValue = expectedPartitionValueEntity.getPartitionValue();

            // Validate that expected partition value does not match to one of the partition value tokens.
            Assert.isTrue(!partitionValue.equals(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN) &&
                !partitionValue.equals(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN),
                "A partition value token cannot be specified as one of the expected partition values.");

            resultPartitionValues.add(partitionValue);
        }

        // Validate that our partition value range results in a non-empty partition value list.
        Assert.notEmpty(resultPartitionValues, String
            .format("Partition value range [\"%s\", \"%s\"] contains no valid partition values.", partitionValueRange.getStartPartitionValue(),
                partitionValueRange.getEndPartitionValue()));

        return resultPartitionValues;
    }

    /**
     * Builds a list of partition values from a "partition value list" partition value filter option. Duplicates will be removed and the list will be ordered
     * ascending.
     *
     * @param partitionValues the partition values passed in the partition value list filter option
     * @param partitionKey the partition key
     * @param partitionColumnPosition the partition column position (one-based numbering)
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectDataVersion the business object data version
     * @param storageNames the list of storage names
     *
     * @return the unique and sorted partition value list
     */
    private List<String> processPartitionValueListFilterOption(List<String> partitionValues, String partitionKey, int partitionColumnPosition,
        BusinessObjectFormatKey businessObjectFormatKey, Integer businessObjectDataVersion, List<String> storageNames)
    {
        List<String> resultPartitionValues = new ArrayList<>();

        // Remove any duplicates and sort the request partition values.
        List<String> uniqueAndSortedPartitionValues = new ArrayList<>();
        uniqueAndSortedPartitionValues.addAll(new TreeSet<>(partitionValues));

        // This flag to be used to indicate if we updated partition value list per special partition value tokens.
        boolean partitionValueListUpdated = false;

        // If the maximum partition value token is specified, substitute special partition value token with the actual partition value.
        // If a business object data version isn't specified, the latest VALID business object data version will be used.
        if (uniqueAndSortedPartitionValues.contains(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN))
        {
            String maxPartitionValue = herdDao
                .getBusinessObjectDataMaxPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion,
                    BusinessObjectDataStatusEntity.VALID, storageNames, null, null);
            if (maxPartitionValue == null)
            {
                throw new ObjectNotFoundException(
                    getPartitionValueNotFoundErrorMessage("maximum", partitionKey, businessObjectFormatKey, businessObjectDataVersion, storageNames));
            }
            uniqueAndSortedPartitionValues.remove(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN);
            uniqueAndSortedPartitionValues.add(maxPartitionValue);
            partitionValueListUpdated = true;
        }

        // If the minimum partition value token is specified, substitute special partition value token with the actual partition value.
        // If a business object data version isn't specified, the latest VALID business object data version will be used.
        if (uniqueAndSortedPartitionValues.contains(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN))
        {
            String minPartitionValue = herdDao
                .getBusinessObjectDataMinPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion,
                    BusinessObjectDataStatusEntity.VALID, storageNames);
            if (minPartitionValue == null)
            {
                throw new ObjectNotFoundException(
                    getPartitionValueNotFoundErrorMessage("minimum", partitionKey, businessObjectFormatKey, businessObjectDataVersion, storageNames));
            }
            uniqueAndSortedPartitionValues.remove(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN);
            uniqueAndSortedPartitionValues.add(minPartitionValue);
            partitionValueListUpdated = true;
        }

        // Build the final list of partition values.
        if (partitionValueListUpdated)
        {
            // Remove any duplicates and sort the list of partition values is we updated the list per special partition value tokens.
            resultPartitionValues.addAll(new TreeSet<>(uniqueAndSortedPartitionValues));
        }
        else
        {
            resultPartitionValues.addAll(uniqueAndSortedPartitionValues);
        }

        return resultPartitionValues;
    }

    /**
     * Validates the business object data keys. This will validate, trim, and make lowercase appropriate fields.
     *
     * @param businessObjectDataKeys the keys to validate.
     */
    public void validateBusinessObjectDataKeys(List<BusinessObjectDataKey> businessObjectDataKeys)
    {
        // Create a cloned business object data keys list where all keys are lowercase. This will be used for ensuring no duplicates are present in a
        // case-insensitive way.
        List<BusinessObjectDataKey> businessObjectDataLowercaseKeys = new ArrayList<>();

        if (businessObjectDataKeys != null)
        {
            for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
            {
                // Verify that appropriate fields have text.
                Assert.hasText(businessObjectDataKey.getNamespace(), "A namespace must be specified.");
                Assert.hasText(businessObjectDataKey.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
                Assert.hasText(businessObjectDataKey.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
                Assert.hasText(businessObjectDataKey.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
                Assert.notNull(businessObjectDataKey.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
                Assert.hasText(businessObjectDataKey.getPartitionValue(), "A business object data partition value must be specified.");
                Assert.notNull(businessObjectDataKey.getBusinessObjectDataVersion(), "A business object data version must be specified.");

                // Remove leading and trailing spaces.
                businessObjectDataKey.setNamespace(businessObjectDataKey.getNamespace().trim());
                businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectDataKey.getBusinessObjectDefinitionName().trim());
                businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectDataKey.getBusinessObjectFormatUsage().trim());
                businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectDataKey.getBusinessObjectFormatFileType().trim());
                businessObjectDataKey.setPartitionValue(businessObjectDataKey.getPartitionValue().trim());

                for (int i = 0; i < herdHelper.getCollectionSize(businessObjectDataKey.getSubPartitionValues()); i++)
                {
                    Assert.hasText(businessObjectDataKey.getSubPartitionValues().get(i), "A subpartition value must be specified.");
                    businessObjectDataKey.getSubPartitionValues().set(i, businessObjectDataKey.getSubPartitionValues().get(i).trim());
                }

                // Add a lowercase clone to the lowercase key list.
                businessObjectDataLowercaseKeys.add(cloneToLowerCase(businessObjectDataKey));
            }
        }

        // Check for duplicates by ensuring the lowercase list size and the hash set (removes duplicates) size are the same.
        if (businessObjectDataLowercaseKeys.size() != new HashSet<>(businessObjectDataLowercaseKeys).size())
        {
            throw new IllegalArgumentException("Business object data keys can not contain duplicates.");
        }
    }

    /**
     * Returns a cloned version of the specified business object data key where all fields are made lowercase.
     *
     * @param businessObjectDataKey the business object data.
     *
     * @return the cloned business object data.
     */
    public BusinessObjectDataKey cloneToLowerCase(BusinessObjectDataKey businessObjectDataKey)
    {
        BusinessObjectDataKey businessObjectDataKeyClone = new BusinessObjectDataKey();

        businessObjectDataKeyClone.setNamespace(businessObjectDataKey.getNamespace().toLowerCase());
        businessObjectDataKeyClone.setBusinessObjectDefinitionName(businessObjectDataKey.getBusinessObjectDefinitionName().toLowerCase());
        businessObjectDataKeyClone.setBusinessObjectFormatUsage(businessObjectDataKey.getBusinessObjectFormatUsage().toLowerCase());
        businessObjectDataKeyClone.setBusinessObjectFormatFileType(businessObjectDataKey.getBusinessObjectFormatFileType().toLowerCase());
        businessObjectDataKeyClone.setBusinessObjectFormatVersion(businessObjectDataKey.getBusinessObjectFormatVersion());
        businessObjectDataKeyClone.setPartitionValue(businessObjectDataKey.getPartitionValue());
        businessObjectDataKeyClone.setBusinessObjectDataVersion(businessObjectDataKey.getBusinessObjectDataVersion());
        businessObjectDataKeyClone.setSubPartitionValues(businessObjectDataKey.getSubPartitionValues());

        return businessObjectDataKeyClone;
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
        businessObjectDataKey.setSubPartitionValues(herdHelper.getSubPartitionValues(businessObjectDataEntity));
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());
        return businessObjectDataKey;
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
     * Creates a new business object data from the request information.
     *
     * @param request the request
     *
     * @return the newly created and persisted business object data
     */
    public BusinessObjectData createBusinessObjectData(BusinessObjectDataCreateRequest request)
    {
        // By default, fileSize value is required.
        return createBusinessObjectData(request, true);
    }

    /**
     * Creates a new business object data from the request information.
     *
     * @param request the request
     * @param fileSizeRequired specifies if fileSizeBytes value is required or not
     *
     * @return the newly created and persisted business object data
     */
    public BusinessObjectData createBusinessObjectData(BusinessObjectDataCreateRequest request, boolean fileSizeRequired)
    {
        // Perform the validation.
        validateBusinessObjectDataCreateRequest(request, fileSizeRequired);

        // Get the status entity if status is specified else set it to VALID
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = herdDaoHelper
            .getBusinessObjectDataStatusEntity(StringUtils.isBlank(request.getStatus()) ? BusinessObjectDataStatusEntity.VALID : request.getStatus());

        // Get the business object format for the specified parameters and make sure it exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDaoHelper.getBusinessObjectFormatEntity(
            new BusinessObjectFormatKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()));

        validateAttributesAgainstFormat(request, businessObjectFormatEntity);

        // Ensure the specified partition key matches what's configured within the business object format.
        Assert.isTrue(businessObjectFormatEntity.getPartitionKey().equalsIgnoreCase(request.getPartitionKey()), String
            .format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", request.getPartitionKey(),
                businessObjectFormatEntity.getPartitionKey()));

        // Get the latest format version for this business object data, if it exists.
        BusinessObjectDataEntity existingBusinessObjectDataEntity = herdDao.getBusinessObjectDataByAltKey(
            new BusinessObjectDataKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion(), request.getPartitionValue(),
                request.getSubPartitionValues(), null));

        // Throw an error if this business object data already exists and createNewVersion flag is not set.
        if (existingBusinessObjectDataEntity != null && !Boolean.TRUE.equals(request.isCreateNewVersion()))
        {
            throw new AlreadyExistsException("Unable to create business object data because it already exists.");
        }

        // Create a business object data entity from the request information.
        // Please note that simply adding 1 to the latest version without "DB locking" is sufficient here,
        // even for multi-threading, since we are relying on the DB having version as part of the alternate key.
        Integer businessObjectDataVersion = existingBusinessObjectDataEntity == null ? BusinessObjectDataEntity.BUSINESS_OBJECT_DATA_INITIAL_VERSION :
            existingBusinessObjectDataEntity.getVersion() + 1;
        BusinessObjectDataEntity newVersionBusinessObjectDataEntity =
            createBusinessObjectDataEntity(request, businessObjectFormatEntity, businessObjectDataVersion, businessObjectDataStatusEntity);

        // Update the existing latest business object data version entity, so it would not be flagged as the latest version anymore.
        if (existingBusinessObjectDataEntity != null)
        {
            existingBusinessObjectDataEntity.setLatestVersion(Boolean.FALSE);
            herdDao.saveAndRefresh(existingBusinessObjectDataEntity);
        }

        // Add an entry to the business object data status history table.
        BusinessObjectDataStatusHistoryEntity businessObjectDataStatusHistoryEntity = new BusinessObjectDataStatusHistoryEntity();
        businessObjectDataStatusHistoryEntity.setBusinessObjectData(newVersionBusinessObjectDataEntity);
        businessObjectDataStatusHistoryEntity.setStatus(businessObjectDataStatusEntity);
        List<BusinessObjectDataStatusHistoryEntity> businessObjectDataStatusHistoryEntities = new ArrayList<>();
        businessObjectDataStatusHistoryEntities.add(businessObjectDataStatusHistoryEntity);
        newVersionBusinessObjectDataEntity.setHistoricalStatuses(businessObjectDataStatusHistoryEntities);

        // Persist the new entity.
        newVersionBusinessObjectDataEntity = herdDao.saveAndRefresh(newVersionBusinessObjectDataEntity);

        // Create a status change notification to be sent on create business object data event.
        sqsNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(herdDaoHelper.getBusinessObjectDataKey(newVersionBusinessObjectDataEntity),
                businessObjectDataStatusEntity.getCode(), null);

        // Create and return the business object data object from the persisted entity.
        return createBusinessObjectDataFromEntity(newVersionBusinessObjectDataEntity);
    }

    /**
     * Update the business object data status.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param status the status
     */
    public void updateBusinessObjectDataStatus(BusinessObjectDataEntity businessObjectDataEntity, String status)
    {
        // Retrieve and ensure the status is valid.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = herdDaoHelper.getBusinessObjectDataStatusEntity(status);

        // Save the current status value.
        String oldStatus = businessObjectDataEntity.getStatus().getCode();

        // Update the entity with the new values.
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

        // Add an entry to the business object data status history table
        BusinessObjectDataStatusHistoryEntity businessObjectDataStatusHistoryEntity = new BusinessObjectDataStatusHistoryEntity();
        businessObjectDataEntity.getHistoricalStatuses().add(businessObjectDataStatusHistoryEntity);
        businessObjectDataStatusHistoryEntity.setBusinessObjectData(businessObjectDataEntity);
        businessObjectDataStatusHistoryEntity.setStatus(businessObjectDataStatusEntity);

        // Persist the entity.
        herdDao.saveAndRefresh(businessObjectDataEntity);

        // Sent a business object data status change notification.
        sqsNotificationEventService.processBusinessObjectDataStatusChangeNotificationEvent(herdDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity),
            businessObjectDataStatusEntity.getCode(), oldStatus);
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
        businessObjectData.setSubPartitionValues(herdHelper.getSubPartitionValues(businessObjectDataEntity));
        businessObjectData.setStatus(businessObjectDataEntity.getStatus().getCode());
        businessObjectData.setVersion(businessObjectDataEntity.getVersion());
        businessObjectData.setLatestVersion(businessObjectDataEntity.getLatestVersion());

        // Add in the storage units.
        businessObjectData.setStorageUnits(createStorageUnitsFromEntities(businessObjectDataEntity.getStorageUnits()));

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

        return businessObjectData;
    }

    /**
     * Creates a list of storage units from the list of storage unit entities.
     *
     * @param storageUnitEntities the storage unit entities.
     *
     * @return the list of storage units.
     */
    private List<StorageUnit> createStorageUnitsFromEntities(Collection<StorageUnitEntity> storageUnitEntities)
    {
        List<StorageUnit> storageUnits = new ArrayList<>();

        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            StorageUnit storageUnit = new StorageUnit();
            storageUnits.add(storageUnit);

            Storage storage = new Storage();
            storageUnit.setStorage(storage);

            StorageEntity storageEntity = storageUnitEntity.getStorage();
            storage.setName(storageEntity.getName());
            storage.setStoragePlatformName(storageEntity.getStoragePlatform().getName());

            // Add the storage attributes.
            if (!CollectionUtils.isEmpty(storageEntity.getAttributes()))
            {
                List<Attribute> storageAttributes = new ArrayList<>();
                storage.setAttributes(storageAttributes);
                for (StorageAttributeEntity storageAttributeEntity : storageEntity.getAttributes())
                {
                    Attribute attribute = new Attribute();
                    storageAttributes.add(attribute);
                    attribute.setName(storageAttributeEntity.getName());
                    attribute.setValue(storageAttributeEntity.getValue());
                }
            }

            // Add the storage directory.
            if (storageUnitEntity.getDirectoryPath() != null)
            {
                StorageDirectory storageDirectory = new StorageDirectory();
                storageUnit.setStorageDirectory(storageDirectory);
                storageDirectory.setDirectoryPath(storageUnitEntity.getDirectoryPath());
            }

            // Add the storage files.
            if (!storageUnitEntity.getStorageFiles().isEmpty())
            {
                List<StorageFile> storageFiles = new ArrayList<>();
                storageUnit.setStorageFiles(storageFiles);

                for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
                {
                    storageFiles.add(storageFileHelper.createStorageFileFromEntity(storageFileEntity));
                }
            }
        }

        return storageUnits;
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
        storageUnitCreateRequest.setStorageFiles(Arrays.asList(new StorageFile(storageFilePath, storageFileSizeBytes, storageFileRowCount, null)));

        // Set the attributes if any are specified.
        request.setAttributes(attributes);

        // Since we are using UUID as our partition value, set the flag to only allow business object data initial version creation.
        request.setCreateNewVersion(false);

        return request;
    }

    /**
     * Build partition filters based on the specified partition value filters.  This method also validates the partition value filters (including partition
     * keys) against the business object format schema.  When request contains multiple partition value filters, the system will check business object data
     * availability for n-fold Cartesian product of the partition values specified, where n is a number of partition value filters (partition value sets).
     *
     * @param partitionValueFilters the list of partition value filters
     * @param standalonePartitionValueFilter the standalone partition value filter
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectDataVersion the business object data version
     * @param storageNames the list of storage names
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the list of partition filters
     */
    public List<List<String>> buildPartitionFilters(List<PartitionValueFilter> partitionValueFilters, PartitionValueFilter standalonePartitionValueFilter,
        BusinessObjectFormatKey businessObjectFormatKey, Integer businessObjectDataVersion, List<String> storageNames,
        BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        // Build a list of partition value filters to process based on the specified partition value filters.
        List<PartitionValueFilter> partitionValueFiltersToProcess = getPartitionValuesToProcess(partitionValueFilters, standalonePartitionValueFilter);

        // Build a map of column positions and the relative partition values.
        Map<Integer, List<String>> partitionValues = new HashMap<>();

        // Initialize the map with null partition values for all possible primary and sub-partition values. Partition column position uses one-based numbering.
        for (int i = 0; i < BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1; i++)
        {
            partitionValues.put(i, NULL_VALUE_LIST);
        }

        // Process all partition value filters one by one and populate the relative entries in the map.
        for (PartitionValueFilter partitionValueFilter : partitionValueFiltersToProcess)
        {
            // Get the partition key.  If partition key is not specified, use the primary partition column.
            String partitionKey = StringUtils.isNotBlank(partitionValueFilter.getPartitionKey()) ? partitionValueFilter.getPartitionKey() :
                businessObjectFormatEntity.getPartitionKey();

            // Get the partition column position (one-based numbering).
            int partitionColumnPosition = getPartitionColumnPosition(partitionKey, businessObjectFormatEntity);

            // Get unique and sorted list of partition values to check the availability for.
            List<String> uniqueAndSortedPartitionValues =
                getPartitionValues(partitionValueFilter, partitionKey, partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion,
                    storageNames, businessObjectFormatEntity);

            // Add this partition value filter to the map.
            List<String> previousPartitionValues = partitionValues.put(partitionColumnPosition - 1, uniqueAndSortedPartitionValues);

            // Check if this partition column has not been already added.
            if (!NULL_VALUE_LIST.equals(previousPartitionValues))
            {
                throw new IllegalArgumentException("Partition value filters specify duplicate partition columns.");
            }
        }

        // When request contains multiple partition value filters, the system will check business object data availability for n-fold Cartesian product
        // of the partition values specified, where n is a number of partition value filters (partition value sets).
        List<String[]> crossProductResult = getCrossProduct(partitionValues);
        List<List<String>> partitionFilters = new ArrayList<>();
        for (String[] crossProductRow : crossProductResult)
        {
            partitionFilters.add(Arrays.asList(crossProductRow));
        }

        return partitionFilters;
    }

    /**
     * Gets the partition values to process.
     *
     * @param partitionValueFilters the list of partition values.
     * @param standalonePartitionValueFilter the standalone partition value.
     *
     * @return the list of partition values to process.
     */
    private List<PartitionValueFilter> getPartitionValuesToProcess(List<PartitionValueFilter> partitionValueFilters,
        PartitionValueFilter standalonePartitionValueFilter)
    {
        // Build a list of partition value filters to process based on the specified partition value filters.
        List<PartitionValueFilter> partitionValueFiltersToProcess = new ArrayList<>();
        if (partitionValueFilters != null)
        {
            partitionValueFiltersToProcess.addAll(partitionValueFilters);
        }
        if (standalonePartitionValueFilter != null)
        {
            partitionValueFiltersToProcess.add(standalonePartitionValueFilter);
        }
        return partitionValueFiltersToProcess;
    }

    private String getPartitionValueNotFoundErrorMessage(String partitionValueType, String partitionKey, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, List<String> storageNames)
    {
        return String.format("Failed to find %s partition value for partition key = \"%s\" due to " +
            "no available business object data in \"%s\" storage(s) that is registered using that partition. " +
            "Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
            "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataVersion: %d}", partitionValueType, partitionKey,
            StringUtils.join(storageNames, ","), businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
            businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
            businessObjectFormatKey.getBusinessObjectFormatVersion(), businessObjectDataVersion);
    }

    private String getLatestPartitionValueNotFoundErrorMessage(String boundType, String boundPartitionValue, String partitionKey,
        BusinessObjectFormatKey businessObjectFormatKey, Integer businessObjectDataVersion, List<String> storageNames)
    {
        return String.format("Failed to find partition value which is the latest %s partition value = \"%s\" for partition key = \"%s\" due to " +
            "no available business object data in \"%s\" storage that satisfies the search criteria. " +
            "Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
            "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataVersion: %d}", boundType, boundPartitionValue,
            partitionKey, StringUtils.join(storageNames, ","), businessObjectFormatKey.getNamespace(),
            businessObjectFormatKey.getBusinessObjectDefinitionName(), businessObjectFormatKey.getBusinessObjectFormatUsage(),
            businessObjectFormatKey.getBusinessObjectFormatFileType(), businessObjectFormatKey.getBusinessObjectFormatVersion(), businessObjectDataVersion);
    }

    /**
     * Validates the business object data create request. This method also trims appropriate request parameters.
     *
     * @param request the request
     * @param fileSizeRequired specifies if fileSizeBytes value is required or not
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateBusinessObjectDataCreateRequest(BusinessObjectDataCreateRequest request, boolean fileSizeRequired)
    {
        // Validate and trim the request parameters.
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        request.setNamespace(request.getNamespace().trim());

        Assert.hasText(request.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().trim());

        Assert.hasText(request.getBusinessObjectFormatUsage(), "A business object format usage name must be specified.");
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().trim());

        Assert.hasText(request.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().trim());

        Assert.notNull(request.getBusinessObjectFormatVersion(), "A business object format version must be specified.");

        Assert.hasText(request.getPartitionKey(), "A business object format partition key must be specified.");
        request.setPartitionKey(request.getPartitionKey().trim());

        Assert.hasText(request.getPartitionValue(), "A business object data partition value must be specified.");
        request.setPartitionValue(request.getPartitionValue().trim());

        int subPartitionValuesCount = herdHelper.getCollectionSize(request.getSubPartitionValues());
        Assert.isTrue(subPartitionValuesCount <= BusinessObjectDataEntity.MAX_SUBPARTITIONS,
            "Exceeded maximum number of allowed subpartitions:" + BusinessObjectDataEntity.MAX_SUBPARTITIONS + ".");

        for (int i = 0; i < subPartitionValuesCount; i++)
        {
            Assert.hasText(request.getSubPartitionValues().get(i), "A subpartition value must be specified.");
            request.getSubPartitionValues().set(i, request.getSubPartitionValues().get(i).trim());
        }

        // Validate status
        if (StringUtils.isNotBlank(request.getStatus()))
        {
            request.setStatus(request.getStatus().trim());
        }

        Assert.isTrue(!CollectionUtils.isEmpty(request.getStorageUnits()), "At least one storage unit must be specified.");
        for (StorageUnitCreateRequest storageUnit : request.getStorageUnits())
        {
            Assert.notNull(storageUnit, "A storage unit can't be null.");

            // Validate and trim the storage name.
            Assert.hasText(storageUnit.getStorageName(), "A storage name is required for each storage unit.");
            storageUnit.setStorageName(storageUnit.getStorageName().trim());

            if (BooleanUtils.isTrue(storageUnit.isDiscoverStorageFiles()))
            {
                // The auto-discovery of storage files is enabled, thus a storage directory is required and storage files cannot be specified.
                Assert.isTrue(storageUnit.getStorageDirectory() != null, "A storage directory must be specified when discovery of storage files is enabled.");
                Assert.isTrue(CollectionUtils.isEmpty(storageUnit.getStorageFiles()),
                    "Storage files cannot be specified when discovery of storage files is enabled.");
            }
            else
            {
                // Since auto-discovery is disabled, a storage directory or at least one storage file are required for each storage unit.
                Assert.isTrue(storageUnit.getStorageDirectory() != null || !CollectionUtils.isEmpty(storageUnit.getStorageFiles()),
                    "A storage directory or at least one storage file must be specified for each storage unit.");
            }

            // If storageDirectory element is present in the request, we require it to contain a non-empty directoryPath element.
            if (storageUnit.getStorageDirectory() != null)
            {
                Assert.hasText(storageUnit.getStorageDirectory().getDirectoryPath(), "A storage directory path must be specified.");
                storageUnit.getStorageDirectory().setDirectoryPath(storageUnit.getStorageDirectory().getDirectoryPath().trim());
            }

            if (!CollectionUtils.isEmpty(storageUnit.getStorageFiles()))
            {
                for (StorageFile storageFile : storageUnit.getStorageFiles())
                {
                    Assert.hasText(storageFile.getFilePath(), "A file path must be specified.");
                    storageFile.setFilePath(storageFile.getFilePath().trim());

                    if (fileSizeRequired)
                    {
                        Assert.notNull(storageFile.getFileSizeBytes(), "A file size must be specified.");
                    }

                    // Ensure row count is positive.
                    if (storageFile.getRowCount() != null)
                    {
                        Assert.isTrue(storageFile.getRowCount() >= 0, "File \"" + storageFile.getFilePath() + "\" has a row count which is < 0.");
                    }
                }
            }
        }

        // Validate and trim the parents' keys.
        validateBusinessObjectDataKeys(request.getBusinessObjectDataParents());

        // Validate attributes.
        herdHelper.validateAttributes(request.getAttributes());
    }

    /**
     * Validates that the attributes specified in the request are consistent with the attribute definitions in the format.
     *
     * @param request the business object create request.
     * @param businessObjectFormatEntity the business object format entity.
     */
    private void validateAttributesAgainstFormat(BusinessObjectDataCreateRequest request, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        // Build a map of the specified attributes in the request where the key is lower case for case insensitive checks.
        Map<String, String> attributeMap = new HashMap<>();
        if (!CollectionUtils.isEmpty(request.getAttributes()))
        {
            for (Attribute attribute : request.getAttributes())
            {
                attributeMap.put(attribute.getName().toLowerCase(), attribute.getValue());
            }
        }

        // Loop through each attribute definition (i.e. the required attributes) and verify that each definition was specified in the request
        // and that the specified value has non-blank data.
        for (BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity : businessObjectFormatEntity.getAttributeDefinitions())
        {
            String attributeDefinitionName = attributeDefinitionEntity.getName().toLowerCase();
            if ((attributeDefinitionEntity.isRequired()) &&
                ((!attributeMap.containsKey(attributeDefinitionName)) || (StringUtils.isBlank(attributeMap.get(attributeDefinitionName)))))
            {
                throw new IllegalArgumentException(String
                    .format("The business object format has a required attribute \"%s\" which was not specified or has a value which is blank.",
                        attributeDefinitionEntity.getName()));
            }
        }
    }

    /**
     * Creates a new business object data entity from the request information.
     *
     * @param request the request.
     * @param businessObjectFormatEntity the business object format entity.
     * @param businessObjectDataVersion the business object data version.
     *
     * @return the newly created business object data entity.
     */
    private BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectDataCreateRequest request,
        BusinessObjectFormatEntity businessObjectFormatEntity, Integer businessObjectDataVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity)
    {
        // Create a new entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(request.getPartitionValue());
        int subPartitionValuesCount = herdHelper.getCollectionSize(request.getSubPartitionValues());
        businessObjectDataEntity.setPartitionValue2(subPartitionValuesCount > 0 ? request.getSubPartitionValues().get(0) : null);
        businessObjectDataEntity.setPartitionValue3(subPartitionValuesCount > 1 ? request.getSubPartitionValues().get(1) : null);
        businessObjectDataEntity.setPartitionValue4(subPartitionValuesCount > 2 ? request.getSubPartitionValues().get(2) : null);
        businessObjectDataEntity.setPartitionValue5(subPartitionValuesCount > 3 ? request.getSubPartitionValues().get(3) : null);
        businessObjectDataEntity.setVersion(businessObjectDataVersion);
        businessObjectDataEntity.setLatestVersion(true);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

        // Create the storage unit entities.
        businessObjectDataEntity
            .setStorageUnits(createStorageUnitEntitiesFromStorageUnits(request.getStorageUnits(), businessObjectFormatEntity, businessObjectDataEntity));

        // Create the attributes.
        List<BusinessObjectDataAttributeEntity> attributeEntities = new ArrayList<>();
        businessObjectDataEntity.setAttributes(attributeEntities);

        if (!CollectionUtils.isEmpty(request.getAttributes()))
        {
            for (Attribute attribute : request.getAttributes())
            {
                BusinessObjectDataAttributeEntity attributeEntity = new BusinessObjectDataAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setBusinessObjectData(businessObjectDataEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }

        // Create the parents.
        List<BusinessObjectDataEntity> businessObjectDataParents = new ArrayList<>();
        businessObjectDataEntity.setBusinessObjectDataParents(businessObjectDataParents);

        // Loop through all the business object data parents.
        if (request.getBusinessObjectDataParents() != null)
        {
            for (BusinessObjectDataKey businessObjectDataKey : request.getBusinessObjectDataParents())
            {
                // Look up the business object data for each parent.
                BusinessObjectDataEntity businessObjectDataParent = herdDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

                // Add our newly created entity as a dependent (i.e. child) of the looked up parent.
                businessObjectDataParent.getBusinessObjectDataChildren().add(businessObjectDataEntity);

                // Add the looked up parent as a parent of our newly created entity.
                businessObjectDataParents.add(businessObjectDataParent);
            }
        }

        // Return the newly created entity.
        return businessObjectDataEntity;
    }

    /**
     * Creates a list of storage unit entities from a list of storage unit create requests.
     *
     * @param storageUnitCreateRequests the storage unit create requests.
     * @param businessObjectFormatEntity the business object format entity.
     * @param businessObjectDataEntity the business object data entity.
     *
     * @return the list of storage unit entities.
     */
    private List<StorageUnitEntity> createStorageUnitEntitiesFromStorageUnits(List<StorageUnitCreateRequest> storageUnitCreateRequests,
        BusinessObjectFormatEntity businessObjectFormatEntity, BusinessObjectDataEntity businessObjectDataEntity)
    {
        // Create the storage units for the data.
        List<StorageUnitEntity> storageUnitEntities = new ArrayList<>();

        // Get the storage unit status entity for the ENABLED status.
        StorageUnitStatusEntity storageUnitStatusEntity = storageDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.ENABLED);

        for (StorageUnitCreateRequest storageUnit : storageUnitCreateRequests)
        {
            // Get the storage entity per request and verify that it exists.
            StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageUnit.getStorageName());

            // Set up flags which are used to make flow logic easier.
            boolean isS3StoragePlatform = storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.S3);
            boolean isStorageDirectorySpecified = storageUnit.getStorageDirectory() != null;
            boolean validatePathPrefix = storageDaoHelper
                .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX),
                    storageEntity, false, true);
            boolean validateFileExistence = storageDaoHelper
                .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE),
                    storageEntity, false, true);

            // If the storage has any validation configured, get the expected S3 key prefix.
            String expectedS3KeyPrefix = null;
            if ((validatePathPrefix || validateFileExistence) && isS3StoragePlatform)
            {
                // Build the expected S3 key prefix as per S3 naming convention.
                expectedS3KeyPrefix = buildS3KeyPrefix(businessObjectFormatEntity, herdDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity));
            }

            // Create the storage unit and associated storage files.
            StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
            storageUnitEntities.add(storageUnitEntity);
            storageUnitEntity.setStorage(storageEntity);
            storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
            storageUnitEntity.setStatus(storageUnitStatusEntity);

            // Process storage directory path if it is specified.
            String directoryPath = null;
            if (isStorageDirectorySpecified)
            {
                // Get the specified directory path.
                directoryPath = storageUnit.getStorageDirectory().getDirectoryPath();

                // If the validate path prefix flag is configured for this storage, validate the directory path value.
                if (validatePathPrefix && isS3StoragePlatform)
                {
                    // Ensure the directory path adheres to the S3 naming convention.
                    Assert.isTrue(directoryPath.equals(expectedS3KeyPrefix),
                        String.format("Specified directory path \"%s\" does not match the expected S3 key prefix \"%s\".", directoryPath, expectedS3KeyPrefix));

                    // Ensure that the directory path is not already registered with another business object data instance.
                    StorageUnitEntity alreadyRegisteredStorageUnitEntity =
                        herdDao.getStorageUnitByStorageNameAndDirectoryPath(storageEntity.getName(), directoryPath);
                    if (alreadyRegisteredStorageUnitEntity != null)
                    {
                        throw new AlreadyExistsException(String
                            .format("Storage directory \"%s\" in \"%s\" storage is already registered by the business object data {%s}.", directoryPath,
                                storageEntity.getName(),
                                herdDaoHelper.businessObjectDataEntityAltKeyToString(alreadyRegisteredStorageUnitEntity.getBusinessObjectData())));
                    }
                }

                // Store the directory.
                storageUnitEntity.setDirectoryPath(directoryPath);
            }

            // Discover storage files if storage file discovery is enabled. Otherwise, get the storage files specified in the request, if any.
            List<StorageFile> storageFiles =
                BooleanUtils.isTrue(storageUnit.isDiscoverStorageFiles()) ? discoverStorageFiles(storageEntity, directoryPath) : storageUnit.getStorageFiles();

            // Create the storage file entities.
            createStorageFileEntitiesFromStorageFiles(storageFiles, storageEntity, BooleanUtils.isTrue(storageUnit.isDiscoverStorageFiles()),
                expectedS3KeyPrefix, storageUnitEntity, directoryPath, validatePathPrefix, validateFileExistence, isS3StoragePlatform);
        }

        return storageUnitEntities;
    }

    private List<StorageFile> discoverStorageFiles(StorageEntity storageEntity, String s3KeyPrefix)
    {
        // Only S3 storage platform is currently supported for storage file discovery.
        Assert.isTrue(storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.S3),
            String.format("Cannot discover storage files at \"%s\" storage platform.", storageEntity.getStoragePlatform().getName()));

        // Get S3 bucket access parameters.
        S3FileTransferRequestParamsDto params = storageDaoHelper.getS3BucketAccessParams(storageEntity);
        // Retrieve a list of all keys/objects from the S3 bucket matching the specified S3 key prefix.
        // Since S3 key prefix represents the directory, we add a trailing '/' character to it, unless it is already present.
        params.setS3KeyPrefix(s3KeyPrefix.endsWith("/") ? s3KeyPrefix : s3KeyPrefix + "/");
        // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
        List<StorageFile> storageFiles = s3Service.listDirectory(params, true);

        // Fail registration if no storage files were discovered.
        if (CollectionUtils.isEmpty(storageFiles))
        {
            throw new ObjectNotFoundException(String.format("Found no files at \"s3://%s/%s\" location.", params.getS3BucketName(), params.getS3KeyPrefix()));
        }

        return storageFiles;
    }

    private List<StorageFileEntity> createStorageFileEntitiesFromStorageFiles(List<StorageFile> storageFiles, StorageEntity storageEntity,
        boolean storageFilesDiscovered, String expectedS3KeyPrefix, StorageUnitEntity storageUnitEntity, String directoryPath, boolean validatePathPrefix,
        boolean validateFileExistence, boolean isS3StoragePlatform)
    {
        List<StorageFileEntity> storageFileEntities = null;

        // Process storage files if they are specified.
        if (!CollectionUtils.isEmpty(storageFiles))
        {
            storageFileEntities = new ArrayList<>();
            storageUnitEntity.setStorageFiles(storageFileEntities);

            // If the validate file existence flag is configured for this storage and storage files were not discovered, prepare for S3 file validation.
            S3FileTransferRequestParamsDto params = null;
            List<String> actualKeys = null;
            if (validateFileExistence && isS3StoragePlatform && !storageFilesDiscovered)
            {
                // Get the validate file parameters.
                params = getFileValidationParams(storageEntity, expectedS3KeyPrefix, storageUnitEntity, validatePathPrefix);

                // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
                actualKeys = storageFileHelper.getFilePaths(s3Service.listDirectory(params, true));
            }

            // If the validate path prefix flag is configured, ensure that there are no storage files already registered in this
            // storage by some other business object data that start with the expected S3 key prefix.
            if (validatePathPrefix && isS3StoragePlatform)
            {
                // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
                String expectedS3KeyPrefixWithTrailingSlash = expectedS3KeyPrefix + "/";
                Long registeredStorageFileCount = herdDao.getStorageFileCount(storageEntity.getName(), expectedS3KeyPrefixWithTrailingSlash);
                if (registeredStorageFileCount > 0)
                {
                    throw new AlreadyExistsException(String.format(
                        "Found %d storage file(s) matching \"%s\" S3 key prefix in \"%s\" " + "storage that is registered with another business object data.",
                        registeredStorageFileCount, expectedS3KeyPrefix, storageEntity.getName()));
                }
            }

            for (StorageFile storageFile : storageFiles)
            {
                StorageFileEntity storageFileEntity = new StorageFileEntity();
                storageFileEntities.add(storageFileEntity);
                storageFileEntity.setStorageUnit(storageUnitEntity);
                storageFileEntity.setPath(storageFile.getFilePath());
                storageFileEntity.setFileSizeBytes(storageFile.getFileSizeBytes());
                storageFileEntity.setRowCount(storageFile.getRowCount());

                // Skip storage file validation if storage files were discovered.
                if (!storageFilesDiscovered)
                {
                    // Validate that the storage file path matches the key prefix if the validate path prefix flag is configured for this storage.
                    // Otherwise, if a directory path is specified, ensure it is consistent with the file path.
                    if (validatePathPrefix && isS3StoragePlatform)
                    {
                        // Ensure the S3 file key prefix adheres to the S3 naming convention.
                        Assert.isTrue(storageFileEntity.getPath().startsWith(expectedS3KeyPrefix), String
                            .format("Specified storage file path \"%s\" does not match the expected S3 key prefix \"%s\".", storageFileEntity.getPath(),
                                expectedS3KeyPrefix));
                    }
                    else if (directoryPath != null)
                    {
                        // When storage directory path is specified, ensure that storage file path starts with it.
                        Assert.isTrue(storageFileEntity.getPath().startsWith(directoryPath), String
                            .format("Storage file path \"%s\" does not match the storage directory path \"%s\".", storageFileEntity.getPath(), directoryPath));
                    }

                    // Ensure the file exists in S3 if the validate file existence flag is configured for this storage.
                    if (validateFileExistence && isS3StoragePlatform && !actualKeys.contains(storageFileEntity.getPath()))
                    {
                        throw new ObjectNotFoundException(
                            String.format("File not found at s3://%s/%s location.", params.getS3BucketName(), storageFileEntity.getPath()));
                    }
                }
            }
        }

        return storageFileEntities;
    }

    /**
     * Gets the file validation parameters that can be used for getting a list of files by the S3 service. The returned DTO will contain the expected S3 key
     * prefix when the "validate path prefix" flag is set or it will contain the directory of the storage entity if not.
     *
     * @param storageEntity the storage entity.
     * @param expectedS3KeyPrefix the expected key prefix.
     * @param storageUnitEntity the storage unit entity.
     * @param validatePathPrefix the validate path prefix flag.
     *
     * @return the parameters.
     * @throws IllegalArgumentException if the "validate path prefix" flag is not present and no directory is configured on the storage entity.
     */
    public S3FileTransferRequestParamsDto getFileValidationParams(StorageEntity storageEntity, String expectedS3KeyPrefix, StorageUnitEntity storageUnitEntity,
        boolean validatePathPrefix) throws IllegalArgumentException
    {
        // Use the expected S3 key prefix by default (which is set when the validatePathPrefix flag is set).
        String actualFileS3KeyPrefix = expectedS3KeyPrefix;

        // In the case where the validate path prefix flag isn't set, we will either use the specified directory if it exists or it's an error
        // since we have no way of knowing how to validate the files. It isn't reasonable to get a list of all files from S3 individually one by
        // one since this would cause performance problems if a large number of files are present. Getting all files within a single key
        // prefix is reasonable on the other hand since we can list all files at once starting at the key prefix.
        if (!validatePathPrefix)
        {
            if (storageUnitEntity.getDirectoryPath() != null)
            {
                actualFileS3KeyPrefix = storageUnitEntity.getDirectoryPath();
            }
            else
            {
                throw new IllegalArgumentException("Unable to validate file existence because storage \"" + storageEntity.getName() +
                    "\" does not validate path prefix and storage unit doesn't have a directory configured.");
            }
        }

        // Add a trailing backslash if it doesn't already exist.
        // TODO: Consider placing this logic into a helper method since it is being done in multiple places.
        if (!actualFileS3KeyPrefix.endsWith("/"))
        {
            actualFileS3KeyPrefix += "/";
        }

        // Get S3 bucket access parameters and set the actual key prefix.
        S3FileTransferRequestParamsDto params = storageDaoHelper.getS3BucketAccessParams(storageEntity);
        params.setS3KeyPrefix(actualFileS3KeyPrefix);

        // Return the newly created parameters.
        return params;
    }

    /**
     * Returns the partition key column position (one-based numbering).
     *
     * @param partitionKey the partition key
     * @param businessObjectFormatEntity, the business object format entity
     *
     * @return the partition key column position
     * @throws IllegalArgumentException if partition key is not found in schema
     */
    private int getPartitionColumnPosition(String partitionKey, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        int partitionColumnPosition = 0;

        if (partitionKey.equalsIgnoreCase(businessObjectFormatEntity.getPartitionKey()))
        {
            partitionColumnPosition = BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION;
        }
        else
        {
            // Get business object format model object to directly access schema columns and partitions.
            BusinessObjectFormat businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

            Assert.notNull(businessObjectFormat.getSchema(), String.format(
                "Partition key \"%s\" doesn't match configured business object format partition key \"%s\" and " +
                    "there is no schema defined to check subpartition columns for business object format {%s}.", partitionKey,
                businessObjectFormatEntity.getPartitionKey(), herdDaoHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));

            for (int i = 0; i < Math.min(BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1, businessObjectFormat.getSchema().getPartitions().size()); i++)
            {
                if (partitionKey.equalsIgnoreCase(businessObjectFormat.getSchema().getPartitions().get(i).getName()))
                {
                    // Partition key found in schema.
                    partitionColumnPosition = i + 1;
                    break;
                }
            }

            Assert.isTrue(partitionColumnPosition > 0, String
                .format("The partition key \"%s\" does not exist in first %d partition columns in the schema for business object format {%s}.", partitionKey,
                    BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1, herdDaoHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
        }

        return partitionColumnPosition;
    }

    /**
     * Returns a Cartesian product of the lists of values specified.
     *
     * @param lists the lists of values
     *
     * @return the Cartesian product
     */
    private List<String[]> getCrossProduct(Map<Integer, List<String>> lists)
    {
        List<String[]> results = new ArrayList<>();
        getCrossProduct(results, lists, 0, new String[lists.size()]);
        return results;
    }

    /**
     * A helper function used to compute a Cartesian product of the lists of values specified.
     */
    private void getCrossProduct(List<String[]> results, Map<Integer, List<String>> lists, int depth, String[] current)
    {
        for (int i = 0; i < lists.get(depth).size(); i++)
        {
            current[depth] = lists.get(depth).get(i);
            if (depth < lists.keySet().size() - 1)
            {
                getCrossProduct(results, lists, depth + 1, current);
            }
            else
            {
                results.add(Arrays.copyOf(current, current.length));
            }
        }
    }
}
