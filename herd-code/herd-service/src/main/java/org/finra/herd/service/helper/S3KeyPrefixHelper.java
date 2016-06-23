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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;

/**
 * A helper class for S3 key prefix related code.
 */
@Component
public class S3KeyPrefixHelper
{
    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private VelocityHelper velocityHelper;

    /**
     * Returns the S3 object key prefix based on the given storage, business object format and business object data key.
     *
     * @param storageEntity the storage entity
     * @param businessObjectFormatEntity the business object format entity
     * @param businessObjectDataKey the business object data key
     *
     * @return the S3 key prefix
     */
    public String buildS3KeyPrefix(StorageEntity storageEntity, BusinessObjectFormatEntity businessObjectFormatEntity,
        BusinessObjectDataKey businessObjectDataKey)
    {
        // Validate that storage platform is of "S3" storage platform type.
        Assert.isTrue(StoragePlatformEntity.S3.equals(storageEntity.getStoragePlatform().getName()),
            String.format("The specified storage \"%s\" is not an S3 storage platform.", storageEntity.getName()));

        // Retrieve S3 key prefix velocity template storage attribute value and store it in memory.
        // Please note that it is not required, so we pass in a "false" flag.
        String s3KeyPrefixVelocityTemplate = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), storageEntity,
                false);

        // Validate that S3 key prefix velocity template is configured.
        Assert.isTrue(StringUtils.isNotBlank(s3KeyPrefixVelocityTemplate),
            String.format("Storage \"%s\" has no S3 key prefix velocity template configured.", storageEntity.getName()));

        return buildS3KeyPrefix(s3KeyPrefixVelocityTemplate, businessObjectFormatEntity, businessObjectDataKey, storageEntity.getName());
    }

    /**
     * Returns the S3 object key prefix based on the given velocity template, business object format and business object data key.
     *
     * @param s3KeyPrefixVelocityTemplate the S3 key prefix velocity template, not null
     * @param businessObjectFormatEntity the business object format entity
     * @param businessObjectDataKey the business object data key
     * @param storageName the storage name
     *
     * @return the S3 key prefix
     */
    public String buildS3KeyPrefix(String s3KeyPrefixVelocityTemplate, BusinessObjectFormatEntity businessObjectFormatEntity,
        BusinessObjectDataKey businessObjectDataKey, String storageName)
    {
        // Get business object format model object to directly access schema columns and partitions.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

        return buildS3KeyPrefixHelper(s3KeyPrefixVelocityTemplate, businessObjectFormatEntity.getBusinessObjectDefinition().getDataProvider().getName(),
            businessObjectFormat, businessObjectDataKey, storageName);
    }

    /**
     * Returns S3 key prefix constructed per specified velocity template.
     *
     * @param s3KeyPrefixVelocityTemplate the S3 key prefix velocity template,
     * @param dataProviderName the data provider name
     * @param businessObjectFormat the business object format
     * @param businessObjectDataKey the business object data key
     * @param storageName the storage name
     *
     * @return the S3 key prefix
     */
    private String buildS3KeyPrefixHelper(String s3KeyPrefixVelocityTemplate, String dataProviderName, BusinessObjectFormat businessObjectFormat,
        BusinessObjectDataKey businessObjectDataKey, String storageName)
    {
        // Create and populate the velocity context with variable values.
        Map<String, Object> context = new HashMap<>();
        context.put("environment", s3KeyPrefixFormat(configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT)));
        context.put("namespace", s3KeyPrefixFormat(businessObjectFormat.getNamespace()));
        context.put("dataProviderName", s3KeyPrefixFormat(dataProviderName));
        context.put("businessObjectDefinitionName", s3KeyPrefixFormat(businessObjectFormat.getBusinessObjectDefinitionName()));
        context.put("businessObjectFormatUsage", s3KeyPrefixFormat(businessObjectFormat.getBusinessObjectFormatUsage()));
        context.put("businessObjectFormatFileType", s3KeyPrefixFormat(businessObjectFormat.getBusinessObjectFormatFileType()));
        context.put("businessObjectFormatVersion", s3KeyPrefixFormat(String.valueOf(businessObjectFormat.getBusinessObjectFormatVersion())));
        context.put("businessObjectDataVersion", s3KeyPrefixFormat(String.valueOf(businessObjectDataKey.getBusinessObjectDataVersion())));
        context.put("businessObjectFormatPartitionKey", s3KeyPrefixFormat(s3KeyPrefixFormat(businessObjectFormat.getPartitionKey())));
        context.put("businessObjectDataPartitionValue", businessObjectDataKey.getPartitionValue());

        // Build an ordered map of sub-partition column names to sub-partition values.
        Map<String, String> subPartitions = new LinkedHashMap<>();
        if (!CollectionUtils.isEmpty(businessObjectDataKey.getSubPartitionValues()))
        {
            // Validate that business object format has a schema.
            Assert.notNull(businessObjectFormat.getSchema(), String
                .format("Schema must be defined when using subpartition values for business object format {%s}.",
                    businessObjectFormatHelper.businessObjectFormatKeyToString(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormat))));

            // Validate that business object format has a schema with partitions.
            Assert.notNull(businessObjectFormat.getSchema().getPartitions(), String
                .format("Schema partition(s) must be defined when using subpartition values for business object " + "format {%s}.",
                    businessObjectFormatHelper.businessObjectFormatKeyToString(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormat))));

            // Validate that we have sub-partition columns specified in the business object format schema.
            Assert.isTrue(businessObjectFormat.getSchema().getPartitions().size() > businessObjectDataKey.getSubPartitionValues().size(),
                String.format("Number of subpartition values specified for the business object data is greater than or equal to " +
                    "the number of partition columns defined in the schema for the associated business object format." +
                    "Business object data: {%s}", businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

            for (int i = 0; i < businessObjectDataKey.getSubPartitionValues().size(); i++)
            {
                subPartitions.put(s3KeyPrefixFormat(businessObjectFormat.getSchema().getPartitions().get(i + 1).getName()),
                    businessObjectDataKey.getSubPartitionValues().get(i));
            }
        }

        // Add the map of sub-partitions to the context.
        context.put("businessObjectDataSubPartitions", subPartitions);
        context.put("CollectionUtils", CollectionUtils.class);

        // Process the velocity template.
        String s3KeyPrefix = velocityHelper
            .evaluate(s3KeyPrefixVelocityTemplate, context, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE));

        // Validate that S3 key prefix is not blank.
        Assert.isTrue(StringUtils.isNotBlank(s3KeyPrefix), String
            .format("S3 key prefix velocity template \"%s\" configured for \"%s\" storage results in an empty S3 key prefix.", s3KeyPrefixVelocityTemplate,
                storageName));

        // Return the S3 key prefix.
        return s3KeyPrefix;
    }

    /**
     * Converts the specified string into tht S3 key prefix format. This implies making the string lower case and converting underscores to dashes.
     *
     * @param string the string to convert
     *
     * @return the string in S3 format
     */
    private String s3KeyPrefixFormat(String string)
    {
        return string.toLowerCase().replace('_', '-');
    }
}
