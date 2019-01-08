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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.exception.ParseErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceDescriptiveInformation;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;

/**
 * A helper class for business object format external interface descriptive information related code.
 */
@Component
public class BusinessObjectFormatExternalInterfaceDescriptiveInformationHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectFormatExternalInterfaceDescriptiveInformationHelper.class);

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private VelocityNonStrictHelper velocityHelper;

    /**
     * Creates the business object format external interface descriptive information from the business object format entity and external interface entity.
     * This method will apply the velocity engine to the velocity template in the external interface entity description.
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param externalInterfaceEntity the external interface entity
     *
     * @return the business object format external interface descriptive information
     */
    public BusinessObjectFormatExternalInterfaceDescriptiveInformation createBusinessObjectFormatExternalInterfaceDescriptiveInformationFromEntities(
        BusinessObjectFormatEntity businessObjectFormatEntity, ExternalInterfaceEntity externalInterfaceEntity)
    {
        // Convert the business object format entity to the business object format model object
        BusinessObjectFormat businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

        // Build the BusinessObjectFormatExternalInterfaceKey with information from the business object format entity and the external interface entity
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(businessObjectFormat.getNamespace(), businessObjectFormat.getBusinessObjectDefinitionName(),
                businessObjectFormat.getBusinessObjectFormatUsage(), businessObjectFormat.getBusinessObjectFormatFileType(), externalInterfaceEntity.getCode());

        // Velocity Template Resource Names:
        // ${StringUtils}                       The string utilities class
        // ${CollectionUtils}                   The collection utilities class
        // ${Collections}                       The collections class
        // ${namespace}                         The namespace associated with this business object format
        // ${businessObjectDefinitionName}      The name of the business object definition associated with this business object format
        // ${businessObjectFormatUsage}         The usage associated with the business object format
        // ${businessObjectFormatFileType}      The file type associated with the business object format
        // ${businessObjectFormatAttributes}    The business object format attributes map of key value pairs
        // ${partitionColumnNames}              The partition column names associated with the business object format
        // ${partitionKeyGroup}                 The partition key group associated with the business object format

        // Build velocity context variable map
        Map<String, Object> velocityContext = Maps.newHashMap();
        velocityContext.put("StringUtils", StringUtils.class);
        velocityContext.put("CollectionUtils", CollectionUtils.class);
        velocityContext.put("Collections", Collections.class);
        velocityContext.put("namespace", businessObjectFormat.getNamespace());
        velocityContext.put("businessObjectDefinitionName", businessObjectFormat.getBusinessObjectDefinitionName());
        velocityContext.put("businessObjectFormatUsage", businessObjectFormat.getBusinessObjectFormatUsage());
        velocityContext.put("businessObjectFormatFileType", businessObjectFormat.getBusinessObjectFormatFileType());

        // Build an insertion ordered map of business object format attributes.
        Map<String, String> businessObjectFormatAttributes = Maps.newLinkedHashMap();
        for (Attribute attribute : businessObjectFormat.getAttributes())
        {
            businessObjectFormatAttributes.put(attribute.getName(), attribute.getValue());
        }

        velocityContext.put("businessObjectFormatAttributes", businessObjectFormatAttributes);

        // Build an insertion ordered list of partition column names.
        List<String> partitionColumnNames = Lists.newLinkedList();
        if (businessObjectFormat.getSchema() != null)
        {
            for (SchemaColumn schemaColumn : businessObjectFormat.getSchema().getPartitions())
            {
                partitionColumnNames.add(schemaColumn.getName());
            }
        }

        velocityContext.put("partitionColumnNames", partitionColumnNames);

        if (businessObjectFormatEntity.getPartitionKeyGroup() != null)
        {
            velocityContext.put("partitionKeyGroup", businessObjectFormatEntity.getPartitionKeyGroup().getPartitionKeyGroupName());
        }
        else
        {
            velocityContext.put("partitionKeyGroup", "");
        }

        // Create a string to hold the velocity template evaluated external interface description.
        String velocityEvaluatedExternalInterfaceDescription;

        // Catch any parse error exceptions, and throw an illegal argument exception instead.
        try
        {
            // Use the velocity helper to evaluate the external interface description velocity template.
            // During the evaluation of the velocity template by the velocity engine a parse error exception may occur.
            velocityEvaluatedExternalInterfaceDescription =
                velocityHelper.evaluate(externalInterfaceEntity.getDescription(), velocityContext, "External Interface Description", false);
        }
        catch (ParseErrorException parseErrorException)
        {
            // Build an exception message that contains the external interface information as well as the parse error.
            String exceptionMessage = String
                .format("Failed to evaluate velocity template in the external interface with name \"%s\". Reason: %s", externalInterfaceEntity.getCode(),
                    parseErrorException.getMessage());

            // Log the parsing error.
            LOGGER.error(exceptionMessage, parseErrorException);

            // Throw a new illegal argument exception with the exception message.
            throw new IllegalArgumentException(exceptionMessage);
        }

        return new BusinessObjectFormatExternalInterfaceDescriptiveInformation(businessObjectFormatExternalInterfaceKey,
            externalInterfaceEntity.getDisplayName(), velocityEvaluatedExternalInterfaceDescription);
    }
}