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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceDescriptiveInformation;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceDescriptiveInformationKey;
import org.finra.herd.model.jpa.BusinessObjectFormatAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;

/**
 * A helper class for business object format external interface descriptive information related code.
 */
@Component
public class BusinessObjectFormatExternalInterfaceDescriptiveInformationHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private VelocityHelper velocityHelper;

    /**
     * Validates a business object format external interface descriptive information key. This method also trims the key parameters.
     *
     * @param businessObjectFormatExternalInterfaceDescriptiveInformationKey the business object format to external interface mapping key
     */
    public void validateAndTrimBusinessObjectFormatExternalInterfaceDescriptiveInformationKey(
        BusinessObjectFormatExternalInterfaceDescriptiveInformationKey businessObjectFormatExternalInterfaceDescriptiveInformationKey)
    {
        Assert.notNull(businessObjectFormatExternalInterfaceDescriptiveInformationKey,
            "A business object format external interface descriptive information key must be specified.");
        businessObjectFormatExternalInterfaceDescriptiveInformationKey.setNamespace(
            alternateKeyHelper.validateStringParameter("namespace", businessObjectFormatExternalInterfaceDescriptiveInformationKey.getNamespace()));
        businessObjectFormatExternalInterfaceDescriptiveInformationKey.setBusinessObjectDefinitionName(alternateKeyHelper
            .validateStringParameter("business object definition name",
                businessObjectFormatExternalInterfaceDescriptiveInformationKey.getBusinessObjectDefinitionName()));
        businessObjectFormatExternalInterfaceDescriptiveInformationKey.setBusinessObjectFormatUsage(alternateKeyHelper
            .validateStringParameter("business object format usage",
                businessObjectFormatExternalInterfaceDescriptiveInformationKey.getBusinessObjectFormatUsage()));
        businessObjectFormatExternalInterfaceDescriptiveInformationKey.setBusinessObjectFormatFileType(alternateKeyHelper
            .validateStringParameter("business object format file type",
                businessObjectFormatExternalInterfaceDescriptiveInformationKey.getBusinessObjectFormatFileType()));
        businessObjectFormatExternalInterfaceDescriptiveInformationKey.setExternalInterfaceName(alternateKeyHelper
            .validateStringParameter("An", "external interface name",
                businessObjectFormatExternalInterfaceDescriptiveInformationKey.getExternalInterfaceName()));
    }

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
        // Build the BusinessObjectFormatExternalInterfaceDescriptiveInformationKey with information from the business object format entity and the external
        // interface entity
        BusinessObjectFormatExternalInterfaceDescriptiveInformationKey businessObjectFormatExternalInterfaceDescriptiveInformationKey =
            new BusinessObjectFormatExternalInterfaceDescriptiveInformationKey(
                businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
                businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
                businessObjectFormatEntity.getFileType().getCode(), externalInterfaceEntity.getCode());

        // Velocity Template Resource Names:
        // ${namespace}            The namespace associated with this business object format
        // ${bdefName}             The name of the business object definition associated with this business object format
        // ${usage}                The usage associated with the business object format
        // ${fileType}             The file type associated with the business object format
        // ${attributes}           The business object format attributes map of key value pairs
        // ${schemaColumns}        The schema columns associated with the business object format
        // ${partitions}           The partitions (name and data type) associated with the business object format
        // ${partitionKeyGroup}    The partition key group associated with the business object format
        // ${delimiter}            The delimiter associated with the business object format
        // ${nullValue}            The null value associated with the business object format

        // Build velocity context variable map
        Map<String, Object> velocityContext = Maps.newHashMap();
        velocityContext.put("namespace", businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        velocityContext.put("bdefName", businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        velocityContext.put("usage", businessObjectFormatEntity.getUsage());
        velocityContext.put("fileType", businessObjectFormatEntity.getFileType().getCode());

        // Loop through attribute names
        Map<String, String> attributes = Maps.newHashMap();
        for (BusinessObjectFormatAttributeEntity businessObjectFormatAttributeEntity : businessObjectFormatEntity.getAttributes())
        {
            attributes.put(businessObjectFormatAttributeEntity.getName(), businessObjectFormatAttributeEntity.getValue());
        }

        velocityContext.put("attributes", attributes.toString());

        // Loop through schema columns
        List<String> columnNames = Lists.newArrayList();
        List<String> partitionColumnNames = Lists.newArrayList();
        for (SchemaColumnEntity schemaColumn : businessObjectFormatEntity.getSchemaColumns())
        {
            if (schemaColumn.getPartitionLevel() == null)
            {
                columnNames.add(schemaColumn.getName());
            }
            else
            {
                partitionColumnNames.add(schemaColumn.getName());
            }
        }

        velocityContext.put("schemaColumns", String.join(",", columnNames));
        velocityContext.put("partitions", String.join(",", partitionColumnNames));

        if (businessObjectFormatEntity.getPartitionKeyGroup() != null)
        {
            velocityContext.put("partitionKeyGroup", businessObjectFormatEntity.getPartitionKeyGroup().getPartitionKeyGroupName());
        }
        else
        {
            velocityContext.put("partitionKeyGroup", "");
        }

        velocityContext.put("delimiter", businessObjectFormatEntity.getDelimiter());
        velocityContext.put("nullValue", businessObjectFormatEntity.getNullValue());

        return new BusinessObjectFormatExternalInterfaceDescriptiveInformation(businessObjectFormatExternalInterfaceDescriptiveInformationKey,
            externalInterfaceEntity.getDisplayName(),
            velocityHelper.evaluate(externalInterfaceEntity.getDescription(), velocityContext, "External Interface Description", false));
    }
}