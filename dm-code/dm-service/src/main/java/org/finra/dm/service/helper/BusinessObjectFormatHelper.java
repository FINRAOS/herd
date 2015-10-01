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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.dm.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.SchemaColumnEntity;
import org.finra.dm.model.api.xml.AttributeDefinition;
import org.finra.dm.model.api.xml.BusinessObjectFormat;
import org.finra.dm.model.api.xml.BusinessObjectFormatKey;
import org.finra.dm.model.api.xml.Schema;
import org.finra.dm.model.api.xml.SchemaColumn;

/**
 * A helper class for BusinessObjectFormatService related data management code.
 */
@Component
public class BusinessObjectFormatHelper
{
    @Autowired
    private DmDaoHelper dmDaoHelper;

    /**
     * Populates a business object format key with a legacy namespace if namespace if not there.
     *
     * @param key the business object format key
     */
    public void populateLegacyNamespace(BusinessObjectFormatKey key)
    {
        if (StringUtils.isBlank(key.getNamespace()))
        {
            // Set namespace to the retrieved namespace code value for the specified legacy business object definition.
            key.setNamespace(dmDaoHelper.getNamespaceCode(key.getBusinessObjectDefinitionName()));
        }
    }

    /**
     * Creates the business object format from the persisted entity.
     *
     * @param businessObjectFormatEntity the newly persisted business object format entity.
     *
     * @return the business object format.
     */
    public BusinessObjectFormat createBusinessObjectFormatFromEntity(BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        businessObjectFormat.setId(businessObjectFormatEntity.getId());
        businessObjectFormat.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectFormat.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        businessObjectFormat.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        businessObjectFormat.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        businessObjectFormat.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());
        businessObjectFormat.setLatestVersion(businessObjectFormatEntity.getLatestVersion());
        businessObjectFormat.setPartitionKey(businessObjectFormatEntity.getPartitionKey());
        businessObjectFormat.setDescription(businessObjectFormatEntity.getDescription());

        // Add in the attribute definitions.
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        businessObjectFormat.setAttributeDefinitions(attributeDefinitions);

        for (BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity : businessObjectFormatEntity.getAttributeDefinitions())
        {
            AttributeDefinition attributeDefinition = new AttributeDefinition();
            attributeDefinitions.add(attributeDefinition);
            attributeDefinition.setName(attributeDefinitionEntity.getName());
        }

        // Only add schema information if this format has any schema columns defined.
        if (!businessObjectFormatEntity.getSchemaColumns().isEmpty())
        {
            Schema schema = new Schema();
            businessObjectFormat.setSchema(schema);
            schema.setNullValue(businessObjectFormatEntity.getNullValue());
            schema.setDelimiter(businessObjectFormatEntity.getDelimiter());
            schema.setEscapeCharacter(businessObjectFormatEntity.getEscapeCharacter());
            schema.setPartitionKeyGroup(
                businessObjectFormatEntity.getPartitionKeyGroup() != null ? businessObjectFormatEntity.getPartitionKeyGroup().getPartitionKeyGroupName() :
                    null);

            // Create two lists of schema column entities: one for the data columns and one for the partition columns.
            List<SchemaColumnEntity> dataSchemaColumns = new ArrayList<>();
            List<SchemaColumnEntity> partitionSchemaColumns = new ArrayList<>();
            for (SchemaColumnEntity schemaColumnEntity : businessObjectFormatEntity.getSchemaColumns())
            {
                // We can determine which list (or both) a column entity belongs to depending on whether it has a position and/or partition level set.
                if (schemaColumnEntity.getPosition() != null)
                {
                    dataSchemaColumns.add(schemaColumnEntity);
                }
                if (schemaColumnEntity.getPartitionLevel() != null)
                {
                    partitionSchemaColumns.add(schemaColumnEntity);
                }
            }

            // Sort the data schema columns on the position.
            Collections.sort(dataSchemaColumns, new SchemaColumnPositionComparator());

            // Sort the partition schema columns on the partition level.
            Collections.sort(partitionSchemaColumns, new SchemaColumnPartitionLevelComparator());

            // Add in the data schema columns.
            List<SchemaColumn> schemaColumns = new ArrayList<>();
            schema.setColumns(schemaColumns);
            for (SchemaColumnEntity schemaColumnEntity : dataSchemaColumns)
            {
                schemaColumns.add(createSchemaColumn(schemaColumnEntity));
            }

            // Add in the partition schema columns.
            // We need to only add partitions when we have data. Otherwise an empty list will cause JAXB to generate a partitions wrapper tag with no
            // columns which isn't valid from an XSD standpoint.
            if (partitionSchemaColumns.size() > 0)
            {
                schemaColumns = new ArrayList<>();
                schema.setPartitions(schemaColumns);
                for (SchemaColumnEntity schemaColumnEntity : partitionSchemaColumns)
                {
                    schemaColumns.add(createSchemaColumn(schemaColumnEntity));
                }
            }
        }

        return businessObjectFormat;
    }

    /**
     * A schema column "position" comparator. A static named inner class was created as opposed to an anonymous inner class since it has no dependencies on it's
     * containing class and is therefore more efficient.
     */
    private static class SchemaColumnPositionComparator implements Comparator<SchemaColumnEntity>, Serializable
    {
        private static final long serialVersionUID = -5860079250619473538L;

        @Override
        public int compare(SchemaColumnEntity entity1, SchemaColumnEntity entity2)
        {
            return entity1.getPosition().compareTo(entity2.getPosition());
        }
    }

    /**
     * A schema column "partitionLevel" comparator. A static named inner class was created as opposed to an anonymous inner class since it has no dependencies
     * on it's containing class and is therefore more efficient.
     */
    private static class SchemaColumnPartitionLevelComparator implements Comparator<SchemaColumnEntity>, Serializable
    {
        private static final long serialVersionUID = -6222033387743498432L;

        @Override
        public int compare(SchemaColumnEntity entity1, SchemaColumnEntity entity2)
        {
            return entity1.getPartitionLevel().compareTo(entity2.getPartitionLevel());
        }
    }

    /**
     * Creates a schema column from a schema column entity.
     *
     * @param schemaColumnEntity the schema column entity.
     *
     * @return the newly created schema column.
     */
    private SchemaColumn createSchemaColumn(SchemaColumnEntity schemaColumnEntity)
    {
        SchemaColumn schemaColumn = new SchemaColumn();
        schemaColumn.setName(schemaColumnEntity.getName());
        schemaColumn.setType(schemaColumnEntity.getType());
        schemaColumn.setSize(schemaColumnEntity.getSize());
        schemaColumn.setRequired(schemaColumnEntity.getRequired());
        schemaColumn.setDefaultValue(schemaColumnEntity.getDefaultValue());
        schemaColumn.setDescription(schemaColumnEntity.getDescription());
        return schemaColumn;
    }
}
