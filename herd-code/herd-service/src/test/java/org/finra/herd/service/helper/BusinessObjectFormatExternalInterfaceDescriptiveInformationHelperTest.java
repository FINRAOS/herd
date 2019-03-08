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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceDescriptiveInformation;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;
import org.finra.herd.service.AbstractServiceTest;

public class BusinessObjectFormatExternalInterfaceDescriptiveInformationHelperTest extends AbstractServiceTest
{
    @Test
    public void testCreateBusinessObjectFormatExternalInterfaceDescriptiveInformationFromEntities()
    {
        // Create a velocity template for the external interface entity.
        String velocityTemplateDescription = "${namespace}#${businessObjectDefinitionName}#${businessObjectFormatUsage}#${businessObjectFormatFileType}#" +
            "${businessObjectFormatAttributes}#${partitionColumnNames}#${partitionKeyGroup}#$StringUtils.isNotEmpty($namespace)";

        // Get a list of partition columns that is larger than number of partitions supported by business object data registration.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        assertTrue(CollectionUtils.size(partitionColumns) > BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1);

        // Get a list of regular columns.
        List<SchemaColumn> regularColumns = schemaColumnDaoTestHelper.getTestSchemaColumns();

        // Create a list of attributes
        List<Attribute> attributes = Lists.newArrayList(new Attribute(ATTRIBUTE_NAME, ATTRIBUTE_VALUE), new Attribute(ATTRIBUTE_NAME_2, ATTRIBUTE_VALUE_2));

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, partitionColumns.get(0).getName(), PARTITION_KEY_GROUP, attributes, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, regularColumns, partitionColumns);

        // Create an external interface entity.
        ExternalInterfaceEntity externalInterfaceEntity = externalInterfaceDaoTestHelper.createExternalInterfaceEntity(EXTERNAL_INTERFACE);
        externalInterfaceEntity.setDisplayName(DISPLAY_NAME_FIELD);
        externalInterfaceEntity.setDescription(velocityTemplateDescription);

        // Create a business object format to external interface mapping entity.
        businessObjectFormatExternalInterfaceDaoTestHelper
            .createBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatEntity, externalInterfaceEntity);

        // Call the method under test.
        BusinessObjectFormatExternalInterfaceDescriptiveInformation result = businessObjectFormatExternalInterfaceDescriptiveInformationHelper
            .createBusinessObjectFormatExternalInterfaceDescriptiveInformationFromEntities(businessObjectFormatEntity, externalInterfaceEntity);

        // Validate the results.
        assertEquals(result.getBusinessObjectFormatExternalInterfaceKey().getNamespace(), NAMESPACE);
        assertEquals(result.getBusinessObjectFormatExternalInterfaceKey().getBusinessObjectDefinitionName(), BDEF_NAME);
        assertEquals(result.getBusinessObjectFormatExternalInterfaceKey().getBusinessObjectFormatUsage(), FORMAT_USAGE_CODE);
        assertEquals(result.getBusinessObjectFormatExternalInterfaceKey().getBusinessObjectFormatFileType(), FORMAT_FILE_TYPE_CODE);
        assertEquals(result.getBusinessObjectFormatExternalInterfaceKey().getExternalInterfaceName(), EXTERNAL_INTERFACE);
        assertEquals(result.getExternalInterfaceDisplayName(), DISPLAY_NAME_FIELD);

        String[] descriptionTemplateReplacementValues = result.getExternalInterfaceDescription().split("#");

        // ${namespace}
        assertEquals("Namespace not equal to replacement.", descriptionTemplateReplacementValues[0], NAMESPACE);

        // ${businessObjectDefinitionName}
        assertEquals("Business object definition name not equal to replacement.", descriptionTemplateReplacementValues[1], BDEF_NAME);

        // ${businessObjectFormatUsage}
        assertEquals("Usage not equal to replacement.", descriptionTemplateReplacementValues[2], FORMAT_USAGE_CODE);

        // ${businessObjectFormatFileType}
        assertEquals("File type not equal to replacement.", descriptionTemplateReplacementValues[3], FORMAT_FILE_TYPE_CODE);

        // ${businessObjectFormatAttributes}
        Map<String, String> attributesMap = Maps.newLinkedHashMap();
        for (Attribute attribute : attributes)
        {
            attributesMap.put(attribute.getName(), attribute.getValue());
        }
        assertEquals("Attributes not equal to replacement.", descriptionTemplateReplacementValues[4], attributesMap.toString());

        // ${partitionColumnNames}
        List<String> partitionColumnNames = Lists.newArrayList();
        for (SchemaColumnEntity schemaColumn : businessObjectFormatEntity.getSchemaColumns())
        {
            if (schemaColumn.getPartitionLevel() != null)
            {
                partitionColumnNames.add(schemaColumn.getName());
            }
        }
        assertEquals("Partitions not equal to replacement.", descriptionTemplateReplacementValues[5], partitionColumnNames.toString());

        // ${partitionKeyGroup}
        assertEquals("Partition key group not equal to replacement.", descriptionTemplateReplacementValues[6], PARTITION_KEY_GROUP);

        // $StringUtils
        assertEquals("StringUtils not equal to replacement.", descriptionTemplateReplacementValues[7], "true");
    }

    @Test
    public void testCreateBusinessObjectFormatExternalInterfaceDescriptiveInformationFromEntitiesWithNullPartitionKeyGroup()
    {
        // Create a velocity template for the external interface entity.
        String velocityTemplateDescription = "${partitionKeyGroup}";

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, NO_PARTITION_KEY, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, NO_COLUMNS, NO_PARTITION_COLUMNS);

        // Create an external interface entity.
        ExternalInterfaceEntity externalInterfaceEntity = externalInterfaceDaoTestHelper.createExternalInterfaceEntity(EXTERNAL_INTERFACE);
        externalInterfaceEntity.setDescription(velocityTemplateDescription);

        // Create a business object format to external interface mapping entity.
        businessObjectFormatExternalInterfaceDaoTestHelper
            .createBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatEntity, externalInterfaceEntity);

        // Call the method under test.
        BusinessObjectFormatExternalInterfaceDescriptiveInformation result = businessObjectFormatExternalInterfaceDescriptiveInformationHelper
            .createBusinessObjectFormatExternalInterfaceDescriptiveInformationFromEntities(businessObjectFormatEntity, externalInterfaceEntity);

        // Validate no partition key group
        assertEquals("Partition key group not equal to replacement.", result.getExternalInterfaceDescription(), "");
    }

    @Test
    public void testCreateBusinessObjectFormatExternalInterfaceDescriptiveInformationFromEntitiesWithException()
    {
        // Create a velocity template for the external interface entity.
        String velocityTemplateDescription = "#elseif";

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, NO_PARTITION_KEY, NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, NO_COLUMNS, NO_PARTITION_COLUMNS);

        // Create an external interface entity.
        ExternalInterfaceEntity externalInterfaceEntity = externalInterfaceDaoTestHelper.createExternalInterfaceEntity(EXTERNAL_INTERFACE);
        externalInterfaceEntity.setDescription(velocityTemplateDescription);

        // Create a business object format to external interface mapping entity.
        businessObjectFormatExternalInterfaceDaoTestHelper
            .createBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatEntity, externalInterfaceEntity);

        // Build the illegal argument exception message.
        String parseErrorExceptionMessage = String.format(
            "Encountered \"#elseif\" at External Interface Description[line 1, column 1]%n" + "Was expecting one of:%n" + "    <EOF> %n" + "    \"(\" ...%n" +
                "    <RPAREN> ...%n" + "    <ESCAPE_DIRECTIVE> ...%n" + "    <SET_DIRECTIVE> ...%n" + "    \"##\" ...%n" + "    \"\\\\\\\\\" ...%n" +
                "    \"\\\\\" ...%n" + "    <TEXT> ...%n" + "    \"*#\" ...%n" + "    \"*#\" ...%n" + "    \"]]#\" ...%n" + "    <STRING_LITERAL> ...%n" +
                "    <IF_DIRECTIVE> ...%n" + "    <INTEGER_LITERAL> ...%n" + "    <FLOATING_POINT_LITERAL> ...%n" + "    <WORD> ...%n" +
                "    <BRACKETED_WORD> ...%n" + "    <IDENTIFIER> ...%n" + "    <DOT> ...%n" + "    \"{\" ...%n" + "    \"}\" ...%n" +
                "    <EMPTY_INDEX> ...%n" + "    ");

        String illegalArgumentExceptionMessage = String
            .format("Failed to evaluate velocity template in the external interface with name \"%s\". Reason: %s", externalInterfaceEntity.getCode(),
                parseErrorExceptionMessage);

        try
        {
            // Call the method under test.
            businessObjectFormatExternalInterfaceDescriptiveInformationHelper
                .createBusinessObjectFormatExternalInterfaceDescriptiveInformationFromEntities(businessObjectFormatEntity, externalInterfaceEntity);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate the exception message
            assertEquals("Actual illegal argument exception message not equal to expected illegal argument exception message.", illegalArgumentExceptionMessage,
                illegalArgumentException.getMessage());
        }
    }
}
