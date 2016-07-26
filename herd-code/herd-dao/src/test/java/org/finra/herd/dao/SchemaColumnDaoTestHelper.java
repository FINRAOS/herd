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
package org.finra.herd.dao;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;

@Component
public class SchemaColumnDaoTestHelper
{
    @Autowired
    private SchemaColumnDao schemaColumnDao;

    /**
     * Creates and persists a new schema column entity.
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param columnName the name of the schema column
     *
     * @return the newly created schema column entity
     */
    public SchemaColumnEntity createSchemaColumnEntity(BusinessObjectFormatEntity businessObjectFormatEntity, String columnName)
    {
        return createSchemaColumnEntity(businessObjectFormatEntity, columnName, null);
    }

    /**
     * Creates and persists a new schema column entity.
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param columnName the name of the schema column
     * @param businessObjectDefinitionColumnEntity the business object definition column entity
     *
     * @return the newly created schema column entity
     */
    public SchemaColumnEntity createSchemaColumnEntity(BusinessObjectFormatEntity businessObjectFormatEntity, String columnName,
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity)
    {
        SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();

        schemaColumnEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        schemaColumnEntity.setName(columnName);
        schemaColumnEntity.setType(AbstractDaoTest.COLUMN_DATA_TYPE);
        schemaColumnEntity.setBusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity);

        return schemaColumnDao.saveAndRefresh(schemaColumnEntity);
    }

    /**
     * Returns a list of schema columns that use hard coded test values.
     *
     * @return the list of test schema column entities
     */
    public List<SchemaColumn> getTestPartitionColumns()
    {
        return getTestSchemaColumns("PRTN_CLMN", AbstractDaoTest.PARTITION_COLUMNS);
    }

    /**
     * Returns a list of schema partition columns that use hard coded test values.
     *
     * @return the list of test schema partition columns
     */
    public List<SchemaColumn> getTestPartitionColumns(String randomSuffix)
    {
        List<SchemaColumn> partitionColumns = new ArrayList<>();

        // Add first 3 partition column matching to regular partition columns.
        partitionColumns.addAll(getTestSchemaColumns(AbstractDaoTest.SCHEMA_COLUMN_NAME_PREFIX, 0, 3, randomSuffix));

        // Add the remaining partition columns.
        partitionColumns.addAll(getTestSchemaColumns(AbstractDaoTest.SCHEMA_PARTITION_COLUMN_NAME_PREFIX, 3, AbstractDaoTest.MAX_PARTITIONS - 3, randomSuffix));

        // Update top level partition column name to match the business object format partition key.
        partitionColumns.get(0).setName(AbstractDaoTest.PARTITION_KEY);

        return partitionColumns;
    }

    /**
     * Returns a list of schema columns that use hard coded test values.
     *
     * @param randomSuffix the random suffix
     *
     * @return the list of test schema columns
     */
    public List<SchemaColumn> getTestSchemaColumns(String randomSuffix)
    {
        return getTestSchemaColumns(AbstractDaoTest.SCHEMA_COLUMN_NAME_PREFIX, 0, AbstractDaoTest.MAX_COLUMNS, randomSuffix);
    }

    /**
     * Returns a list of schema columns that use passed attributes and hard coded test values.
     *
     * @param columnNamePrefix the column name prefix to use for the test columns
     * @param offset the offset index to start generating columns with
     * @param numColumns the number of columns
     * @param randomSuffix the random suffix
     *
     * @return the list of test schema columns
     */
    public List<SchemaColumn> getTestSchemaColumns(String columnNamePrefix, Integer offset, Integer numColumns, String randomSuffix)
    {
        // Build a list of schema columns.
        List<SchemaColumn> columns = new ArrayList<>();

        for (int i = 0; i < numColumns; i++)
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            columns.add(schemaColumn);
            // Set a value for the required column name field.
            schemaColumn.setName(String.format("%s-%d%s", columnNamePrefix, i + offset, randomSuffix));
            // Set a value for the required column type field.
            schemaColumn.setType(String.format("Type-%d", i + offset));
            // Set a value for the optional column size field for every other column.
            schemaColumn.setSize(i % 2 == 0 ? null : String.format("Size-%d", i + offset));
            // Set a value for the optional column required flag for each two out of 3 columns with the flag value alternating between true and false.
            schemaColumn.setRequired(i % 3 == 0 ? null : i % 2 == 0);
            // Set a value for the optional default value field for every other column.
            schemaColumn.setDefaultValue(i % 2 == 0 ? null : String.format("Clmn-Dflt-Value-%d%s", i, randomSuffix));
            // Set a value for the optional column size field for every other column.
            schemaColumn.setDescription(i % 2 == 0 ? null : String.format("Clmn-Desc-%d%s", i, randomSuffix));
        }

        return columns;
    }

    /**
     * Returns a list of schema columns that use hard coded test values.
     *
     * @return the list of test schema column entities
     */
    public List<SchemaColumn> getTestSchemaColumns()
    {
        return getTestSchemaColumns("COLUMN", AbstractDaoTest.SCHEMA_COLUMNS);
    }

    /**
     * Returns a list of schema columns that use hard coded test values.
     *
     * @param columnNamePrefix the column name prefix
     * @param schemaColumnDataTypes the list of schema column data types
     *
     * @return the list of test schema column entities
     */
    public List<SchemaColumn> getTestSchemaColumns(String columnNamePrefix, String[][] schemaColumnDataTypes)
    {
        // Build a list of schema columns.
        List<SchemaColumn> schemaColumns = new ArrayList<>();

        int index = 1;
        for (String[] schemaColumnDataType : schemaColumnDataTypes)
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumns.add(schemaColumn);
            String columnName = String.format("%s%03d", columnNamePrefix, index);
            schemaColumn.setName(columnName);
            schemaColumn.setType(schemaColumnDataType[0]);
            schemaColumn.setSize(schemaColumnDataType[1]);
            index++;
        }

        // Column comment is an optional field, so provide comment for the second column only.
        schemaColumns.get(1).setDescription(
            String.format("This is '%s' column. Here are \\'single\\' and \"double\" quotes along with a backslash \\.", schemaColumns.get(1).getName()));

        return schemaColumns;
    }
}
