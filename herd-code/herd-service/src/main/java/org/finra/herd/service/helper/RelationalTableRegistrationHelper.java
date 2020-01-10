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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.SchemaColumn;


/**
 * A helper class for RelationalTableRegistration related code.
 */
@Component
public class RelationalTableRegistrationHelper
{
    /**
     * Method to convert the result set to a list of schema columns.
     *
     * @param resultSet the result set to convert to a list of schema columns.
     *
     * @return a list of schema columns.
     *
     * @throws SQLException accessing the result set may result in an sql exception.
     */
    public List<SchemaColumn> getSchemaColumns(final ResultSet resultSet) throws SQLException
    {
        // Create an empty schema columns list.
        List<SchemaColumn> schemaColumns = new ArrayList<>();

        // While there are further results in the result set create a new schema column and add it to the list.
        while (resultSet.next())
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(resultSet.getString("COLUMN_NAME"));
            schemaColumn.setType(resultSet.getString("TYPE_NAME"));

            // If this is a numeric column then include the decimal digits as part of the size.
            if (resultSet.getString("TYPE_NAME").equalsIgnoreCase("NUMERIC")
                && StringUtils.isNotEmpty(resultSet.getString("DECIMAL_DIGITS")))
            {
                schemaColumn.setSize(resultSet.getString("COLUMN_SIZE") + "," + resultSet.getString("DECIMAL_DIGITS"));
            }
            else
            {
                schemaColumn.setSize(resultSet.getString("COLUMN_SIZE"));
            }

            schemaColumn.setRequired(resultSet.getInt("NULLABLE") == 0);
            schemaColumn.setDefaultValue(resultSet.getString("COLUMN_DEF"));
            schemaColumns.add(schemaColumn);
        }

        return schemaColumns;
    }
}
