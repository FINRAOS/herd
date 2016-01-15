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
package org.finra.herd.dao.impl;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.finra.herd.dao.JdbcDao;
import org.finra.herd.dao.JdbcOperations;
import org.finra.herd.model.api.xml.JdbcStatementResultSet;
import org.finra.herd.model.api.xml.JdbcStatementResultSetRow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Repository;

/**
 * Default implementation of {@link JdbcDao}. Delegates to {@link JdbcOperations}.
 */
@Repository
public class JdbcDaoImpl implements JdbcDao
{
    @Autowired
    private JdbcOperations jdbcOperations;

    /**
     * Delegates to {@link JdbcOperations#update(JdbcTemplate, String)}
     */
    @Override
    public int update(JdbcTemplate jdbcTemplate, String sql)
    {
        return jdbcOperations.update(jdbcTemplate, sql);
    }

    /**
     * Delegates to {@link JdbcOperations#query(JdbcTemplate, String, ResultSetExtractor)} where the {@link ResultSetExtractor} converts the {@link ResultSet}
     * into {@link JdbcStatementResultSet}.
     */
    @Override
    public JdbcStatementResultSet query(JdbcTemplate jdbcTemplate, String sql, final Integer maxResult)
    {
        return jdbcOperations.query(jdbcTemplate, sql, new ResultSetExtractor<JdbcStatementResultSet>()
        {
            @Override
            public JdbcStatementResultSet extractData(ResultSet resultSet) throws SQLException, DataAccessException
            {
                JdbcStatementResultSet jdbcStatementResultSet = new JdbcStatementResultSet();

                List<String> columnNames = getColumnNames(resultSet.getMetaData());
                jdbcStatementResultSet.setColumnNames(columnNames);

                List<JdbcStatementResultSetRow> rows = getRows(resultSet, maxResult);
                jdbcStatementResultSet.setRows(rows);

                return jdbcStatementResultSet;
            }
        });
    }

    /**
     * Gets the column names from the given {@link ResultSetMetaData}.
     * 
     * @param resultSetMetaData {@link ResultSetMetaData}
     * @return {@link List} of column names
     * @throws SQLException when there is an error reading from the {@link ResultSetMetaData}
     */
    private List<String> getColumnNames(ResultSetMetaData resultSetMetaData) throws SQLException
    {
        List<String> columnNames = new ArrayList<>();
        int columnCount = resultSetMetaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++)
        {
            String columnName = resultSetMetaData.getColumnName(i);
            columnNames.add(columnName);
        }
        return columnNames;
    }

    /**
     * Gets the rows from the given {@link ResultSet}. Optionally, limits the number of rows returned using maxResult.
     * 
     * @param resultSet {@link ResultSet}
     * @param maxResult The maximum number of rows returned
     * @return {@link List} of {@link JdbcStatementResultSetRow}
     * @throws SQLException when there is an error reading from the {@link ResultSet}
     */
    private List<JdbcStatementResultSetRow> getRows(ResultSet resultSet, Integer maxResult) throws SQLException
    {
        List<JdbcStatementResultSetRow> rows = new ArrayList<>();
        int columnCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next())
        {
            JdbcStatementResultSetRow row = new JdbcStatementResultSetRow();
            for (int i = 1; i <= columnCount; i++)
            {
                String column = resultSet.getString(i);
                row.getColumns().add(column);
            }
            rows.add(row);

            // Exit loop if the maxResult is reached.
            if (maxResult != null && resultSet.getRow() == maxResult)
            {
                break;
            }
        }
        return rows;
    }
}
