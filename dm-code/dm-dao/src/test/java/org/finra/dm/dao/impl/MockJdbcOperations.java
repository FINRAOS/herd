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
package org.finra.dm.dao.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.finra.dm.dao.JdbcOperations;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * Mocked implementation of {@link JdbcOperations}. Does not actually use {@link JdbcTemplate}. Instead, a predefined result is returned.
 */
public class MockJdbcOperations implements JdbcOperations
{
    private static final Logger LOGGER = Logger.getLogger(MockJdbcOperations.class);

    /**
     * Case1:
     * Returns success with result 1
     */
    public static final String CASE_1_SQL = "case1";

    /**
     * Case2:
     * Throws DataIntegrityViolationException wrapping a SQLException
     */
    public static final String CASE_2_SQL = "case2";

    /**
     * Case2:
     * Throws CannotGetJdbcConnectionException wrapping a SQLException
     */
    public static final String CASE_3_SQL = "case3";

    @Override
    public int update(JdbcTemplate jdbcTemplate, String sql)
    {
        LOGGER.debug("sql = " + sql);
        if (CASE_1_SQL.equals(sql))
        {
            return 1;
        }
        else if (CASE_2_SQL.equals(sql))
        {
            throw new DataIntegrityViolationException("test", new SQLException("test DataIntegrityViolationException cause"));
        }
        else if (CASE_3_SQL.equals(sql))
        {
            throw new CannotGetJdbcConnectionException("test", new SQLException("test CannotGetJdbcConnectionException cause"));
        }

        return 0;
    }

    /**
     * Executes query based on some predefined sql strings.
     * 
     * CASE_1:
     * - Runs extractor on a result set which has 3 columns [COL1, COL2, COL3] and 2 rows [A, B, C] and [D, E, F]
     * CASE_2:
     * - Throws a DataIntegrityViolationException
     */
    @SuppressWarnings("resource")
    @Override
    public <T> T query(JdbcTemplate jdbcTemplate, String sql, ResultSetExtractor<T> resultSetExtractor)
    {
        LOGGER.debug("sql = " + sql);

        MockResultSet mockResultSet = new MockResultSet();

        List<List<String>> rows = new ArrayList<>();

        MockResultSetMetaData mockResultSetMetaData = new MockResultSetMetaData();

        if (CASE_1_SQL.equals(sql))
        {
            mockResultSetMetaData.setColumnNames(Arrays.asList("COL1", "COL2", "COL3"));

            rows.add(Arrays.asList("A", "B", "C"));
            rows.add(Arrays.asList("D", "E", "F"));
        }
        else if (CASE_2_SQL.equals(sql))
        {
            throw new DataIntegrityViolationException("test", new SQLException("test DataIntegrityViolationException cause"));
        }

        try
        {
            mockResultSet.setRowIterator(rows.iterator());
            mockResultSet.setMockResultSetMetaData(mockResultSetMetaData);
            return resultSetExtractor.extractData(mockResultSet);
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
    }
}
