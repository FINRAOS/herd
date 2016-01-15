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

import java.util.Arrays;

import org.finra.herd.dao.impl.MockJdbcOperations;
import org.finra.herd.model.api.xml.JdbcStatementResultSet;
import org.finra.herd.model.api.xml.JdbcStatementResultSetRow;

import org.junit.Assert;
import org.junit.Test;

public class JdbcDaoTest extends AbstractDaoTest
{
    @Test
    public void testQueryNoMaxResults()
    {
        JdbcStatementResultSet resultSet = jdbcDao.query(null, MockJdbcOperations.CASE_1_SQL, null);

        Assert.assertNotNull("resultSet", resultSet);
        Assert.assertNotNull("resultSet columnNames", resultSet.getColumnNames());
        Assert.assertEquals("resultSet columnNames", Arrays.asList("COL1", "COL2", "COL3"), resultSet.getColumnNames());
        Assert.assertNotNull("resultSet rows", resultSet.getRows());
        Assert.assertEquals("resultSet rows size", 2, resultSet.getRows().size());
        {
            JdbcStatementResultSetRow row = resultSet.getRows().get(0);
            Assert.assertNotNull("resultSet rows[0]", row);
            Assert.assertNotNull("resultSet rows[0] columns", row.getColumns());
            Assert.assertEquals("resultSet rows[0] columns", Arrays.asList("A", "B", "C"), row.getColumns());
        }
        {
            JdbcStatementResultSetRow row = resultSet.getRows().get(1);
            Assert.assertNotNull("resultSet rows[1]", row);
            Assert.assertNotNull("resultSet rows[1] columns", row.getColumns());
            Assert.assertEquals("resultSet rows[1] columns", Arrays.asList("D", "E", "F"), row.getColumns());
        }
    }

    @Test
    public void testQueryWithMaxResultsLimitsResults()
    {
        JdbcStatementResultSet resultSet = jdbcDao.query(null, MockJdbcOperations.CASE_1_SQL, 1);

        Assert.assertNotNull("resultSet", resultSet);
        Assert.assertNotNull("resultSet columnNames", resultSet.getColumnNames());
        Assert.assertEquals("resultSet columnNames", Arrays.asList("COL1", "COL2", "COL3"), resultSet.getColumnNames());
        Assert.assertNotNull("resultSet rows", resultSet.getRows());
        Assert.assertEquals("resultSet rows size", 1, resultSet.getRows().size());
        {
            JdbcStatementResultSetRow row = resultSet.getRows().get(0);
            Assert.assertNotNull("resultSet rows[0]", row);
            Assert.assertNotNull("resultSet rows[0] columns", row.getColumns());
            Assert.assertEquals("resultSet rows[0] columns", Arrays.asList("A", "B", "C"), row.getColumns());
        }
    }
    @Test
    public void testQueryWithMaxResults()
    {
        JdbcStatementResultSet resultSet = jdbcDao.query(null, MockJdbcOperations.CASE_1_SQL, 2);

        Assert.assertNotNull("resultSet", resultSet);
        Assert.assertNotNull("resultSet columnNames", resultSet.getColumnNames());
        Assert.assertEquals("resultSet columnNames", Arrays.asList("COL1", "COL2", "COL3"), resultSet.getColumnNames());
        Assert.assertNotNull("resultSet rows", resultSet.getRows());
        Assert.assertEquals("resultSet rows size", 2, resultSet.getRows().size());
    }
}
