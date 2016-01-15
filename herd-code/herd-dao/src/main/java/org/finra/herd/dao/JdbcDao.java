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

import org.finra.herd.model.api.xml.JdbcStatementResultSet;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * DAO layer for executing arbitrary JDBC statements.
 */
public interface JdbcDao
{
    /**
     * Executes a JDBC update.
     * 
     * @param jdbcTemplate JDBC template to use
     * @param sql SQL statement to execute
     * @return number of rows updated
     */
    int update(JdbcTemplate jdbcTemplate, String sql);

    /**
     * Executes a JDBC query. Optionally, the number of rows returned may be limited by setting maxResult.
     * 
     * @param jdbcTemplate JDBC template to use
     * @param sql SQL statement to execute
     * @param maxResult The maximum number of rows returned, or null
     * @return the {@link JdbcStatementResultSet}
     */
    JdbcStatementResultSet query(JdbcTemplate jdbcTemplate, String sql, Integer maxResult);
}
