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
package org.finra.dm.dao;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * Wrapper interface for {@link JdbcTemplate} operations.
 */
public interface JdbcOperations
{
    /**
     * Executes {@link JdbcTemplate#update(String)}
     * 
     * @param jdbcTemplate JDBC template to use
     * @param sql SQL statement to execute
     * @return number of rows updated
     */
    int update(JdbcTemplate jdbcTemplate, String sql);

    /**
     * Executes {@link JdbcTemplate#query(String, ResultSetExtractor)}
     * 
     * @param jdbcTemplate JDBC template to use
     * @param sql SQL statement to execute
     * @param resultSetExtractor {@link ResultSetExtractor}
     * @return The object constructed by the given {@link ResultSetExtractor}
     */
    <T> T query(JdbcTemplate jdbcTemplate, String sql, ResultSetExtractor<T> resultSetExtractor);
}
