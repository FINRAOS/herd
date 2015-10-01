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

import org.finra.dm.dao.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * Default implementation of {@link JdbcOperations}.
 */
public class JdbcOperationsImpl implements JdbcOperations
{
    /**
     * {@link JdbcTemplate#update(String)}
     */
    @Override
    public int update(JdbcTemplate jdbcTemplate, String sql)
    {
        return jdbcTemplate.update(sql);
    }

    /**
     * {@link JdbcTemplate#query(String, ResultSetExtractor)}
     */
    @Override
    public <T> T query(JdbcTemplate jdbcTemplate, String sql, ResultSetExtractor<T> resultSetExtractor)
    {
        return jdbcTemplate.query(sql, resultSetExtractor);
    }
}
