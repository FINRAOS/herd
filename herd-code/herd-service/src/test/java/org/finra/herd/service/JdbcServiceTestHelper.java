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
package org.finra.herd.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import org.finra.herd.dao.impl.MockJdbcOperations;
import org.finra.herd.model.api.xml.JdbcConnection;
import org.finra.herd.model.api.xml.JdbcDatabaseType;
import org.finra.herd.model.api.xml.JdbcExecutionRequest;
import org.finra.herd.model.api.xml.JdbcStatement;
import org.finra.herd.model.api.xml.JdbcStatementType;

@Component
public class JdbcServiceTestHelper
{
    /**
     * Creates a default test JDBC connection which is guaranteed to work. The connection points to the in-memory database setup as part of DAO mocks.
     *
     * @return a valid JDBC connection
     */
    public JdbcConnection createDefaultJdbcConnection()
    {
        JdbcConnection jdbcConnection = new JdbcConnection();
        jdbcConnection.setUrl("jdbc:h2:mem:herdTestDb");
        jdbcConnection.setUsername("");
        jdbcConnection.setPassword("");
        jdbcConnection.setDatabaseType(JdbcDatabaseType.POSTGRES);
        return jdbcConnection;
    }

    /**
     * Creates a default JDBC execution request which is guaranteed to work. The request contains a single statement of QUERY type.
     *
     * @return a valid JDBC request
     */
    public JdbcExecutionRequest createDefaultQueryJdbcExecutionRequest()
    {
        JdbcConnection jdbcConnection = createDefaultJdbcConnection();
        List<JdbcStatement> jdbcStatements = createDefaultQueryJdbcStatements();
        JdbcExecutionRequest jdbcExecutionRequest = createJdbcExecutionRequest(jdbcConnection, jdbcStatements);
        return jdbcExecutionRequest;
    }

    /**
     * Creates a default JDBC execution request which is guaranteed to work. The request contains a single statement of UPDATE type.
     *
     * @return a valid JDBC request
     */
    public JdbcExecutionRequest createDefaultUpdateJdbcExecutionRequest()
    {
        JdbcConnection jdbcConnection = createDefaultJdbcConnection();
        List<JdbcStatement> jdbcStatements = createDefaultUpdateJdbcStatements();
        JdbcExecutionRequest jdbcExecutionRequest = createJdbcExecutionRequest(jdbcConnection, jdbcStatements);
        return jdbcExecutionRequest;
    }

    /**
     * Creates a JDBC request with the specified values.
     *
     * @param jdbcConnection JDBC connection
     * @param jdbcStatements JDBC statements
     *
     * @return an execution request.
     */
    public JdbcExecutionRequest createJdbcExecutionRequest(JdbcConnection jdbcConnection, List<JdbcStatement> jdbcStatements)
    {
        JdbcExecutionRequest jdbcExecutionRequest = new JdbcExecutionRequest();
        jdbcExecutionRequest.setConnection(jdbcConnection);
        jdbcExecutionRequest.setStatements(jdbcStatements);
        return jdbcExecutionRequest;
    }

    /**
     * Returns a valid list of JDBC QUERY statements.It contains only 1 statement, and the statement is CASE_1_SQL in mock JDBC (success, result 1)
     *
     * @return list of statements
     */
    private List<JdbcStatement> createDefaultQueryJdbcStatements()
    {
        List<JdbcStatement> jdbcStatements = new ArrayList<>();
        {
            JdbcStatement jdbcStatement = new JdbcStatement();
            jdbcStatement.setType(JdbcStatementType.QUERY);
            jdbcStatement.setSql(MockJdbcOperations.CASE_1_SQL);
            jdbcStatements.add(jdbcStatement);
        }
        return jdbcStatements;
    }

    /**
     * Returns a valid list of JDBC UPDATE statements. It contains only 1 statement, and the statement is CASE_1_SQL in mock JDBC (success, result 1)
     *
     * @return list of statements.
     */
    private List<JdbcStatement> createDefaultUpdateJdbcStatements()
    {
        List<JdbcStatement> jdbcStatements = new ArrayList<>();
        {
            JdbcStatement jdbcStatement = new JdbcStatement();
            jdbcStatement.setType(JdbcStatementType.UPDATE);
            jdbcStatement.setSql(MockJdbcOperations.CASE_1_SQL);
            jdbcStatements.add(jdbcStatement);
        }
        return jdbcStatements;
    }
}
