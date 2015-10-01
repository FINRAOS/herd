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
package org.finra.dm.service;

import org.finra.dm.model.api.xml.JdbcExecutionRequest;
import org.finra.dm.model.api.xml.JdbcExecutionResponse;

public interface JdbcService
{
    /**
     * Executes JDBC statements in the request.
     * The entire request will be executed in a single transaction, that is, if one of the statement fails, all previously executed statements will be rolled
     * back.
     * Each statement that is executed will be returned in the response with the appropriate status and result.
     * Depending on the type and status of the statement, the result may vary.
     * For UPDATE statements:
     * - On SUCCESS, the result will be the number of rows affected.
     * - On ERROR, the result will be the SQL exception, which includes the exception type and the message.
     * - On SKIPPED, the result will not be present, since the statement never executed.
     * 
     * @param jdbcExecutionRequest The JDBC execution request
     * @return The JDBC execution response
     */
    JdbcExecutionResponse executeJdbc(JdbcExecutionRequest jdbcExecutionRequest);
}
