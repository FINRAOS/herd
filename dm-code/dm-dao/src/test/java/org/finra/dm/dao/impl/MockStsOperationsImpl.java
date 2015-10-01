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

import static org.junit.Assert.assertNotNull;

import java.util.Date;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

import org.finra.dm.dao.StsOperations;

/**
 * Mock implementation of AWS STS operations.
 */
public class MockStsOperationsImpl implements StsOperations
{
    public static final String MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY = "mock_aws_assumed_role_access_key";
    public static final String MOCK_AWS_ASSUMED_ROLE_SECRET_KEY = "mock_aws_assumed_role_secret_key";
    public static final String MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN = "mock_aws_assumed_role_session_token";

    @Override
    public AssumeRoleResult assumeRole(AWSSecurityTokenServiceClient awsSecurityTokenServiceClient, AssumeRoleRequest assumeRoleRequest)
    {
        assertNotNull(assumeRoleRequest);

        if (assumeRoleRequest.getPolicy() != null && assumeRoleRequest.getPolicy().equals(MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION))
        {
            AmazonServiceException throttlingException = new AmazonServiceException("test throttling exception");
            throttlingException.setErrorCode("ThrottlingException");

            throw throttlingException;
        }

        AssumeRoleResult assumeRoleResult = new AssumeRoleResult();

        assumeRoleResult.setCredentials(new Credentials(MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY, MOCK_AWS_ASSUMED_ROLE_SECRET_KEY, MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN,
            new Date(System.currentTimeMillis() + 1000 * assumeRoleRequest.getDurationSeconds())));

        return assumeRoleResult;
    }
}
