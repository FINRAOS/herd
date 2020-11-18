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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.impl.StsDaoImpl;
import org.finra.herd.model.dto.AwsParamsDto;

/**
 * This class tests the functionality of security token service DAO.
 */
public class StsDaoTest extends AbstractDaoTest
{
    @Mock
    private AwsHelper awsHelper;

    @InjectMocks
    private StsDaoImpl stsDaoImpl;

    @Mock
    private StsOperations stsOperations;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetTemporarySecurityCredentials()
    {
        // Create an AWS parameters DTO with proxy settings.
        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(HTTP_PROXY_HOST);
        awsParamsDto.setHttpProxyPort(HTTP_PROXY_PORT);
        awsParamsDto.setAwsRegionName(AWS_REGION_NAME);

        // Build AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Specify the duration, in seconds, of the role session.
        int awsRoleDurationSeconds = INTEGER_VALUE;

        // Create an IAM policy.
        Policy policy = new Policy(STRING_VALUE);

        // Create the expected assume role request.
        AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest().withRoleArn(AWS_ROLE_ARN).withRoleSessionName(SESSION_NAME).withPolicy(policy.toJson())
            .withDurationSeconds(awsRoleDurationSeconds);

        // Create AWS credentials for API authentication.
        Credentials credentials = new Credentials();
        credentials.setAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        credentials.setSecretAccessKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        credentials.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

        // Create an assume role result.
        AssumeRoleResult assumeRoleResult = new AssumeRoleResult();
        assumeRoleResult.setCredentials(credentials);

        // Mock the external calls.
        when(awsHelper.getClientConfiguration(awsParamsDto)).thenReturn(clientConfiguration);
        when(stsOperations.assumeRole(any(AWSSecurityTokenServiceClient.class), eq(assumeRoleRequest))).thenReturn(assumeRoleResult);

        // Call the method under test.
        Credentials result = stsDaoImpl.getTemporarySecurityCredentials(awsParamsDto, SESSION_NAME, AWS_ROLE_ARN, awsRoleDurationSeconds, policy);

        // Verify the external calls.
        verify(awsHelper).getClientConfiguration(awsParamsDto);
        verify(stsOperations).assumeRole(any(AWSSecurityTokenServiceClient.class), eq(assumeRoleRequest));
        verifyNoMoreInteractionsHelper();

        // Validate the returned object.
        assertEquals(credentials, result);
    }

    @Test
    public void testGetTemporarySecurityCredentialsMissingOptionalParameters()
    {
        // Create an AWS parameters DTO without proxy settings.
        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setAwsRegionName(AWS_REGION_NAME);

        // Build AWS client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Specify the duration, in seconds, of the role session.
        int awsRoleDurationSeconds = INTEGER_VALUE;

        // Create the expected assume role request.
        AssumeRoleRequest assumeRoleRequest =
            new AssumeRoleRequest().withRoleArn(AWS_ROLE_ARN).withRoleSessionName(SESSION_NAME).withDurationSeconds(awsRoleDurationSeconds);

        // Create AWS credentials for API authentication.
        Credentials credentials = new Credentials();
        credentials.setAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        credentials.setSecretAccessKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        credentials.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

        // Create an assume role result.
        AssumeRoleResult assumeRoleResult = new AssumeRoleResult();
        assumeRoleResult.setCredentials(credentials);

        // Mock the external calls.
        when(awsHelper.getClientConfiguration(awsParamsDto)).thenReturn(clientConfiguration);
        when(stsOperations.assumeRole(any(AWSSecurityTokenServiceClient.class), eq(assumeRoleRequest))).thenReturn(assumeRoleResult);

        // Call the method under test. Please note that we do not specify an IAM policy.
        Credentials result = stsDaoImpl.getTemporarySecurityCredentials(awsParamsDto, SESSION_NAME, AWS_ROLE_ARN, awsRoleDurationSeconds, null);

        // Verify the external calls.
        verify(awsHelper).getClientConfiguration(awsParamsDto);
        verify(stsOperations).assumeRole(any(AWSSecurityTokenServiceClient.class), eq(assumeRoleRequest));
        verifyNoMoreInteractionsHelper();

        // Validate the returned object.
        assertEquals(credentials, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(awsHelper, stsOperations);
    }
}
