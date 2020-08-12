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
package org.finra.herd.dao.helper;

import static org.finra.herd.core.AbstractCoreTest.BLANK_TEXT;
import static org.finra.herd.dao.AbstractDaoTest.AWS_ACCOUNT_ID;
import static org.finra.herd.dao.AbstractDaoTest.AWS_ASSUMED_ROLE_ACCESS_KEY;
import static org.finra.herd.dao.AbstractDaoTest.AWS_ASSUMED_ROLE_SECRET_KEY;
import static org.finra.herd.dao.AbstractDaoTest.AWS_ASSUMED_ROLE_SESSION_TOKEN;
import static org.finra.herd.dao.AbstractDaoTest.AWS_REGION_NAME_US_EAST_1;
import static org.finra.herd.dao.AbstractDaoTest.AWS_ROLE_ARN;
import static org.finra.herd.dao.AbstractDaoTest.HTTP_PROXY_HOST;
import static org.finra.herd.dao.AbstractDaoTest.HTTP_PROXY_PORT;
import static org.finra.herd.dao.AbstractDaoTest.NO_AWS_ACCESS_KEY;
import static org.finra.herd.dao.AbstractDaoTest.NO_AWS_SECRET_KEY;
import static org.finra.herd.dao.AbstractDaoTest.NO_S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME;
import static org.finra.herd.dao.AbstractDaoTest.NO_SESSION_TOKEN;
import static org.finra.herd.dao.AbstractDaoTest.S3_STAGING_BUCKET_NAME;
import static org.finra.herd.dao.AbstractDaoTest.S3_STAGING_RESOURCE_BASE;
import static org.finra.herd.dao.AbstractDaoTest.S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME;
import static org.finra.herd.dao.AbstractDaoTest.S3_URL_PATH_DELIMITER;
import static org.finra.herd.dao.AbstractDaoTest.S3_URL_PROTOCOL;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.services.securitytoken.model.Credentials;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.RetryPolicyFactory;
import org.finra.herd.dao.StsDao;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmrParamsDto;
import org.finra.herd.model.jpa.TrustingAccountEntity;

/**
 * This class tests functionality within the EMR Helper implementation.
 */
public class EmrHelperMockTest
{
    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private EmrDao emrDao;

    @InjectMocks
    private EmrHelper emrHelper;

    @Mock
    private HerdStringHelper herdStringHelper;

    @Mock
    private RetryPolicyFactory retryPolicyFactory;

    @Mock
    private StsDao stsDao;

    @Mock
    private TrustingAccountDaoHelper trustingAccountDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetEmrParamsDtoByAccountId()
    {
        // Create a trusting account entity.
        TrustingAccountEntity trustingAccountEntity = new TrustingAccountEntity();
        trustingAccountEntity.setRoleArn(AWS_ROLE_ARN);
        trustingAccountEntity.setStagingBucketName(S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);

        // Create AWS credentials.
        Credentials credentials = new Credentials();
        credentials.setAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        credentials.setSecretAccessKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        credentials.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);
        credentials.setExpiration(AbstractCoreTest.getRandomDate());

        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_HOST)).thenReturn(HTTP_PROXY_HOST);
        when(configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_PORT, Integer.class)).thenReturn(HTTP_PROXY_PORT);
        when(configurationHelper.getProperty(ConfigurationValue.AWS_REGION_NAME)).thenReturn(AWS_REGION_NAME_US_EAST_1);
        when(trustingAccountDaoHelper.getTrustingAccountEntity(AWS_ACCOUNT_ID)).thenReturn(trustingAccountEntity);
        when(stsDao.getTemporarySecurityCredentials(any(AwsParamsDto.class), any(String.class), eq(AWS_ROLE_ARN), eq(3600), isNull())).thenReturn(credentials);

        // Call the method under test. In order to test trimming, add whitespace characters to the account identifier.
        EmrParamsDto result = emrHelper.getEmrParamsDtoByAccountId(BLANK_TEXT + AWS_ACCOUNT_ID + BLANK_TEXT);

        // Validate the returned object.
        assertEquals(
            new EmrParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT,
                AWS_REGION_NAME_US_EAST_1, S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME), result);

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.HTTP_PROXY_HOST);
        verify(configurationHelper).getProperty(ConfigurationValue.HTTP_PROXY_PORT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.AWS_REGION_NAME);
        verify(trustingAccountDaoHelper).getTrustingAccountEntity(AWS_ACCOUNT_ID);
        verify(stsDao).getTemporarySecurityCredentials(any(AwsParamsDto.class), any(String.class), eq(AWS_ROLE_ARN), eq(3600), isNull());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetEmrParamsDtoByAccountIdWhenAccountIdIsBlank()
    {
        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_HOST)).thenReturn(HTTP_PROXY_HOST);
        when(configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_PORT, Integer.class)).thenReturn(HTTP_PROXY_PORT);
        when(configurationHelper.getProperty(ConfigurationValue.AWS_REGION_NAME)).thenReturn(AWS_REGION_NAME_US_EAST_1);

        // Call the method under test.
        AwsParamsDto result = emrHelper.getEmrParamsDtoByAccountId(BLANK_TEXT);

        // Validate the returned object.
        assertEquals(new EmrParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1,
            NO_S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME), result);

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.HTTP_PROXY_HOST);
        verify(configurationHelper).getProperty(ConfigurationValue.HTTP_PROXY_PORT, Integer.class);
        verify(configurationHelper).getProperty(ConfigurationValue.AWS_REGION_NAME);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetS3StagingLocation()
    {
        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.S3_URL_PROTOCOL)).thenReturn(S3_URL_PROTOCOL);
        when(configurationHelper.getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER)).thenReturn(S3_URL_PATH_DELIMITER);
        when(configurationHelper.getProperty(ConfigurationValue.S3_STAGING_RESOURCE_BASE)).thenReturn(S3_STAGING_RESOURCE_BASE);

        // Call the method under test.
        String result = emrHelper.getS3StagingLocation(S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME);

        // Validate the returned object.
        assertEquals(String.format("%s%s%s%s", S3_URL_PROTOCOL, S3_TRUSTING_ACCOUNT_STAGING_BUCKET_NAME, S3_URL_PATH_DELIMITER, S3_STAGING_RESOURCE_BASE),
            result);

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.S3_URL_PROTOCOL);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_STAGING_RESOURCE_BASE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetS3StagingLocationWhenTrustingAccountStagingBucketNameIsBlank()
    {
        // Mock the external calls.
        when(configurationHelper.getProperty(ConfigurationValue.S3_STAGING_BUCKET_NAME)).thenReturn(S3_STAGING_BUCKET_NAME);
        when(configurationHelper.getProperty(ConfigurationValue.S3_URL_PROTOCOL)).thenReturn(S3_URL_PROTOCOL);
        when(configurationHelper.getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER)).thenReturn(S3_URL_PATH_DELIMITER);
        when(configurationHelper.getProperty(ConfigurationValue.S3_STAGING_RESOURCE_BASE)).thenReturn(S3_STAGING_RESOURCE_BASE);

        // Call the method under test.
        String result = emrHelper.getS3StagingLocation(BLANK_TEXT);

        // Validate the returned object.
        assertEquals(String.format("%s%s%s%s", S3_URL_PROTOCOL, S3_STAGING_BUCKET_NAME, S3_URL_PATH_DELIMITER, S3_STAGING_RESOURCE_BASE), result);

        // Verify the external calls.
        verify(configurationHelper).getProperty(ConfigurationValue.S3_STAGING_BUCKET_NAME);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_URL_PROTOCOL);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER);
        verify(configurationHelper).getProperty(ConfigurationValue.S3_STAGING_RESOURCE_BASE);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(configurationHelper, emrDao, herdStringHelper, retryPolicyFactory, stsDao, trustingAccountDaoHelper);
    }
}
