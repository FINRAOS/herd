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

import java.util.Map;

import com.amazonaws.ClientConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.CredStashFactory;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.credstash.CredStash;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.model.dto.ConfigurationValue;

@Component
public class CredStashHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CredStashHelper.class);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private CredStashFactory credStashFactory;

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * Gets a password from the credstash.
     *
     * @param credStashEncryptionContext the encryption context
     * @param credentialName the credential name
     *
     * @return the password
     * @throws CredStashGetCredentialFailedException if CredStash fails to get a credential
     */
    @Retryable(maxAttempts = 3, value = CredStashGetCredentialFailedException.class, backoff = @Backoff(delay = 5000, multiplier = 2))
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public String getCredentialFromCredStash(String credStashEncryptionContext, String credentialName) throws CredStashGetCredentialFailedException
    {
        // Get AWS region and table name for the credstash.
        String credStashAwsRegion = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        String credStashTableName = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);

        // Log configuration values and input parameters.
        LOGGER.info("credStashAwsRegion={} credStashTableName={} credStashEncryptionContext={} credentialName={}", credStashAwsRegion, credStashTableName,
            credStashEncryptionContext, credentialName);

        // Get the AWS client configuration.
        ClientConfiguration clientConfiguration = awsHelper.getClientConfiguration(awsHelper.getAwsParamsDto());

        // Get the credstash interface for getting a credential from credstash.
        CredStash credstash = credStashFactory.getCredStash(credStashAwsRegion, credStashTableName, clientConfiguration);

        // Try to obtain the credential from credstash.
        String password = null;
        String errorMessage = null;
        try
        {
            // Convert the JSON config file version of the encryption context to a Java Map class.
            @SuppressWarnings("unchecked")
            Map<String, String> credstashEncryptionContextMap = jsonHelper.unmarshallJsonToObject(Map.class, credStashEncryptionContext);
            // Get password value from the credstash.
            password = credstash.getCredential(credentialName, credstashEncryptionContextMap);
        }
        catch (Exception exception)
        {
            LOGGER.error("Caught exception when attempting to get a credential value from CredStash.", exception);
            errorMessage = exception.getMessage();
        }

        // If password value is empty and could not be obtained as credential from the credstash, then throw a CredStashGetCredentialFailedException.
        if (StringUtils.isEmpty(password))
        {
            throw new CredStashGetCredentialFailedException(String.format("Failed to obtain credential from credstash.%s " +
                    "credStashAwsRegion=%s credStashTableName=%s credStashEncryptionContext=%s credentialName=%s",
                StringUtils.isNotBlank(errorMessage) ? " Reason: " + errorMessage : "", credStashAwsRegion, credStashTableName, credStashEncryptionContext,
                credentialName));
        }

        // Return the retrieved password value.
        return password;
    }
}
