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

import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.net.ssl.SSLContext;

import com.amazonaws.ClientConfiguration;
import io.searchbox.client.JestClient;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.credstash.CredStash;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.ConfigurationValue;

@Component
public class JestClientFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JestClientFactory.class);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private CredStashFactory credStashFactory;

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * Builds and returns a JEST client.
     *
     * @return the configured JEST client
     */
    public JestClient getJestClient()
    {
        // Retrieve the configuration values used for setting up an Elasticsearch JEST client.
        final String hostname = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME);
        final int port = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class);
        final String scheme = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME);
        final String serverUri = String.format("%s://%s:%d", scheme, hostname, port);
        final int connectionTimeout = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class);
        final int readTimeout = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class);

        LOGGER.info("Elasticsearch REST Client Settings:  scheme={}, hostname={}, port={}, serverUri={}", scheme, hostname, port, serverUri);

        io.searchbox.client.JestClientFactory jestClientFactory = new io.searchbox.client.JestClientFactory();

        if (StringUtils.equalsIgnoreCase(scheme, "https"))
        {
            final String userName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERNAME);
            final String credentialName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME);

            String password;
            try
            {
                password = getCredentialFromCredStash(credentialName);
            }
            catch (CredStashGetCredentialFailedException e)
            {
                throw new IllegalStateException(e);
            }

            SSLConnectionSocketFactory sslSocketFactory;
            try
            {
                sslSocketFactory = new SSLConnectionSocketFactory(SSLContext.getDefault(), NoopHostnameVerifier.INSTANCE);
            }
            catch (NoSuchAlgorithmException e)
            {
                throw new IllegalStateException(e);
            }

            jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder(serverUri).connTimeout(connectionTimeout).readTimeout(readTimeout).
                defaultCredentials(userName, password).sslSocketFactory(sslSocketFactory).multiThreaded(true).build());
        }
        else
        {
            jestClientFactory.setHttpClientConfig(
                new HttpClientConfig.Builder(serverUri).connTimeout(connectionTimeout).readTimeout(readTimeout).multiThreaded(true).build());
        }

        return jestClientFactory.getObject();
    }

    /**
     * Gets a password from the credstash.
     *
     * @param credentialName the credential name
     *
     * @return the password
     * @throws CredStashGetCredentialFailedException if CredStash fails to get a credential
     */
    @Retryable(maxAttempts = 3, value = CredStashGetCredentialFailedException.class, backoff = @Backoff(delay = 5000, multiplier = 2))
    private String getCredentialFromCredStash(String credentialName) throws CredStashGetCredentialFailedException
    {
        // Get the credstash table name and credential names for the keystore and truststore
        String credstashEncryptionContext = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT);
        String credstashAwsRegion = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        String credstashTableName = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);

        LOGGER.info("credstashTableName={}", credstashTableName);
        LOGGER.info("credentialName={}", credentialName);

        // Get the AWS client configuration.
        ClientConfiguration clientConfiguration = awsHelper.getClientConfiguration(awsHelper.getAwsParamsDto());

        // Get the keystore and truststore passwords from Credstash
        CredStash credstash = credStashFactory.getCredStash(credstashAwsRegion, credstashTableName, clientConfiguration);

        String password = null;

        // Try to obtain the credentials from cred stash
        try
        {
            // Convert the JSON config file version of the encryption context to a Java Map class
            @SuppressWarnings("unchecked")
            Map<String, String> credstashEncryptionContextMap = jsonHelper.unmarshallJsonToObject(Map.class, credstashEncryptionContext);
            // Get the keystore and truststore passwords from credstash
            password = credstash.getCredential(credentialName, credstashEncryptionContextMap);
        }
        catch (Exception exception)
        {
            LOGGER.error("Caught exception when attempting to get a credential value from CredStash", exception);
        }

        // If either the keystorePassword or truststorePassword values are empty and could not be obtained as credentials from cred stash,
        // then throw a new CredStashGetCredentialFailedException
        if (StringUtils.isEmpty(password))
        {
            throw new CredStashGetCredentialFailedException("Failed to obtain the keystore or truststore credential from cred stash.");
        }

        // Return the keystore and truststore passwords in a map
        return password;
    }
}
