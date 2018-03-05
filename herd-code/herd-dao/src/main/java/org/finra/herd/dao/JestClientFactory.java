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

import javax.net.ssl.SSLContext;

import io.searchbox.client.JestClient;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.CredStashHelper;
import org.finra.herd.model.dto.ConfigurationValue;

@Component
public class JestClientFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JestClientFactory.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private CredStashHelper credStashHelper;

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
            final String credstashEncryptionContext = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT);

            String password;
            try
            {
                password = credStashHelper.getCredentialFromCredStash(credstashEncryptionContext, credentialName);
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
}
