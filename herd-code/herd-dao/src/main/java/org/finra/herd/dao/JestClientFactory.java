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
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import javax.net.ssl.SSLContext;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import io.searchbox.client.JestClient;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.dto.ConfigurationValue;

@Component
public class JestClientFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JestClientFactory.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Builds and returns a JEST client.
     *
     * @return the configured JEST client
     */
    public JestClient getJestClient()
    {
        // Retrieve the configuration values used for setting up an Elasticsearch JEST client.
        final String esRegionName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_AWS_REGION_NAME);
        final String hostname = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DOMAIN_REST_CLIENT_HOSTNAME);
        final int port = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DOMAIN_REST_CLIENT_PORT, Integer.class);
        final String scheme = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DOMAIN_REST_CLIENT_SCHEME);
        final String serverUri = String.format("%s://%s:%d", scheme, hostname, port);
        final int connectionTimeout = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT, Integer.class);
        final int readTimeout = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT, Integer.class);

        LOGGER.info("Elasticsearch REST Client Settings:  scheme={}, hostname={}, port={}, serverUri={}", scheme, hostname, port, serverUri);

        DefaultAWSCredentialsProviderChain awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final AWSSigner awsSigner = new AWSSigner(awsCredentialsProvider, esRegionName, "es", () -> LocalDateTime.now(ZoneOffset.UTC));

        final AWSSigningRequestInterceptor requestInterceptor = new AWSSigningRequestInterceptor(awsSigner);

        JestClientFactoryStaticInner jestClientFactory = new JestClientFactoryStaticInner(requestInterceptor);

        if (StringUtils.equalsIgnoreCase(scheme, "https"))
        {
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
                sslSocketFactory(sslSocketFactory).multiThreaded(true).build());
        }
        else
        {
            jestClientFactory.setHttpClientConfig(
                new HttpClientConfig.Builder(serverUri).connTimeout(connectionTimeout).readTimeout(readTimeout).multiThreaded(true).build());
        }

        return jestClientFactory.getObject();
    }

    /**
     * Static inner class used to extend the JestClientFactory. This class overrides the configure http client methods to add the AWS signing request
     * interceptor which will add the IAM role APP_DATAMGT to the JestClientFactory https requests.
     */
    private static class JestClientFactoryStaticInner extends io.searchbox.client.JestClientFactory
    {
        final AWSSigningRequestInterceptor requestInterceptor;

        public JestClientFactoryStaticInner(AWSSigningRequestInterceptor requestInterceptor)
        {
            this.requestInterceptor = requestInterceptor;
        }

        @Override
        protected HttpClientBuilder configureHttpClient(HttpClientBuilder builder)
        {
            builder.addInterceptorLast(requestInterceptor);
            return builder;
        }

        @Override
        protected HttpAsyncClientBuilder configureHttpClient(HttpAsyncClientBuilder builder)
        {
            builder.addInterceptorLast(requestInterceptor);
            return builder;
        }
    }
}
