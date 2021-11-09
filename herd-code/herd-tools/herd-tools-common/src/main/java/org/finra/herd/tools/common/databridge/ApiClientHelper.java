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
package org.finra.herd.tools.common.databridge;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.HostnameVerifier;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;

import org.finra.herd.sdk.invoker.ApiClient;
/**
 * A base helper class for ApiClient.
 */
public class ApiClientHelper
{
    /**
     * Rebuild ApiClient.
     *
     * @param apiClient the provided api client
     * @param trustSelfSignedCertificate specifies whether to trust a self-signed certificate
     * @param disableHostnameVerification specifies whether to turn off hostname verification
     *
     * @return the api client
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    public ApiClient rebuildClient(ApiClient apiClient, Boolean trustSelfSignedCertificate, Boolean disableHostnameVerification)
        throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        // Add the JSON serialization support to Jersey.
        ClientConfig conf = buildClientConfig(trustSelfSignedCertificate, disableHostnameVerification);

        // Append SDK's default object mapper.
        JacksonJsonProvider jsonProvider = new JacksonJsonProvider(apiClient.getObjectMapper());
        conf.getSingletons().add(jsonProvider);

        // Build client.
        Client client = Client.create(conf);
        client.addFilter(new GZIPContentEncodingFilter(false));
        if (apiClient.isDebugging())
        {
            client.addFilter(new LoggingFilter());
        }

        apiClient.setHttpClient(client);
        return apiClient;
    }

    private ClientConfig buildClientConfig(Boolean trustSelfSignedCertificate, Boolean disableHostnameVerification)
        throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        // Create an SSL context builder.
        SSLContextBuilder sslContextBuilder = SSLContexts.custom();

        // If specified, setup a trust strategy that allows all certificates.
        if (BooleanUtils.isTrue(trustSelfSignedCertificate))
        {
            sslContextBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
        }

        // If specified, turn hostname verification off.
        HostnameVerifier hostnameVerifier = BooleanUtils.isTrue(disableHostnameVerification) ? SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER :
            SSLConnectionSocketFactory.STRICT_HOSTNAME_VERIFIER;

        // Create client config
        ClientConfig config = new DefaultClientConfig();
        config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(hostnameVerifier, sslContextBuilder.build()));

        return config;
    }
}
