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

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.HostnameVerifier;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.springframework.stereotype.Component;

/**
 * A base helper class that provides HTTP client functions.
 */
@Component
public class HttpClientHelper
{
    /**
     * Creates a new HTTP client.
     *
     * @param trustSelfSignedCertificate specifies whether to trust a self-signed certificate
     * @param disableHostnameVerification specifies whether to turn off hostname verification
     *
     * @return the HTTP client
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    public CloseableHttpClient createHttpClient(Boolean trustSelfSignedCertificate, Boolean disableHostnameVerification)
        throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException
    {
        // Create an HTTP client builder.
        HttpClientBuilder httpClientBuilder = HttpClients.custom();

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

        // Create and assign an SSL connection socket factory.
        SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContextBuilder.build(), hostnameVerifier);
        httpClientBuilder.setSSLSocketFactory(sslConnectionSocketFactory);

        // Build and return an HTTP client.
        return httpClientBuilder.build();
    }
}
