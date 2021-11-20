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

import static org.junit.Assert.assertEquals;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.junit.Test;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.sdk.invoker.ApiClient;

public class ApiClientHelperTest extends AbstractCoreTest
{
    @Test
    public void validateRebuildClient() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        ApiClient apiClient = new ApiClient();
        Client client = new Client();
        apiClient.setHttpClient(client);

        ApiClientHelper apiClientHelper = new ApiClientHelper();
        ApiClient rebuiltApiClient = apiClientHelper.rebuildClient(apiClient, true, false);
        HTTPSProperties httpsProperties = (HTTPSProperties) rebuiltApiClient.getHttpClient().getProperties().get(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES);
        assertEquals(DefaultHostnameVerifier.class, httpsProperties.getHostnameVerifier().getClass());

        rebuiltApiClient = apiClientHelper.rebuildClient(apiClient, true, true);
        httpsProperties = (HTTPSProperties) rebuiltApiClient.getHttpClient().getProperties().get(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES);
        assertEquals(NoopHostnameVerifier.INSTANCE, httpsProperties.getHostnameVerifier());
    }
}
