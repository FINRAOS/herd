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

import java.net.ConnectException;
import java.util.concurrent.ConcurrentMap;

import org.joda.time.DateTime;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.tools.common.dto.AccessToken;

public class OAuthTokenProviderTest extends AbstractCoreTest
{
    @Test
    public void testAccessToken() throws ApiException
    {
        OAuthTokenProvider oAuthTokenProvider = new OAuthTokenProvider();
        ConcurrentMap<String, AccessToken> accessTokenCache =
            (ConcurrentMap<String, AccessToken>) ReflectionTestUtils.getField(oAuthTokenProvider, "accessTokenCache");

        AccessToken accessToken = new AccessToken();
        accessToken.setAccessToken("token");
        accessToken.setExpiresIn(DateTime.now().plusSeconds(5));
        accessTokenCache.put("username", accessToken);

        String token = oAuthTokenProvider.getAccessToken("username", "", "");
        oAuthTokenProvider.getAccessToken("username", "", "");
        assertEquals(accessToken.getAccessToken(), token);
    }

    @Test(expected = ConnectException.class)
    public void testAccessTokenExpiredToken() throws ApiException
    {
        OAuthTokenProvider oAuthTokenProvider = new OAuthTokenProvider();
        ConcurrentMap<String, AccessToken> accessTokenCache =
            (ConcurrentMap<String, AccessToken>) ReflectionTestUtils.getField(oAuthTokenProvider, "accessTokenCache");

        AccessToken accessToken = new AccessToken();
        accessToken.setAccessToken("token");
        accessToken.setExpiresIn(DateTime.now().plusSeconds(-5));
        accessTokenCache.put("username", accessToken);

        oAuthTokenProvider.getAccessToken("username", "", "");
    }

    @Test(expected = ConnectException.class)
    public void testAccessTokenEmptyToken() throws ApiException
    {
        OAuthTokenProvider oAuthTokenProvider = new OAuthTokenProvider();

        oAuthTokenProvider.getAccessToken("username", "", "");
    }
}
