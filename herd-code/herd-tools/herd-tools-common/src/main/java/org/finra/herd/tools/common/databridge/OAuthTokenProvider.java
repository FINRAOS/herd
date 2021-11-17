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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import org.apache.http.HttpStatus;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.tools.common.dto.AccessToken;

/**
 * Class used to refresh and provide OAuth token
 */
public class OAuthTokenProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuthTokenProvider.class);

    private ConcurrentMap<String, AccessToken> accessTokenCache = new ConcurrentHashMap();

    /**
     * Retrieve OAuth access token
     *
     * @param username       username used for OAuth access token retrieval
     * @param password       username used for OAuth access token retrieval
     * @param accessTokenUrl access token url used for OAuth access token retrieval
     * @return OAuth access token as String
     * @throws ApiException if an Api exception was encountered
     */
    public String getAccessToken(String username, String password, String accessTokenUrl) throws ApiException
    {
        LOGGER.info(String.format("Getting Access Token from accessTokenUrl: %s for user: %s ", username, accessTokenUrl));
        if (!accessTokenCache.containsKey(username) || accessTokenCache.get(username).getExpiresIn().isBefore(DateTime.now()))
        {
            refreshOauthToken(username, password, accessTokenUrl);
        }
        return accessTokenCache.get(username).getAccessToken();
    }

    @Retryable(value = ApiException.class, backoff = @Backoff(delay = 2000, multiplier = 2))
    private void refreshOauthToken(String username, String password, String accessTokenUrl) throws ApiException
    {
        Response response =
            RestAssured.given().auth().preemptive().basic(username, password).formParam("grant_type", "client_credentials").when().post(accessTokenUrl);
        if (response.statusCode() != HttpStatus.SC_OK)
        {
            throw new ApiException(String.format("Failed to retrieve OAuth access token from accessTokenUrl:%s, errorCode=%d, errorMessage=%s", accessTokenUrl,
                response.getStatusCode(), response.asString()));
        }
        AccessToken accessToken = new AccessToken();
        accessToken.setAccessToken(response.jsonPath().getString("access_token"));
        accessToken.setScope(response.jsonPath().getString("scope"));
        accessToken.setTokenType(response.jsonPath().getString("token_type"));
        // Set expire time to actual expire time minus 1 minute
        accessToken.setExpiresIn(DateTime.now().plusSeconds(Integer.parseInt(response.jsonPath().getString("expires_in")) - 60));
        accessTokenCache.put(username, accessToken);
    }
}
