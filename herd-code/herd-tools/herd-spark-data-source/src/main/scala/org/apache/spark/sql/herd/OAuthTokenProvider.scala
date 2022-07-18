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
package org.apache.spark.sql.herd

import java.util.concurrent.ConcurrentHashMap

import com.jayway.restassured.RestAssured
import org.apache.http.HttpStatus
import org.apache.log4j.Logger
import org.joda.time.DateTime

import org.finra.herd.sdk.invoker.ApiException

object OAuthTokenProvider extends Retry {
  override val log: Logger = Logger.getLogger(getClass.getName)
  var accessTokenCache = new ConcurrentHashMap[String, AccessToken]()

  /**
   * Retrieve OAuth access token
   *
   * @param username       username used for OAuth access token retrieval
   * @param password       username used for OAuth access token retrieval
   * @param accessTokenUrl access token url used for OAuth access token retrieval
   * @return OAuth access token as String
   * @throws ApiException if an Api exception was encountered
   */
  @throws[ApiException]
  def getAccessToken(username: String, password: String, accessTokenUrl: String): String = {
    if (!accessTokenCache.containsKey(username) || accessTokenCache.get(username).getExpiresIn.isBefore(DateTime.now)) {
      refreshOauthToken(username, password, accessTokenUrl)
    }
    accessTokenCache.get(username).getAccessToken
  }

  @throws[ApiException]
  private def refreshOauthToken(username: String, password: String, accessTokenUrl: String): Unit = {
    log.info(String.format("Getting Access Token from accessTokenUrl: %s for user: %s ", username, accessTokenUrl))
    val response = RestAssured.`given`.auth.preemptive.basic(username, password).formParam("grant_type", "client_credentials").when.post(accessTokenUrl)
    if (response.statusCode != HttpStatus.SC_OK) {
      throw new ApiException(s"Failed to retrieve OAuth access token from accessTokenUrl:$accessTokenUrl, errorCode=${response.getStatusCode}, " +
        s"errorMessage=${response.asString()}")
    }
    val accessToken = new AccessToken
    accessToken.accessToken = response.jsonPath.getString("access_token")
    accessToken.scope = response.jsonPath.getString("scope")
    accessToken.tokenType = response.jsonPath.getString("token_type")
    // Set expire time to actual expire time minus 1 minute
    accessToken.expiresIn = DateTime.now.plusSeconds(response.jsonPath.getString("expires_in").toInt - 60)
    accessTokenCache.put(username, accessToken)
  }
}
