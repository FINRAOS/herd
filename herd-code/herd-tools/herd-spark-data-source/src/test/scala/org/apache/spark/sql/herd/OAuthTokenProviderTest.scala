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

import java.net.ConnectException
import java.util.concurrent.ConcurrentHashMap

import org.joda.time.DateTime
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class OAuthTokenProviderTest extends FunSuite with MockitoSugar with BeforeAndAfterEach {

  var oAuthTokenProvider = OAuthTokenProvider
  var accessTokenCache = new ConcurrentHashMap[String, AccessToken]()


  // set up
  override def beforeEach(): Unit = {
    oAuthTokenProvider.accessTokenCache = accessTokenCache
  }

  test("test access token") {
    val accessToken = new AccessToken
    accessToken.accessToken = "token"
    accessToken.expiresIn = DateTime.now.plusSeconds(5)
    accessTokenCache.put("username", accessToken)
    assertEquals(accessToken.getAccessToken, oAuthTokenProvider.getAccessToken("username", "", ""))
    assertEquals(accessToken.getAccessToken, oAuthTokenProvider.getAccessToken("username", "", ""))
  }

  test("test expired access token") {
    val accessToken = new AccessToken
    accessToken.accessToken = "token"
    accessToken.expiresIn = DateTime.now.plusSeconds(-5)
    accessTokenCache.put("username", accessToken)
    intercept[ConnectException] {
      oAuthTokenProvider.getAccessToken("username", "", "")
    }
  }

  test("test empty access token") {
    intercept[ConnectException] {
      oAuthTokenProvider.getAccessToken("username", "", "")
    }
  }
}
