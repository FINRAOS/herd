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
package org.finra.herd.tools.common.oauth2;

import io.restassured.RestAssured;
import org.apache.http.HttpStatus;

public class AccessToken
{
    public static String getAccessToken(AuthKey authKey)
    {
        return RestAssured.given().auth().preemptive().basic(authKey.getUsername(), authKey.getPassword()).formParam("grant_type", "client_credentials").when()
            .post(authKey.getAuthUrl()).then().log().all().statusCode(HttpStatus.SC_OK).extract().response().jsonPath().getString("access_token");
    }
}
