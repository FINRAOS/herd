package org.finra.herd.tools.common.oauth2;

import com.jayway.restassured.RestAssured;
import org.apache.http.HttpStatus;

public class AccessToken
{
    public static String getAccessToken(AuthKey authKey)
    {
        return RestAssured.given().auth().preemptive().basic(authKey.getUsername(), authKey.getPassword()).formParam("grant_type", "client_credentials").when()
            .post(authKey.getAuthUrl()).then().log().all().statusCode(HttpStatus.SC_OK).extract().response().jsonPath().getString("access_token");
    }
}
