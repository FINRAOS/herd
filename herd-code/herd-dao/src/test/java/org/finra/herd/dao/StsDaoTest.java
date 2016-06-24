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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.dao.impl.MockStsOperationsImpl;
import org.finra.herd.model.dto.AwsParamsDto;

/**
 * This class tests the functionality of StsDao.
 */
public class StsDaoTest extends AbstractDaoTest
{
    /**
     * Tests the scenario where the job is run.
     */
    @Test
    public void testGetTemporarySecurityCredentials()
    {
        // Retrieve the temporary security credentials.
        AwsParamsDto testAwsParamsDto = new AwsParamsDto();
        testAwsParamsDto.setHttpProxyHost(HTTP_PROXY_HOST);
        testAwsParamsDto.setHttpProxyPort(HTTP_PROXY_PORT);
        int testAwsRoleDurationSeconds = INTEGER_VALUE;
        Policy testPolicy = new Policy();
        Credentials resultCredentials =
            stsDao.getTemporarySecurityCredentials(testAwsParamsDto, SESSION_NAME, AWS_ROLE_ARN, testAwsRoleDurationSeconds, testPolicy);

        // Validate the results.
        assertNotNull(resultCredentials);
        Assert.assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY, resultCredentials.getAccessKeyId());
        assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SECRET_KEY, resultCredentials.getSecretAccessKey());
        assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN, resultCredentials.getSessionToken());
        // Using >= here just to avoid a race condition.
        assertTrue((System.currentTimeMillis() + 1000 * testAwsRoleDurationSeconds) >= resultCredentials.getExpiration().getTime());

        // Retrieve the temporary security credentials without specifying HTTP proxy settings.
        testAwsParamsDto.setHttpProxyHost(null);
        testAwsParamsDto.setHttpProxyPort(null);
        resultCredentials = stsDao.getTemporarySecurityCredentials(testAwsParamsDto, SESSION_NAME, AWS_ROLE_ARN, testAwsRoleDurationSeconds, testPolicy);

        // Validate the results.
        assertNotNull(resultCredentials);
        Assert.assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_ACCESS_KEY, resultCredentials.getAccessKeyId());
        assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SECRET_KEY, resultCredentials.getSecretAccessKey());
        assertEquals(MockStsOperationsImpl.MOCK_AWS_ASSUMED_ROLE_SESSION_TOKEN, resultCredentials.getSessionToken());
        // Using >= here just to avoid a race condition.
        assertTrue((System.currentTimeMillis() + 1000 * testAwsRoleDurationSeconds) >= resultCredentials.getExpiration().getTime());
    }
}
