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
package org.finra.dm.dao;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.dao.helper.AwsHelper;
import org.finra.dm.model.dto.AwsParamsDto;

/**
 * This class tests functionality of Ec2Dao.
 */
public class Ec2DaoTest extends AbstractDaoTest
{
    @Autowired
    private AwsHelper awsHelper;

    @Test
    public void testAddSecurityGroupsToEc2Instance()
    {
        // Add the security groups to an EC2 instance.
        List<String> testSecurityGroups = Arrays.asList(EC2_SECURITY_GROUP_1, EC2_SECURITY_GROUP_2);
        List<String> resultSecurityGroups = ec2Dao.addSecurityGroupsToEc2Instance(EC2_INSTANCE_ID, testSecurityGroups, awsHelper.getAwsParamsDto());

        // Validate the results.
        assertNotNull(resultSecurityGroups);
        assertTrue(resultSecurityGroups.containsAll(testSecurityGroups));
    }

    @Test
    public void testGetEc2Client() throws Exception
    {
        // Get the EMR client with proxy configuration.
        AwsParamsDto awsParamsDto = awsHelper.getAwsParamsDto();
        awsParamsDto.setHttpProxyHost(HTTP_PROXY_HOST);
        awsParamsDto.setHttpProxyPort(HTTP_PROXY_PORT);
        assertNotNull(ec2Dao.getEc2Client(awsParamsDto));

        // Set the proxy host as blank to get the EMR client without proxy.
        awsParamsDto.setHttpProxyHost(BLANK_TEXT);
        awsParamsDto.setHttpProxyPort(HTTP_PROXY_PORT);
        assertNotNull(ec2Dao.getEc2Client(awsParamsDto));

        // Set the proxy port as null to get the EMR client without proxy.
        awsParamsDto.setHttpProxyHost(HTTP_PROXY_HOST);
        awsParamsDto.setHttpProxyPort(null);
        assertNotNull(ec2Dao.getEc2Client(awsParamsDto));

        // Set the proxy host as blank and proxy port as null to get the EMR client without proxy.
        awsParamsDto.setHttpProxyHost(BLANK_TEXT);
        awsParamsDto.setHttpProxyPort(null);
        assertNotNull(ec2Dao.getEc2Client(awsParamsDto));
    }
}
