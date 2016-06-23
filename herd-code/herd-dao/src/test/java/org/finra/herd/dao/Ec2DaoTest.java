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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryResult;
import com.google.common.base.Objects;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.impl.Ec2DaoImpl;
import org.finra.herd.model.dto.AwsParamsDto;

/**
 * This class tests functionality of Ec2Dao.
 */
public class Ec2DaoTest extends AbstractDaoTest
{
    /**
     * Argument matcher which matches a DescribeSpotPriceHistoryRequest
     */
    private static class EqualsDescribeSpotPriceHistoryRequest extends ArgumentMatcher<DescribeSpotPriceHistoryRequest>
    {
        private String availabilityZone;

        private Collection<String> instanceTypes;

        private Collection<String> productDescriptions;

        public EqualsDescribeSpotPriceHistoryRequest(String availabilityZone, Collection<String> instanceTypes, Collection<String> productDescriptions)
        {
            this.availabilityZone = availabilityZone;
            this.instanceTypes = instanceTypes;
            this.productDescriptions = productDescriptions;
        }

        @Override
        public boolean matches(Object argument)
        {
            DescribeSpotPriceHistoryRequest describeSpotPriceHistoryRequest = (DescribeSpotPriceHistoryRequest) argument;
            return Objects.equal(availabilityZone, describeSpotPriceHistoryRequest.getAvailabilityZone())
                && Objects.equal(instanceTypes, describeSpotPriceHistoryRequest.getInstanceTypes())
                && Objects.equal(productDescriptions, describeSpotPriceHistoryRequest.getProductDescriptions());
        }
    }

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

    @Test
    public void testGetLatestSpotPricesAssertConstructsCorrectDescribeSpotPriceHistoryRequest()
    {
        /*
         * Initialize inputs
         */
        String availabilityZone = "a";
        Collection<String> instanceTypes = Arrays.asList("a", "b", "c");
        Collection<String> productDescriptions = Arrays.asList("b", "c", "d");
        AwsParamsDto awsParamsDto = new AwsParamsDto();

        /*
         * Set up mock
         */
        RetryPolicyFactory retryPolicyFactory = mock(RetryPolicyFactory.class);
        Ec2Operations ec2Operations = mock(Ec2Operations.class);

        Ec2DaoImpl ec2Dao = new Ec2DaoImpl();
        ec2Dao.setRetryPolicyFactory(retryPolicyFactory);
        ec2Dao.setEc2Operations(ec2Operations);

        when(retryPolicyFactory.getRetryPolicy()).thenReturn(ClientConfiguration.DEFAULT_RETRY_POLICY);

        DescribeSpotPriceHistoryResult describeSpotPriceHistoryResult = new DescribeSpotPriceHistoryResult();
        when(ec2Operations.describeSpotPriceHistory(any(), any())).thenReturn(describeSpotPriceHistoryResult);

        /*
         * Execute MUT
         */
        ec2Dao.getLatestSpotPrices(availabilityZone, instanceTypes, productDescriptions, awsParamsDto);

        /*
         * Verify that the dependency was called with the correct parameters
         */
        verify(ec2Operations).describeSpotPriceHistory(any(), equalsDescribeSpotPriceHistoryRequest(availabilityZone, instanceTypes, productDescriptions));
    }

    /**
     * Returns a matcher proxy which matches a DescribeSpotPriceHistoryRequest with the given parameters.
     * 
     * @param availabilityZone Availability zone
     * @param instanceTypes Instance types
     * @param productDescriptions Product descriptions
     * @return DescribeSpotPriceHistoryRequest matcher proxy
     */
    private DescribeSpotPriceHistoryRequest equalsDescribeSpotPriceHistoryRequest(String availabilityZone, Collection<String> instanceTypes,
        Collection<String> productDescriptions)
    {
        return argThat(new EqualsDescribeSpotPriceHistoryRequest(availabilityZone, instanceTypes, productDescriptions));
    }
}
