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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryResult;
import com.amazonaws.services.ec2.model.SpotPrice;
import com.google.common.base.Objects;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;

import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.impl.Ec2DaoImpl;
import org.finra.herd.dao.impl.MockEc2OperationsImpl;
import org.finra.herd.dao.impl.MockSpotPrice;
import org.finra.herd.model.dto.AwsParamsDto;

/**
 * This class tests functionality of Ec2Dao.
 */
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
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

    @Test
    public void testGetLatestSpotPricesAssertConstructsCorrectDescribeSpotPriceHistoryRequest()
    {
        // Initialize inputs.
        String availabilityZone = "a";
        Collection<String> instanceTypes = Arrays.asList("a", "b", "c");
        Collection<String> productDescriptions = Arrays.asList("b", "c", "d");

        // Set up mock.
        mock(RetryPolicyFactory.class);
        Ec2Operations ec2Operations = mock(Ec2Operations.class);
        ((Ec2DaoImpl) ec2Dao).setEc2Operations(ec2Operations);
        DescribeSpotPriceHistoryResult describeSpotPriceHistoryResult = new DescribeSpotPriceHistoryResult();
        when(ec2Operations.describeSpotPriceHistory(any(), any())).thenReturn(describeSpotPriceHistoryResult);

        // Execute MUT.
        ec2Dao.getLatestSpotPrices(availabilityZone, instanceTypes, productDescriptions,
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, NO_HTTP_PROXY_HOST, NO_HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1));

        // Verify that the dependency was called with the correct parameters.
        verify(ec2Operations).describeSpotPriceHistory(any(), equalsDescribeSpotPriceHistoryRequest(availabilityZone, instanceTypes, productDescriptions));
    }

    @Test
    public void testGetLatestSpotPricesSpotPriceHistoryContainDuplicateTypeInstances()
    {
        // Initialize inputs.
        Collection<String> instanceTypes = Arrays.asList(MockEc2OperationsImpl.INSTANCE_TYPE_1, MockEc2OperationsImpl.INSTANCE_TYPE_2);
        Collection<String> productDescriptions = Arrays.asList("b", "c");

        // Set up mock.
        mock(RetryPolicyFactory.class);
        Ec2Operations ec2Operations = mock(Ec2Operations.class);
        ((Ec2DaoImpl) ec2Dao).setEc2Operations(ec2Operations);
        DescribeSpotPriceHistoryResult describeSpotPriceHistoryResult = new DescribeSpotPriceHistoryResult();
        List<SpotPrice> spotPrices = new ArrayList<>();
        spotPrices.add(
            new MockSpotPrice(MockEc2OperationsImpl.INSTANCE_TYPE_1, MockEc2OperationsImpl.AVAILABILITY_ZONE_1, MockEc2OperationsImpl.SPOT_PRICE_HIGH)
                .toAwsObject());
        spotPrices.add(
            new MockSpotPrice(MockEc2OperationsImpl.INSTANCE_TYPE_1, MockEc2OperationsImpl.AVAILABILITY_ZONE_2, MockEc2OperationsImpl.SPOT_PRICE_HIGH)
                .toAwsObject());
        describeSpotPriceHistoryResult.setSpotPriceHistory(spotPrices);
        when(ec2Operations.describeSpotPriceHistory(any(), any())).thenReturn(describeSpotPriceHistoryResult);

        // Execute MUT.
        List<SpotPrice> result = ec2Dao.getLatestSpotPrices(MockEc2OperationsImpl.AVAILABILITY_ZONE_1, instanceTypes, productDescriptions,
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, NO_HTTP_PROXY_HOST, NO_HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1));

        // Verify that the dependency was called with the correct parameters.
        verify(ec2Operations).describeSpotPriceHistory(any(),
            equalsDescribeSpotPriceHistoryRequest(MockEc2OperationsImpl.AVAILABILITY_ZONE_1, instanceTypes, productDescriptions));

        // Verify that result contains only one spot price entry.
        assertEquals(1, CollectionUtils.size(result));
    }

    /**
     * Returns a matcher proxy which matches a DescribeSpotPriceHistoryRequest with the given parameters.
     *
     * @param availabilityZone Availability zone
     * @param instanceTypes Instance types
     * @param productDescriptions Product descriptions
     *
     * @return DescribeSpotPriceHistoryRequest matcher proxy
     */
    private DescribeSpotPriceHistoryRequest equalsDescribeSpotPriceHistoryRequest(String availabilityZone, Collection<String> instanceTypes,
        Collection<String> productDescriptions)
    {
        return argThat(describeSpotPriceHistoryRequest -> Objects.equal(availabilityZone, describeSpotPriceHistoryRequest.getAvailabilityZone()) &&
            Objects.equal(instanceTypes, describeSpotPriceHistoryRequest.getInstanceTypes()) &&
            Objects.equal(productDescriptions, describeSpotPriceHistoryRequest.getProductDescriptions()));
    }
}
