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
package org.finra.herd.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.finra.herd.dao.Ec2Operations;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesRequest;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceAttributeResult;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryResult;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.GroupIdentifier;
import com.amazonaws.services.ec2.model.InstanceAttribute;
import com.amazonaws.services.ec2.model.ModifyInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.SpotPrice;
import com.amazonaws.services.ec2.model.Subnet;

/**
 * Mock implementation of AWS EC2 operations.
 * 
 * The mock contains several pre-configured subnets, availability zones, regions, and instance types.
 */
public class MockEc2OperationsImpl implements Ec2Operations
{
    public static final int AVAILABLE_IP_HIGH = 20;
    public static final int AVAILABLE_IP_LOW = 10;

    public static final String SPOT_PRICE_VERY_HIGH = "2";
    public static final String SPOT_PRICE_HIGH = "1.5";
    public static final String SPOT_PRICE_EQUAL = "1";
    public static final String SPOT_PRICE_LOW = ".5";
    public static final String SPOT_PRICE_VERY_LOW = ".25";

    public static String SUBNET_1 = "SUBNET_1";
    public static String SUBNET_2 = "SUBNET_2";
    public static String SUBNET_3 = "SUBNET_3";
    public static String SUBNET_4 = "SUBNET_4";
    public static String SUBNET_5 = "SUBNET_5";

    public static String AVAILABILITY_ZONE_1 = "AVAILABILITY_ZONE_1";
    public static String AVAILABILITY_ZONE_2 = "AVAILABILITY_ZONE_2";
    public static String AVAILABILITY_ZONE_3 = "AVAILABILITY_ZONE_3";
    public static String AVAILABILITY_ZONE_4 = "AVAILABILITY_ZONE_4";

    public static String REGION_1 = "REGION_1";
    public static String REGION_2 = "REGION_2";

    public static String INSTANCE_TYPE_1 = "INSTANCE_TYPE_1";
    public static String INSTANCE_TYPE_2 = "INSTANCE_TYPE_2";
    public static String INSTANCE_TYPE_3 = "INSTANCE_TYPE_3";

    /*
     * Pre-configured in-memory EC2 instance information.
     * These are initialized in the constructor.
     * Note that on-demand prices are database configurations, therefore are stored in the mock reference data DB script.
     */
    private Map<String, MockSubnet> mockSubnets = new HashMap<>();
    private Map<String, MockAvailabilityZone> mockAvailabilityZones = new HashMap<>();
    private Set<String> mockInstanceTypes = new HashSet<>();

    public MockEc2OperationsImpl()
    {
        Map<String, MockEc2Region> mockEc2Regions = new HashMap<>();

        // Pre-configure REGION_1
        {
            MockEc2Region mockEc2Region = new MockEc2Region();
            mockEc2Region.setName(REGION_1);
            mockEc2Regions.put(REGION_1, mockEc2Region);

            // Pre-configure AVAILABILITY_ZONE_1
            {
                MockAvailabilityZone availabilityZone = new MockAvailabilityZone();
                availabilityZone.setZoneName(AVAILABILITY_ZONE_1);
                mockEc2Region.putAvailabilityZone(availabilityZone);

                // Pre-configured instance types for AVAILABILITY_ZONE_1
                {
                    MockSpotPrice spotPrice = new MockSpotPrice();
                    spotPrice.setType(INSTANCE_TYPE_1);
                    spotPrice.setSpotPrice(SPOT_PRICE_LOW);
                    availabilityZone.putSpotPrice(spotPrice);
                }

                {
                    MockSpotPrice spotPrice = new MockSpotPrice();
                    spotPrice.setType(INSTANCE_TYPE_2);
                    spotPrice.setSpotPrice(SPOT_PRICE_EQUAL);
                    availabilityZone.putSpotPrice(spotPrice);
                }

                {
                    MockSpotPrice spotPrice = new MockSpotPrice();
                    spotPrice.setType(INSTANCE_TYPE_3);
                    spotPrice.setSpotPrice(SPOT_PRICE_HIGH);
                    availabilityZone.putSpotPrice(spotPrice);
                }

                // Pre-configured subnets for AVAILABILITY_ZONE_1
                {
                    MockSubnet subnet = new MockSubnet();
                    subnet.setSubnetId(SUBNET_1);
                    subnet.setAvailableIpAddressCount(AVAILABLE_IP_LOW);
                    availabilityZone.putSubnet(subnet);
                }

                {
                    MockSubnet subnet = new MockSubnet();
                    subnet.setSubnetId(SUBNET_2);
                    subnet.setAvailableIpAddressCount(AVAILABLE_IP_HIGH);
                    availabilityZone.putSubnet(subnet);
                }
            }

            // Pre-configure AVAILABILITY_ZONE_2
            {
                MockAvailabilityZone availabilityZone = new MockAvailabilityZone();
                availabilityZone.setZoneName(AVAILABILITY_ZONE_2);
                mockEc2Region.putAvailabilityZone(availabilityZone);

                // Pre-configured instance types for AVAILABILITY_ZONE_2
                {
                    MockSpotPrice spotPrice = new MockSpotPrice();
                    spotPrice.setType(INSTANCE_TYPE_1);
                    spotPrice.setSpotPrice(SPOT_PRICE_EQUAL);
                    availabilityZone.putSpotPrice(spotPrice);
                }

                // Pre-configured subnets for AVAILABILITY_ZONE_2
                {
                    MockSubnet subnet = new MockSubnet();
                    subnet.setSubnetId(SUBNET_3);
                    subnet.setAvailableIpAddressCount(AVAILABLE_IP_HIGH);
                    availabilityZone.putSubnet(subnet);
                }
            }

            // Pre-configure AVAILABILITY_ZONE_3
            {
                MockAvailabilityZone availabilityZone = new MockAvailabilityZone();
                availabilityZone.setZoneName(AVAILABILITY_ZONE_3);
                mockEc2Region.putAvailabilityZone(availabilityZone);

                // Pre-configured instance types for AVAILABILITY_ZONE_3
                {
                    MockSpotPrice spotPrice = new MockSpotPrice();
                    spotPrice.setType(INSTANCE_TYPE_1);
                    spotPrice.setSpotPrice(SPOT_PRICE_VERY_LOW);
                    availabilityZone.putSpotPrice(spotPrice);
                }

                {
                    MockSpotPrice spotPrice = new MockSpotPrice();
                    spotPrice.setType(INSTANCE_TYPE_2);
                    spotPrice.setSpotPrice(SPOT_PRICE_VERY_HIGH);
                    availabilityZone.putSpotPrice(spotPrice);
                }

                // Pre-configured subnets for AVAILABILITY_ZONE_3
                {
                    MockSubnet subnet = new MockSubnet();
                    subnet.setSubnetId(SUBNET_4);
                    subnet.setAvailableIpAddressCount(AVAILABLE_IP_LOW);
                    availabilityZone.putSubnet(subnet);
                }
            }
        }

        // Pre-configure REGION_2
        {
            MockEc2Region ec2Region = new MockEc2Region();
            ec2Region.setName(REGION_2);
            mockEc2Regions.put(REGION_2, ec2Region);

            // Pre-configure availability zones for REGION_2
            {
                MockAvailabilityZone availabilityZone = new MockAvailabilityZone();
                availabilityZone.setZoneName(AVAILABILITY_ZONE_4);
                ec2Region.putAvailabilityZone(availabilityZone);

                // Pre-configure spot prices for AVAILABILITY_ZONE_4
                {
                    MockSpotPrice spotPrice = new MockSpotPrice();
                    spotPrice.setType(INSTANCE_TYPE_1);
                    spotPrice.setSpotPrice(SPOT_PRICE_LOW);
                    availabilityZone.putSpotPrice(spotPrice);
                }

                {
                    MockSpotPrice spotPrice = new MockSpotPrice();
                    spotPrice.setType(INSTANCE_TYPE_2);
                    spotPrice.setSpotPrice(SPOT_PRICE_LOW);
                    availabilityZone.putSpotPrice(spotPrice);
                }

                // Pre-configure subnets for AVAILABILITY_ZONE_4
                {
                    MockSubnet subnet = new MockSubnet();
                    subnet.setSubnetId(SUBNET_5);
                    subnet.setAvailableIpAddressCount(AVAILABLE_IP_LOW);
                    availabilityZone.putSubnet(subnet);
                }
            }
        }

        // Populate subnets from the above configurations
        for (MockEc2Region ec2Region : mockEc2Regions.values())
        {
            for (MockAvailabilityZone availabilityZone : ec2Region.getAvailabilityZones().values())
            {
                for (MockSubnet subnet : availabilityZone.getSubnets().values())
                {
                    mockSubnets.put(subnet.getSubnetId(), subnet);
                }
            }
        }

        // Populate availability zones from the above configurations
        for (MockEc2Region ec2Region : mockEc2Regions.values())
        {
            for (MockAvailabilityZone availabilityZone : ec2Region.getAvailabilityZones().values())
            {
                mockAvailabilityZones.put(availabilityZone.getZoneName(), availabilityZone);
            }
        }

        // Populate availability zones from the above configurations
        for (MockEc2Region ec2Region : mockEc2Regions.values())
        {
            for (MockAvailabilityZone availabilityZone : ec2Region.getAvailabilityZones().values())
            {
                for (MockSpotPrice mockSpotPrice : availabilityZone.getSpotPrices().values())
                {
                    mockInstanceTypes.add(mockSpotPrice.getInstanceType());
                }
            }
        }
    }

    @Override
    public DescribeInstanceAttributeResult describeInstanceAttribute(AmazonEC2Client ec2Client,
        DescribeInstanceAttributeRequest describeInstanceAttributeRequest)
    {
        InstanceAttribute instanceAttribute = new InstanceAttribute();
        instanceAttribute.withGroups(new GroupIdentifier().withGroupId("A_TEST_SECURITY_GROUP"));
        return new DescribeInstanceAttributeResult().withInstanceAttribute(instanceAttribute);
    }

    @Override
    public void modifyInstanceAttribute(AmazonEC2Client ec2Client, ModifyInstanceAttributeRequest modifyInstanceAttributeRequest)
    {
        if (modifyInstanceAttributeRequest.getGroups() != null
            && modifyInstanceAttributeRequest.getGroups().get(0).equals(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION))
        {
            throw new AmazonServiceException(MockAwsOperationsHelper.AMAZON_SERVICE_EXCEPTION);
        }

        if (modifyInstanceAttributeRequest.getGroups() != null
            && modifyInstanceAttributeRequest.getGroups().get(0).equals(MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION))
        {
            AmazonServiceException throttlingException = new AmazonServiceException("test throttling exception");
            throttlingException.setErrorCode("ThrottlingException");

            throw throttlingException;
        }

    }

    /**
     * In-memory implementation of describeSubnets. Returns the subnets from the pre-configured subnets.
     * The method can be ordered to throw an AmazonServiceException with a specified error code by specifying a subnet ID with prefix "throw." followed by a
     * string indicating the error code to throw. The exception thrown in this manner will always set the status code to 500.
     */
    @Override
    public DescribeSubnetsResult describeSubnets(AmazonEC2Client ec2Client, DescribeSubnetsRequest describeSubnetsRequest)
    {
        List<Subnet> subnets = new ArrayList<>();

        List<String> requestedSubnetIds = describeSubnetsRequest.getSubnetIds();

        // add all subnets if request is empty (this is AWS behavior)
        if (requestedSubnetIds.isEmpty())
        {
            requestedSubnetIds.addAll(mockSubnets.keySet());
        }

        for (String requestedSubnetId : requestedSubnetIds)
        {
            MockSubnet mockSubnet = mockSubnets.get(requestedSubnetId);

            // Throw exception if any of the subnet ID do not exist
            if (mockSubnet == null)
            {
                AmazonServiceException amazonServiceException;

                if (requestedSubnetId.startsWith("throw."))
                {
                    String errorCode = requestedSubnetId.substring("throw.".length());

                    amazonServiceException = new AmazonServiceException(errorCode);
                    amazonServiceException.setErrorCode(errorCode);
                    amazonServiceException.setStatusCode(500);
                }
                else
                {
                    amazonServiceException = new AmazonServiceException("The subnet ID '" + requestedSubnetId + "' does not exist");
                    amazonServiceException.setErrorCode(Ec2DaoImpl.ERROR_CODE_SUBNET_ID_NOT_FOUND);
                    amazonServiceException.setStatusCode(400);
                }

                throw amazonServiceException;
            }

            subnets.add(mockSubnet.toAwsObject());
        }

        DescribeSubnetsResult describeSubnetsResult = new DescribeSubnetsResult();
        describeSubnetsResult.setSubnets(subnets);
        return describeSubnetsResult;
    }

    @Override
    public DescribeAvailabilityZonesResult describeAvailabilityZones(AmazonEC2Client ec2Client,
        DescribeAvailabilityZonesRequest describeAvailabilityZonesRequest)
    {
        List<AvailabilityZone> availabilityZones = new ArrayList<>();

        List<String> requestedZoneNames = describeAvailabilityZonesRequest.getZoneNames();

        // add all AZ if request is empty (this is AWS behavior)
        if (requestedZoneNames.isEmpty())
        {
            requestedZoneNames.addAll(mockAvailabilityZones.keySet());
        }

        for (String requestedZoneName : requestedZoneNames)
        {
            // ignore AZ name which does not exist (this is AWS behavior)
            MockAvailabilityZone mockAvailabilityZone = mockAvailabilityZones.get(requestedZoneName);
            if (mockAvailabilityZone != null)
            {
                availabilityZones.add(mockAvailabilityZone.toAwsObject());
            }
        }

        DescribeAvailabilityZonesResult describeAvailabilityZonesResult = new DescribeAvailabilityZonesResult();
        describeAvailabilityZonesResult.setAvailabilityZones(availabilityZones);

        return describeAvailabilityZonesResult;
    }

    @Override
    public DescribeSpotPriceHistoryResult describeSpotPriceHistory(AmazonEC2Client ec2Client, DescribeSpotPriceHistoryRequest describeSpotPriceHistoryRequest)
    {
        List<SpotPrice> spotPriceHistories = new ArrayList<>();

        String requestedAvailabilityZone = describeSpotPriceHistoryRequest.getAvailabilityZone();

        // Get availability zones to search for
        Set<MockAvailabilityZone> requestedAvailabilityZones = new HashSet<>();

        // If requested zone is specified, get and add
        if (requestedAvailabilityZone != null)
        {
            requestedAvailabilityZones.add(mockAvailabilityZones.get(requestedAvailabilityZone));
        }
        // If requested zone is not specified, add all
        else
        {
            requestedAvailabilityZones.addAll(mockAvailabilityZones.values());
        }

        // Get instance types to search for
        List<String> requestedInstanceTypes = describeSpotPriceHistoryRequest.getInstanceTypes();
        // If not specified, add all instance types
        if (requestedInstanceTypes.isEmpty())
        {
            requestedInstanceTypes.addAll(mockInstanceTypes);
        }

        // search for price for all AZ and instance types requested
        for (MockAvailabilityZone mockAvailabilityZone : requestedAvailabilityZones)
        {
            for (String requestedInstanceType : requestedInstanceTypes)
            {
                MockSpotPrice mockSpotPrice = mockAvailabilityZone.getSpotPrices().get(requestedInstanceType);
                if (mockSpotPrice != null)
                {
                    spotPriceHistories.add(mockSpotPrice.toAwsObject());
                }
            }
        }

        DescribeSpotPriceHistoryResult describeSpotPriceHistoryResult = new DescribeSpotPriceHistoryResult();
        describeSpotPriceHistoryResult.setSpotPriceHistory(spotPriceHistories);
        return describeSpotPriceHistoryResult;
    }
}
