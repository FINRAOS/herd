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
package org.finra.dm.dao.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
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
import com.amazonaws.services.ec2.model.InstanceAttributeName;
import com.amazonaws.services.ec2.model.ModifyInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.SpotPrice;
import com.amazonaws.services.ec2.model.Subnet;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.dm.dao.Ec2Dao;
import org.finra.dm.dao.Ec2Operations;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.dto.AwsParamsDto;

/**
 * The EC2 DAO implementation.
 */
@Repository
public class Ec2DaoImpl implements Ec2Dao
{
    /**
     * http://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html
     */
    public static final String ERROR_CODE_SUBNET_ID_NOT_FOUND = "InvalidSubnetID.NotFound";

    @Autowired
    private Ec2Operations ec2Operations;

    /**
     * Adds the security groups to an EC2 instance.
     *
     * @param ec2InstanceId, the ec2 instance id.
     * @param securityGroups, security groups to be added.
     * @param awsParams, awsParamsDto object
     *
     * @return updated security groups.
     */
    @Override
    public List<String> addSecurityGroupsToEc2Instance(String ec2InstanceId, List<String> securityGroups, AwsParamsDto awsParams)
    {
        Set<String> updatedSecurityGroups = new HashSet<>();
        for (String securityGroup : securityGroups)
        {
            updatedSecurityGroups.add(securityGroup);
        }

        // Get existing security groups
        DescribeInstanceAttributeRequest describeInstanceAttributeRequest =
            new DescribeInstanceAttributeRequest().withInstanceId(ec2InstanceId).withAttribute(InstanceAttributeName.GroupSet);

        DescribeInstanceAttributeResult describeInstanceAttributeResult =
            ec2Operations.describeInstanceAttribute(getEc2Client(awsParams), describeInstanceAttributeRequest);

        List<GroupIdentifier> groups = describeInstanceAttributeResult.getInstanceAttribute().getGroups();
        for (GroupIdentifier groupIdentifier : groups)
        {
            updatedSecurityGroups.add(groupIdentifier.getGroupId());
        }

        // Add security group on master EC2 instance
        ModifyInstanceAttributeRequest modifyInstanceAttributeRequest =
            new ModifyInstanceAttributeRequest().withInstanceId(ec2InstanceId).withGroups(updatedSecurityGroups);

        ec2Operations.modifyInstanceAttribute(getEc2Client(awsParams), modifyInstanceAttributeRequest);

        return new ArrayList<>(updatedSecurityGroups);
    }

    /**
     * Create the EC2 client with the given proxy and access key details This is the main AmazonEC2Client object
     *
     * @param awsParamsDto AWS related parameters for access/secret keys and proxy details
     *
     * @return the AmazonEC2Client object
     */
    @Override
    public AmazonEC2Client getEc2Client(AwsParamsDto awsParamsDto)
    {
        // TODO Building EC2 client every time requested, if this becomes a performance issue,
        // might need to consider storing a singleton or building the client once per request.

        AmazonEC2Client ec2Client = null;
        // Create an EC2 client with HTTP proxy information.
        if (StringUtils.isNotBlank(awsParamsDto.getHttpProxyHost()) && awsParamsDto.getHttpProxyPort() != null)
        {
            ec2Client =
                new AmazonEC2Client(new ClientConfiguration().withProxyHost(awsParamsDto.getHttpProxyHost()).withProxyPort(awsParamsDto.getHttpProxyPort()));
        }
        // Create an EC2 client with no proxy information
        else
        {
            ec2Client = new AmazonEC2Client();
        }

        // Return the client.
        return ec2Client;
    }

    /**
     * This implementation uses DescribeSpotPriceHistory API which returns the latest spot price history for the specified AZ and instance types. This method
     * then filters the returned list to only contain the latest spot price for each instance type.
     */
    @Override
    public List<SpotPrice> getLatestSpotPrices(String availabilityZone, Collection<String> instanceTypes, AwsParamsDto awsParamsDto)
    {
        AmazonEC2Client ec2Client = getEc2Client(awsParamsDto);
        DescribeSpotPriceHistoryRequest describeSpotPriceHistoryRequest = new DescribeSpotPriceHistoryRequest();
        describeSpotPriceHistoryRequest.setAvailabilityZone(availabilityZone);
        describeSpotPriceHistoryRequest.setInstanceTypes(instanceTypes);
        DescribeSpotPriceHistoryResult describeSpotPriceHistoryResult = ec2Operations.describeSpotPriceHistory(ec2Client, describeSpotPriceHistoryRequest);
        List<SpotPrice> spotPrices = new ArrayList<>();
        Set<String> instanceTypesFound = new HashSet<>();
        for (SpotPrice spotPriceHistoryEntry : describeSpotPriceHistoryResult.getSpotPriceHistory())
        {
            if (instanceTypesFound.add(spotPriceHistoryEntry.getInstanceType()))
            {
                spotPrices.add(spotPriceHistoryEntry);
            }
        }
        return spotPrices;
    }

    /**
     * This implementation uses the DescribeAvailabilityZones API to get the list of AZs.
     */
    @Override
    public List<AvailabilityZone> getAvailabilityZonesForSubnetIds(Collection<Subnet> subnets, AwsParamsDto awsParamsDto)
    {
        Set<String> zoneNames = new HashSet<>();
        for (Subnet subnet : subnets)
        {
            zoneNames.add(subnet.getAvailabilityZone());
        }

        AmazonEC2Client ec2Client = getEc2Client(awsParamsDto);
        DescribeAvailabilityZonesRequest describeAvailabilityZonesRequest = new DescribeAvailabilityZonesRequest();
        describeAvailabilityZonesRequest.setZoneNames(zoneNames);
        DescribeAvailabilityZonesResult describeAvailabilityZonesResult = ec2Operations.describeAvailabilityZones(ec2Client, describeAvailabilityZonesRequest);
        return describeAvailabilityZonesResult.getAvailabilityZones();
    }

    /**
     * This implementation uses the DescribeSubnets API.
     */
    @Override
    public List<Subnet> getSubnets(Collection<String> subnetIds, AwsParamsDto awsParamsDto)
    {
        AmazonEC2Client ec2Client = getEc2Client(awsParamsDto);
        DescribeSubnetsRequest describeSubnetsRequest = new DescribeSubnetsRequest();
        describeSubnetsRequest.setSubnetIds(subnetIds);
        try
        {
            DescribeSubnetsResult describeSubnetsResult = ec2Operations.describeSubnets(ec2Client, describeSubnetsRequest);
            return describeSubnetsResult.getSubnets();
        }
        catch (AmazonServiceException amazonServiceException)
        {
            /*
             * AWS throws a 400 error when any one of the specified subnet ID is not found.
             * We want to catch it and throw as an handled DM error as a 404 not found.
             */
            if (ERROR_CODE_SUBNET_ID_NOT_FOUND.equals(amazonServiceException.getErrorCode()))
            {
                throw new ObjectNotFoundException(amazonServiceException.getErrorMessage(), amazonServiceException);
            }
            // Any other type of error we throw as is because they are unexpected.
            else
            {
                throw amazonServiceException;
            }
        }
    }
}
