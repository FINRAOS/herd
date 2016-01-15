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

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesRequest;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceAttributeResult;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryResult;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.ModifyInstanceAttributeRequest;

import org.finra.herd.dao.Ec2Operations;

public class Ec2OperationsImpl implements Ec2Operations
{
    /**
     * Describe the EC2 instance attribute
     */
    @Override
    public DescribeInstanceAttributeResult describeInstanceAttribute(AmazonEC2Client ec2Client,
        DescribeInstanceAttributeRequest describeInstanceAttributeRequest)
    {
        return ec2Client.describeInstanceAttribute(describeInstanceAttributeRequest);
    }

    /**
     * Modify EC2 instance attributes
     */
    @Override
    public void modifyInstanceAttribute(AmazonEC2Client ec2Client, ModifyInstanceAttributeRequest modifyInstanceAttributeRequest)
    {
        ec2Client.modifyInstanceAttribute(modifyInstanceAttributeRequest);
    }

    @Override
    public DescribeSubnetsResult describeSubnets(AmazonEC2Client ec2Client, DescribeSubnetsRequest describeSubnetsRequest)
    {
        return ec2Client.describeSubnets(describeSubnetsRequest);
    }

    @Override
    public DescribeAvailabilityZonesResult describeAvailabilityZones(AmazonEC2Client ec2Client,
        DescribeAvailabilityZonesRequest describeAvailabilityZonesRequest)
    {
        return ec2Client.describeAvailabilityZones(describeAvailabilityZonesRequest);
    }

    @Override
    public DescribeSpotPriceHistoryResult describeSpotPriceHistory(AmazonEC2Client ec2Client, DescribeSpotPriceHistoryRequest describeSpotPriceHistoryRequest)
    {
        return ec2Client.describeSpotPriceHistory(describeSpotPriceHistoryRequest);
    }
}
