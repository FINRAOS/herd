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

import com.amazonaws.services.ec2.AmazonEC2;
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

/**
 * AWS EC2 Operations Service.
 */
public interface Ec2Operations
{
    /**
     * {@link AmazonEC2#describeInstanceAttribute(DescribeInstanceAttributeRequest)}
     * 
     * @param ec2Client {@link AmazonEC2} to use.
     * @param describeInstanceAttributeRequest The request object.
     * @return {@link DescribeInstanceAttributeResult}
     */
    public DescribeInstanceAttributeResult describeInstanceAttribute(AmazonEC2Client ec2Client,
        DescribeInstanceAttributeRequest describeInstanceAttributeRequest);

    /**
     * {@link AmazonEC2#modifyInstanceAttribute(ModifyInstanceAttributeRequest)}
     * 
     * @param ec2Client {@link AmazonEC2} to use.
     * @param modifyInstanceAttributeRequest The request object.
     */
    public void modifyInstanceAttribute(AmazonEC2Client ec2Client, ModifyInstanceAttributeRequest modifyInstanceAttributeRequest);

    /**
     * {@link AmazonEC2#describeSubnets()}
     * 
     * @param ec2Client {@link AmazonEC2} to use.
     * @param describeSubnetsRequest The request object.
     * @return {@link DescribeSubnetsResult}
     */
    public DescribeSubnetsResult describeSubnets(AmazonEC2Client ec2Client, DescribeSubnetsRequest describeSubnetsRequest);

    /**
     * {@link AmazonEC2#describeAvailabilityZones()}
     * 
     * @param ec2Client {@link AmazonEC2} to use.
     * @param describeAvailabilityZonesRequest The request object.
     * @return {@link DescribeAvailabilityZonesResult}
     */
    public DescribeAvailabilityZonesResult describeAvailabilityZones(AmazonEC2Client ec2Client,
        DescribeAvailabilityZonesRequest describeAvailabilityZonesRequest);

    /**
     * {@link AmazonEC2#describeSpotPriceHistory()}
     * 
     * @param ec2Client {@link AmazonEC2} to use.
     * @param describeSpotPriceHistoryRequest The request object.
     * @return {@link DescribeSpotPriceHistoryResult}
     */
    public DescribeSpotPriceHistoryResult describeSpotPriceHistory(AmazonEC2Client ec2Client, DescribeSpotPriceHistoryRequest describeSpotPriceHistoryRequest);
}
