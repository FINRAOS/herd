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

import java.util.Collection;
import java.util.List;

import org.finra.herd.model.dto.AwsParamsDto;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.SpotPrice;
import com.amazonaws.services.ec2.model.Subnet;

/**
 * A DAO for Amazon AWS EC2.
 */
public interface Ec2Dao
{
    public List<String> addSecurityGroupsToEc2Instance(String ec2InstanceId, List<String> securityGroups, AwsParamsDto awsParams);

    public AmazonEC2Client getEc2Client(AwsParamsDto awsParamsDto);

    /**
     * Returns the latest spot prices for each of the instance types specified in the given AZ. Any instance type not found will be ignored, and will not be
     * included in the list of spot prices.
     * 
     * @param availabilityZone The AZ which the spot prices belong in.
     * @param instanceTypes The instance types of the spot prices.
     * @param awsParamsDto AWS connection parameters.
     * @return List of latest spot prices.
     */
    public List<SpotPrice> getLatestSpotPrices(String availabilityZone, Collection<String> instanceTypes, AwsParamsDto awsParamsDto);

    /**
     * Returns a list of availability zones that contains the given collections of subnets.
     * 
     * @param subnets The subnets which belong to the returned AZs
     * @param awsParamsDto AWS connection parameters.
     * @return List of AZs
     */
    public List<AvailabilityZone> getAvailabilityZonesForSubnetIds(Collection<Subnet> subnets, AwsParamsDto awsParamsDto);

    /**
     * Returns a list of subnets by their subnet IDs.
     * 
     * @param subnetIds List of subnet IDs.
     * @param awsParamsDto AWS connection parameters.
     * @return List of subnets
     */
    public List<Subnet> getSubnets(Collection<String> subnetIds, AwsParamsDto awsParamsDto);
}
