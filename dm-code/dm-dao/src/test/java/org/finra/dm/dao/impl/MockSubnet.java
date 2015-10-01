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

import com.amazonaws.services.ec2.model.Subnet;

/**
 * An in-memory subnet information.
 */
public class MockSubnet
{
    /**
     * The subnet ID.
     */
    private String subnetId;

    /**
     * The AZ which this subnet belongs to.
     */
    private String availabilityZone;

    /**
     * The number of IP addresses available.
     */
    private Integer availableIpAddressCount;

    public String getSubnetId()
    {
        return subnetId;
    }

    public void setSubnetId(String subnetId)
    {
        this.subnetId = subnetId;
    }

    public String getAvailabilityZone()
    {
        return availabilityZone;
    }

    public void setAvailabilityZone(String availabilityZone)
    {
        this.availabilityZone = availabilityZone;
    }

    public Integer getAvailableIpAddressCount()
    {
        return availableIpAddressCount;
    }

    public void setAvailableIpAddressCount(Integer availableIpAddressCount)
    {
        this.availableIpAddressCount = availableIpAddressCount;
    }

    /**
     * Converts this object into an AWS equivalent object.
     * 
     * @return A new equivalent AWS object
     */
    public Subnet toAwsObject()
    {
        Subnet subnet = new Subnet();
        subnet.setSubnetId(subnetId);
        subnet.setAvailabilityZone(availabilityZone);
        subnet.setAvailableIpAddressCount(availableIpAddressCount);
        return subnet;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((subnetId == null) ? 0 : subnetId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MockSubnet other = (MockSubnet) obj;
        if (subnetId == null)
        {
            if (other.subnetId != null)
                return false;
        }
        else if (!subnetId.equals(other.subnetId))
            return false;
        return true;
    }
}
