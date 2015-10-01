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
package org.finra.dm.model.dto;

/**
 * The pricing information on how to run an EMR cluster.
 */
public class EmrClusterPriceDto
{
    /**
     * The AZ to run the cluster in.
     */
    private String availabilityZone;

    /**
     * The master pricing information.
     */
    private Ec2PriceDto masterPrice;

    /**
     * The core pricing information.
     */
    private Ec2PriceDto corePrice;

    /**
     * The task pricing information. This is optional.
     */
    private Ec2PriceDto taskPrice;

    public String getAvailabilityZone()
    {
        return availabilityZone;
    }

    public void setAvailabilityZone(String availabilityZone)
    {
        this.availabilityZone = availabilityZone;
    }

    public Ec2PriceDto getMasterPrice()
    {
        return masterPrice;
    }

    public void setMasterPrice(Ec2PriceDto masterPrice)
    {
        this.masterPrice = masterPrice;
    }

    public Ec2PriceDto getCorePrice()
    {
        return corePrice;
    }

    public void setCorePrice(Ec2PriceDto corePrice)
    {
        this.corePrice = corePrice;
    }

    public Ec2PriceDto getTaskPrice()
    {
        return taskPrice;
    }

    public void setTaskPrice(Ec2PriceDto taskPrice)
    {
        this.taskPrice = taskPrice;
    }

    @Override
    public String toString()
    {
        return "EmrClusterPriceDto [availabilityZone=" + availabilityZone + ", masterPrice=" + masterPrice + ", corePrice=" + corePrice + ", taskPrice="
            + taskPrice + "]";
    }
}
