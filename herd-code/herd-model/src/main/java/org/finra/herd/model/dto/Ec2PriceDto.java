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
package org.finra.herd.model.dto;

import java.math.BigDecimal;

/**
 * The specification of what prices and instance to use for a EMR cluster instance.
 */
public class Ec2PriceDto
{
    /**
     * The price in USD per hour to run one instance.
     */
    private BigDecimal instancePrice;

    /**
     * The number of instance to run.
     */
    private Integer instanceCount;

    /**
     * True if the pricing is spot, false if on-demand.
     */
    private Boolean isSpot;

    /**
     * The amount to bid to run a spot instance. Only applicable when {@link #isSpot} is true.
     */
    private BigDecimal bidPrice;

    public BigDecimal getInstancePrice()
    {
        return instancePrice;
    }

    public void setInstancePrice(BigDecimal instancePrice)
    {
        this.instancePrice = instancePrice;
    }

    public Integer getInstanceCount()
    {
        return instanceCount;
    }

    public void setInstanceCount(Integer instanceCount)
    {
        this.instanceCount = instanceCount;
    }

    public Boolean getIsSpot()
    {
        return isSpot;
    }

    public void setIsSpot(Boolean isSpot)
    {
        this.isSpot = isSpot;
    }

    public BigDecimal getBidPrice()
    {
        return bidPrice;
    }

    public void setBidPrice(BigDecimal bidPrice)
    {
        this.bidPrice = bidPrice;
    }

    @Override
    public String toString()
    {
        return "[instancePrice=" + instancePrice + ", isSpot=" + isSpot + ", bidPrice=" + bidPrice + "]";
    }
}
