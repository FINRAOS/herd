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
package org.finra.dm.model.jpa;

import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "EC2_OD_PRCNG_LK")
@Entity
public class OnDemandPriceEntity extends AuditableEntity
{
    @Id
    @Column(name = "EC2_OD_PRCNG_ID")
    private Long onDemandPriceId;

    @Column(name = "RGN_NM", nullable = false)
    private String region;

    @Column(name = "INSTC_TYPE", nullable = false)
    private String instanceType;

    @Column(name = "HRLY_PR", nullable = false)
    private BigDecimal value;

    public Long getOnDemandPriceId()
    {
        return onDemandPriceId;
    }

    public void setOnDemandPriceId(Long onDemandPriceId)
    {
        this.onDemandPriceId = onDemandPriceId;
    }

    public String getRegion()
    {
        return region;
    }

    public void setRegion(String region)
    {
        this.region = region;
    }

    public String getInstanceType()
    {
        return instanceType;
    }

    public void setInstanceType(String instanceType)
    {
        this.instanceType = instanceType;
    }

    public BigDecimal getValue()
    {
        return value;
    }

    public void setValue(BigDecimal value)
    {
        this.value = value;
    }
}
