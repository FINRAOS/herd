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
package org.finra.herd.model.jpa;

import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

@Table(name = Ec2OnDemandPricingEntity.TABLE_NAME + "_lk")
@Entity
public class Ec2OnDemandPricingEntity extends AuditableEntity
{
    public static final String TABLE_NAME = "ec2_od_prcng";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    @Column(name = "rgn_nm", nullable = false)
    private String regionName;

    @Column(name = "instc_type", nullable = false)
    private String instanceType;

    @Column(name = "hrly_pr", nullable = false)
    private BigDecimal hourlyPrice;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public String getRegionName()
    {
        return regionName;
    }

    public void setRegionName(String regionName)
    {
        this.regionName = regionName;
    }

    public String getInstanceType()
    {
        return instanceType;
    }

    public void setInstanceType(String instanceType)
    {
        this.instanceType = instanceType;
    }

    public BigDecimal getHourlyPrice()
    {
        return hourlyPrice;
    }

    public void setHourlyPrice(BigDecimal hourlyPrice)
    {
        this.hourlyPrice = hourlyPrice;
    }
}
