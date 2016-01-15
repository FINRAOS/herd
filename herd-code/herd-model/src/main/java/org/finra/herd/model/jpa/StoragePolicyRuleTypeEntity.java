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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A storage policy rule type.
 */
@XmlRootElement
@XmlType
@Table(name = "strge_plcy_rule_type_cd_lk")
@Entity
public class StoragePolicyRuleTypeEntity extends AuditableEntity
{
    /**
     * Storage policy rule type that uses business object data "created on" field.
     */
    public static final String DAYS_SINCE_BDATA_REGISTERED = "DAYS_SINCE_BDATA_REGISTERED";

    /**
     * The code column.
     */
    @Id
    @Column(name = "strge_plcy_rule_type_cd")
    private String code;

    @Column(name = "strge_plcy_rule_type_ds")
    private String description;

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }
}
