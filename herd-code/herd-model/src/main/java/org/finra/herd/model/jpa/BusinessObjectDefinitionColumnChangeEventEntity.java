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
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/**
 * Change Events associated with business object definition column.
 */
@Table(name = BusinessObjectDefinitionColumnChangeEventEntity.TABLE_NAME)
@Entity
public class BusinessObjectDefinitionColumnChangeEventEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "bus_objct_dfntn_clmn_chg";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    @ManyToOne
    @JoinColumn(name = "bus_objct_dfntn_clmn_id", referencedColumnName = "bus_objct_dfntn_clmn_id", nullable = false)
    private BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumn;

    @Column(name = "clmn_ds", length = 4000)
    private String description;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public BusinessObjectDefinitionColumnEntity getBusinessObjectDefinitionColumnEntity()
    {
        return businessObjectDefinitionColumn;
    }

    public void setBusinessObjectDefinitionColumnEntity(BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumn)
    {
        this.businessObjectDefinitionColumn = businessObjectDefinitionColumn;
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
