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

import com.fasterxml.jackson.annotation.JsonBackReference;

/**
 * Attribute associated with business object definition.
 */
@Table(name = BusinessObjectDefinitionAttributeEntity.TABLE_NAME)
@Entity
public class BusinessObjectDefinitionAttributeEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "bus_objct_dfntn_atrbt";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Long id;

    /**
     * The attribute name column.
     */
    @Column(name = "atrbt_nm")
    private String name;

    /**
     * The attribute value column.
     */
    @Column(name = "atrbt_value_tx", length = 4000)
    private String value;

    @JsonBackReference(value="businessObjectDefinition-attributes")
    @ManyToOne
    @JoinColumn(name = "bus_objct_dfntn_id", referencedColumnName = "bus_objct_dfntn_id", nullable = false)
    private BusinessObjectDefinitionEntity businessObjectDefinition;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    public BusinessObjectDefinitionEntity getBusinessObjectDefinition()
    {
        return businessObjectDefinition;
    }

    public void setBusinessObjectDefinition(BusinessObjectDefinitionEntity businessObjectDefinition)
    {
        this.businessObjectDefinition = businessObjectDefinition;
    }
}
