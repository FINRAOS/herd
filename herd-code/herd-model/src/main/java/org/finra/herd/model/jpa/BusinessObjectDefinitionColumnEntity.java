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

import java.util.Collection;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonBackReference;

/**
 * Column associated with business object definition.
 */
@Table(name = BusinessObjectDefinitionColumnEntity.TABLE_NAME)
@Entity
public class BusinessObjectDefinitionColumnEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "bus_objct_dfntn_clmn";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    @JsonBackReference(value="businessObjectDefinition-columns")
    @ManyToOne
    @JoinColumn(name = "bus_objct_dfntn_id", referencedColumnName = "bus_objct_dfntn_id", nullable = false)
    private BusinessObjectDefinitionEntity businessObjectDefinition;

    /**
     * The business object definition column name column.
     */
    @Column(name = "clmn_name_tx", nullable = false)
    private String name;

    /**
     * The business object definition column description column.
     */
    @Column(name = "clmn_ds")
    private String description;

    @JsonBackReference
    @OneToMany(mappedBy = "businessObjectDefinitionColumn")
    private Collection<SchemaColumnEntity> schemaColumns;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public BusinessObjectDefinitionEntity getBusinessObjectDefinition()
    {
        return businessObjectDefinition;
    }

    public void setBusinessObjectDefinition(BusinessObjectDefinitionEntity businessObjectDefinition)
    {
        this.businessObjectDefinition = businessObjectDefinition;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public Collection<SchemaColumnEntity> getSchemaColumns()
    {
        return schemaColumns;
    }

    public void setSchemaColumns(Collection<SchemaColumnEntity> schemaColumns)
    {
        this.schemaColumns = schemaColumns;
    }
}
