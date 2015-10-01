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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.hibernate.annotations.Type;

/**
 * A schema column associated with a business object format.
 */
@XmlRootElement
@XmlType
@Table(name = SchemaColumnEntity.TABLE_NAME)
@Entity
public class SchemaColumnEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "schm_clmn";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq")
    private Integer id;

    @ManyToOne
    @JoinColumn(name = "bus_objct_frmt_id", referencedColumnName = "bus_objct_frmt_id", nullable = false)
    private BusinessObjectFormatEntity businessObjectFormat;

    @Column(name = "clmn_name_tx", nullable = false)
    private String name;

    @Column(name = "clmn_type_cd", nullable = false)
    private String type;

    /**
     * The schema column size column is declared a String, since, for "NUMERIC" and "DECIMAL" data types, we should be able to specify both the maximum number
     * of digits (p) that are present in the number and the maximum number of decimal places (s) as a "p,s" string.
     */
    @Column(name = "clmn_size_tx")
    private String size;

    @Column(name = "clmn_rqrd_fl")
    @Type(type = "yes_no")
    private Boolean required;

    @Column(name = "clmn_dflt_tx")
    private String defaultValue;

    /**
     * The position column.
     */
    @Column(name = "clmn_pstn_nb")
    private Integer position;

    /**
     * The partition level column.
     */
    @Column(name = "prtn_level_nb")
    private Integer partitionLevel;

    @Column(name = "clmn_ds")
    private String description;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public BusinessObjectFormatEntity getBusinessObjectFormat()
    {
        return businessObjectFormat;
    }

    public void setBusinessObjectFormat(BusinessObjectFormatEntity businessObjectFormat)
    {
        this.businessObjectFormat = businessObjectFormat;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getSize()
    {
        return size;
    }

    public void setSize(String size)
    {
        this.size = size;
    }

    public Boolean getRequired()
    {
        return required;
    }

    public void setRequired(Boolean required)
    {
        this.required = required;
    }

    public String getDefaultValue()
    {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue)
    {
        this.defaultValue = defaultValue;
    }

    public Integer getPosition()
    {
        return position;
    }

    public void setPosition(Integer position)
    {
        this.position = position;
    }

    public Integer getPartitionLevel()
    {
        return partitionLevel;
    }

    public void setPartitionLevel(Integer partitionLevel)
    {
        this.partitionLevel = partitionLevel;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        SchemaColumnEntity that = (SchemaColumnEntity) o;

        if (defaultValue != null ? !defaultValue.equals(that.defaultValue) : that.defaultValue != null)
        {
            return false;
        }
        if (description != null ? !description.equals(that.description) : that.description != null)
        {
            return false;
        }
        if (!id.equals(that.id))
        {
            return false;
        }
        if (!name.equals(that.name))
        {
            return false;
        }
        if (position != null ? !position.equals(that.position) : that.position != null)
        {
            return false;
        }
        if (partitionLevel != null ? !partitionLevel.equals(that.partitionLevel) : that.partitionLevel != null)
        {
            return false;
        }
        if (required != null ? !required.equals(that.required) : that.required != null)
        {
            return false;
        }
        if (size != null ? !size.equals(that.size) : that.size != null)
        {
            return false;
        }
        if (!type.equals(that.type))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (size != null ? size.hashCode() : 0);
        result = 31 * result + (required != null ? required.hashCode() : 0);
        result = 31 * result + (defaultValue != null ? defaultValue.hashCode() : 0);
        result = 31 * result + (position != null ? position.hashCode() : 0);
        result = 31 * result + (partitionLevel != null ? partitionLevel.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }
}
