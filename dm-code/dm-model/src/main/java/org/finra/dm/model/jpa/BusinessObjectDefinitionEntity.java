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

import java.util.Collection;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.hibernate.annotations.Type;

/**
 * A business object definition.
 */
@XmlRootElement
@XmlType
@Table(name = BusinessObjectDefinitionEntity.TABLE_NAME)
@Entity
public class BusinessObjectDefinitionEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "bus_objct_dfntn";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq")
    private Integer id;

    /**
     * The name column.
     */
    @Column(name = "name_tx")
    private String name;

    @Column(name = "desc_tx", length = 500)
    private String description;

    @ManyToOne
    @JoinColumn(name = "data_prvdr_cd", referencedColumnName = "data_prvdr_cd", nullable = false)
    private DataProviderEntity dataProvider;

    @ManyToOne
    @JoinColumn(name = "name_space_cd", referencedColumnName = "name_space_cd", nullable = false)
    private NamespaceEntity namespace;

    @OneToMany(mappedBy = "businessObjectDefinition")
    private Collection<BusinessObjectFormatEntity> businessObjectFormats;

    @OneToMany(mappedBy = "businessObjectDefinition", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("name")
    private Collection<BusinessObjectDefinitionAttributeEntity> attributes;

    @Column(name = "lgcy_fl")
    @Type(type = "yes_no")
    private Boolean legacy;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
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

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public DataProviderEntity getDataProvider()
    {
        return dataProvider;
    }

    public void setDataProvider(DataProviderEntity dataProvider)
    {
        this.dataProvider = dataProvider;
    }

    public NamespaceEntity getNamespace()
    {
        return namespace;
    }

    public void setNamespace(NamespaceEntity namespace)
    {
        this.namespace = namespace;
    }

    public Collection<BusinessObjectFormatEntity> getBusinessObjectFormats()
    {
        return businessObjectFormats;
    }

    public void setBusinessObjectFormats(Collection<BusinessObjectFormatEntity> businessObjectFormats)
    {
        this.businessObjectFormats = businessObjectFormats;
    }

    public Collection<BusinessObjectDefinitionAttributeEntity> getAttributes()
    {
        return attributes;
    }

    public void setAttributes(Collection<BusinessObjectDefinitionAttributeEntity> attributes)
    {
        this.attributes = attributes;
    }

    public Boolean getLegacy()
    {
        return legacy;
    }

    public void setLegacy(Boolean legacy)
    {
        this.legacy = legacy;
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

        BusinessObjectDefinitionEntity that = (BusinessObjectDefinitionEntity) o;

        if (dataProvider != null ? !dataProvider.equals(that.dataProvider) : that.dataProvider != null)
        {
            return false;
        }
        if (description != null ? !description.equals(that.description) : that.description != null)
        {
            return false;
        }
        if (id != null ? !id.equals(that.id) : that.id != null)
        {
            return false;
        }
        if (legacy != null ? !legacy.equals(that.legacy) : that.legacy != null)
        {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null)
        {
            return false;
        }
        if (namespace != null ? !namespace.equals(that.namespace) : that.namespace != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (namespace != null ? namespace.hashCode() : 0);
        return result;
    }
}
