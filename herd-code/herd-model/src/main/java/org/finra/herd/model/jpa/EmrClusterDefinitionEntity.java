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
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * An instance of EMR cluster definition.
 */
@XmlRootElement
@XmlType
@Table(name = EmrClusterDefinitionEntity.TABLE_NAME)
@Entity
public class EmrClusterDefinitionEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "emr_clstr_dfntn";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq")
    private Integer id;

    /**
     * The definition name column.
     */
    @Column(name = "name_tx", nullable = false)
    private String name;

    /**
     * The definition configuration xml column.
     */
    @Column(name = "cnfgn_cl", nullable = false)
    private String configuration;

    @ManyToOne
    @JoinColumn(name = "name_space_cd", referencedColumnName = "name_space_cd", nullable = false)
    private NamespaceEntity namespace;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public NamespaceEntity getNamespace()
    {
        return namespace;
    }

    public void setNamespace(NamespaceEntity namespace)
    {
        this.namespace = namespace;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getConfiguration()
    {
        return configuration;
    }

    public void setConfiguration(String configuration)
    {
        this.configuration = configuration;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || getClass() != other.getClass())
        {
            return false;
        }

        EmrClusterDefinitionEntity that = (EmrClusterDefinitionEntity) other;

        if (id != null ? !id.equals(that.id) : that.id != null)
        {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null)
        {
            return false;
        }
        if (configuration != null ? !configuration.equals(that.configuration) : that.configuration != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (configuration != null ? configuration.hashCode() : 0);
        return result;
    }
}
