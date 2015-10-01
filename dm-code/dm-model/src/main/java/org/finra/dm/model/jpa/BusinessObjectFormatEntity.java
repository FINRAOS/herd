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
 * A business object format.
 */
@XmlRootElement
@XmlType
@Table(name = BusinessObjectFormatEntity.TABLE_NAME)
@Entity
public class BusinessObjectFormatEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "bus_objct_frmt";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq")
    private Integer id;

    @ManyToOne
    @JoinColumn(name = "bus_objct_dfntn_id", referencedColumnName = "bus_objct_dfntn_id", nullable = false)
    private BusinessObjectDefinitionEntity businessObjectDefinition;

    /**
     * The usage column.
     */
    @Column(name = "usage_cd")
    private String usage;

    @ManyToOne
    @JoinColumn(name = "file_type_cd", referencedColumnName = "file_type_cd", nullable = false)
    private FileTypeEntity fileType;

    /**
     * The format version column.
     */
    @Column(name = "frmt_vrsn_nb", nullable = false)
    private Integer businessObjectFormatVersion;

    @Column(name = "ltst_vrsn_fl")
    @Type(type = "yes_no")
    private Boolean latestVersion;

    @Column(name = "desc_tx")
    private String description;

    @Column(name = "prtn_key_tx")
    private String partitionKey;

    @Column(name = "null_value_tx")
    private String nullValue;

    @Column(name = "dlmtr_tx")
    private String delimiter;

    @Column(name = "escp_char_tx")
    private String escapeCharacter;

    @ManyToOne
    @JoinColumn(name = "prtn_key_group_tx", referencedColumnName = "prtn_key_group_tx")
    private PartitionKeyGroupEntity partitionKeyGroup;

    @OneToMany(mappedBy = "businessObjectFormat", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("name")
    private Collection<BusinessObjectDataAttributeDefinitionEntity> attributeDefinitions;

    @OneToMany(mappedBy = "businessObjectFormat", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("position")
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

    public String getUsage()
    {
        return usage;
    }

    public void setUsage(String usage)
    {
        this.usage = usage;
    }

    public FileTypeEntity getFileType()
    {
        return fileType;
    }

    public void setFileType(FileTypeEntity fileType)
    {
        this.fileType = fileType;
    }

    public Integer getBusinessObjectFormatVersion()
    {
        return businessObjectFormatVersion;
    }

    public void setBusinessObjectFormatVersion(Integer businessObjectFormatVersion)
    {
        this.businessObjectFormatVersion = businessObjectFormatVersion;
    }

    public Boolean getLatestVersion()
    {
        return latestVersion;
    }

    public void setLatestVersion(Boolean latestVersion)
    {
        this.latestVersion = latestVersion;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public String getPartitionKey()
    {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey)
    {
        this.partitionKey = partitionKey;
    }

    public String getNullValue()
    {
        return nullValue;
    }

    public void setNullValue(String nullValue)
    {
        this.nullValue = nullValue;
    }

    public String getDelimiter()
    {
        return delimiter;
    }

    public void setDelimiter(String delimiter)
    {
        this.delimiter = delimiter;
    }

    public String getEscapeCharacter()
    {
        return escapeCharacter;
    }

    public void setEscapeCharacter(String escapeCharacter)
    {
        this.escapeCharacter = escapeCharacter;
    }

    public PartitionKeyGroupEntity getPartitionKeyGroup()
    {
        return partitionKeyGroup;
    }

    public void setPartitionKeyGroup(PartitionKeyGroupEntity partitionKeyGroup)
    {
        this.partitionKeyGroup = partitionKeyGroup;
    }

    public Collection<BusinessObjectDataAttributeDefinitionEntity> getAttributeDefinitions()
    {
        return attributeDefinitions;
    }

    public void setAttributeDefinitions(Collection<BusinessObjectDataAttributeDefinitionEntity> attributeDefinitions)
    {
        this.attributeDefinitions = attributeDefinitions;
    }

    public Collection<SchemaColumnEntity> getSchemaColumns()
    {
        return schemaColumns;
    }

    public void setSchemaColumns(Collection<SchemaColumnEntity> schemaColumns)
    {
        this.schemaColumns = schemaColumns;
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

        BusinessObjectFormatEntity that = (BusinessObjectFormatEntity) o;

        if (!businessObjectFormatVersion.equals(that.businessObjectFormatVersion))
        {
            return false;
        }
        if (delimiter != null ? !delimiter.equals(that.delimiter) : that.delimiter != null)
        {
            return false;
        }
        if (description != null ? !description.equals(that.description) : that.description != null)
        {
            return false;
        }
        if (escapeCharacter != null ? !escapeCharacter.equals(that.escapeCharacter) : that.escapeCharacter != null)
        {
            return false;
        }
        if (!id.equals(that.id))
        {
            return false;
        }
        if (!latestVersion.equals(that.latestVersion))
        {
            return false;
        }
        if (nullValue != null ? !nullValue.equals(that.nullValue) : that.nullValue != null)
        {
            return false;
        }
        if (!partitionKey.equals(that.partitionKey))
        {
            return false;
        }
        if (partitionKeyGroup != null ? !partitionKeyGroup.equals(that.partitionKeyGroup) : that.partitionKeyGroup != null)
        {
            return false;
        }
        if (!usage.equals(that.usage))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = id.hashCode();
        result = 31 * result + usage.hashCode();
        result = 31 * result + businessObjectFormatVersion.hashCode();
        result = 31 * result + latestVersion.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + partitionKey.hashCode();
        result = 31 * result + (nullValue != null ? nullValue.hashCode() : 0);
        result = 31 * result + (delimiter != null ? delimiter.hashCode() : 0);
        result = 31 * result + (escapeCharacter != null ? escapeCharacter.hashCode() : 0);
        result = 31 * result + (partitionKeyGroup != null ? partitionKeyGroup.hashCode() : 0);
        return result;
    }
}
