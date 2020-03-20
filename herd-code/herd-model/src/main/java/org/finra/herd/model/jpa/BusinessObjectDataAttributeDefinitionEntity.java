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
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.hibernate.annotations.Type;

/**
 * Business Object Data attribute definition associated with business object format.
 */
@Table(name = BusinessObjectDataAttributeDefinitionEntity.TABLE_NAME)
@Entity
public class BusinessObjectDataAttributeDefinitionEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "bus_objct_data_atrbt_dfntn";

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

    @JsonBackReference(value="businessObjectFormat-attributeDefinitions")
    @ManyToOne
    @JoinColumn(name = "bus_objct_frmt_id", referencedColumnName = "bus_objct_frmt_id", nullable = false)
    private BusinessObjectFormatEntity businessObjectFormat;

    @Column(name = "pblsh_fl")
    @Type(type = "yes_no")
    private Boolean publish;

    @Column(name = "pblsh_for_fltr_fl")
    @Type(type = "yes_no")
    private Boolean publishForFilter;

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

    /**
     * For now, the existence of the attribute means that it is required so this method will always return true. In the future, this could be a separate
     * property of this entity so it can be individually configured.
     *
     * @return True if the attribute is required or false if not.
     */
    @JsonIgnore
    public boolean isRequired()
    {
        return true;
    }

    public BusinessObjectFormatEntity getBusinessObjectFormat()
    {
        return businessObjectFormat;
    }

    public void setBusinessObjectFormat(BusinessObjectFormatEntity businessObjectFormat)
    {
        this.businessObjectFormat = businessObjectFormat;
    }

    public Boolean getPublish()
    {
        return publish;
    }

    public void setPublish(Boolean publish)
    {
        this.publish = publish;
    }

    public Boolean getPublishForFilter()
    {
        return publishForFilter;
    }

    public void setPublishForFilter(Boolean publishForFilter)
    {
        this.publishForFilter = publishForFilter;
    }
}
