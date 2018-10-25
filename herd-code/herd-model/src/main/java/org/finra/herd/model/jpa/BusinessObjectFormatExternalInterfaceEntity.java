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
 * A business object format to external interface mapping.
 */
@Table(name = BusinessObjectFormatExternalInterfaceEntity.TABLE_NAME)
@Entity
public class BusinessObjectFormatExternalInterfaceEntity extends AuditableEntity
{
    public static final String TABLE_NAME = "bus_objct_frmt_xtrnl_intrfc";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    @ManyToOne
    @JoinColumn(name = "bus_objct_frmt_id", referencedColumnName = "bus_objct_frmt_id", nullable = false)
    private BusinessObjectFormatEntity businessObjectFormat;

    @Column(name = "bus_objct_frmt_id", insertable = false, updatable = false)
    private Integer businessObjectFormatId;

    @ManyToOne
    @JoinColumn(name = "xtrnl_intrfc_cd", referencedColumnName = "xtrnl_intrfc_cd", nullable = false)
    private ExternalInterfaceEntity externalInterface;

    @Column(name = "xtrnl_intrfc_cd", insertable = false, updatable = false)
    private String externalInterfaceName;

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

    public Integer getBusinessObjectFormatId()
    {
        return businessObjectFormatId;
    }

    public void setBusinessObjectFormatId(Integer businessObjectFormatId)
    {
        this.businessObjectFormatId = businessObjectFormatId;
    }

    public ExternalInterfaceEntity getExternalInterface()
    {
        return externalInterface;
    }

    public void setExternalInterface(ExternalInterfaceEntity externalInterface)
    {
        this.externalInterface = externalInterface;
    }

    public String getExternalInterfaceName()
    {
        return externalInterfaceName;
    }

    public void setExternalInterfaceName(String externalInterfaceName)
    {
        this.externalInterfaceName = externalInterfaceName;
    }
}
