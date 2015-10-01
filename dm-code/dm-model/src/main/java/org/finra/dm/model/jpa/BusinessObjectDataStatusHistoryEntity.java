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

/**
 * Status history associated with business object data.
 */
@XmlRootElement
@XmlType
@Table(name = BusinessObjectDataStatusHistoryEntity.TABLE_NAME)
@Entity
public class BusinessObjectDataStatusHistoryEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "bus_objct_data_stts_hs";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq")
    private Integer id;

    @ManyToOne
    @JoinColumn(name = "bus_objct_data_stts_cd", referencedColumnName = "bus_objct_data_stts_cd", nullable = false)
    private BusinessObjectDataStatusEntity status;

    @ManyToOne
    @JoinColumn(name = "bus_objct_data_id", referencedColumnName = "bus_objct_data_id", nullable = false)
    private BusinessObjectDataEntity businessObjectData;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public BusinessObjectDataStatusEntity getStatus()
    {
        return status;
    }

    public void setStatus(BusinessObjectDataStatusEntity status)
    {
        this.status = status;
    }

    public BusinessObjectDataEntity getBusinessObjectData()
    {
        return businessObjectData;
    }

    public void setBusinessObjectData(BusinessObjectDataEntity businessObjectData)
    {
        this.businessObjectData = businessObjectData;
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

        BusinessObjectDataStatusHistoryEntity that = (BusinessObjectDataStatusHistoryEntity) o;

        if (!businessObjectData.equals(that.businessObjectData))
        {
            return false;
        }
        if (!status.equals(that.status))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = status.hashCode();
        result = 31 * result + businessObjectData.hashCode();
        return result;
    }
}
