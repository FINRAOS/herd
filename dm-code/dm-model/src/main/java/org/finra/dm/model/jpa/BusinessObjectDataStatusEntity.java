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
import javax.persistence.Id;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A business object data status.
 */
@XmlRootElement
@XmlType
@Table(name = "bus_objct_data_stts_cd_lk")
@Entity
public class BusinessObjectDataStatusEntity extends AuditableEntity
{
    // List of common statuses
    public static final String VALID = "VALID";
    public static final String UPLOADING = "UPLOADING";
    public static final String RE_ENCRYPTING = "RE-ENCRYPTING";
    public static final String DELETED = "DELETED";
    public static final String INVALID = "INVALID";
    public static final String EXPIRED = "EXPIRED";
    public static final String ARCHIVED = "ARCHIVED";

    @Id
    @Column(name = "bus_objct_data_stts_cd")
    private String code;

    @Column(name = "bus_objct_data_stts_ds")
    private String description;

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
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

        BusinessObjectDataStatusEntity that = (BusinessObjectDataStatusEntity) o;

        if (!code.equals(that.code))
        {
            return false;
        }
        if (!description.equals(that.description))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return code.hashCode();
    }
}
