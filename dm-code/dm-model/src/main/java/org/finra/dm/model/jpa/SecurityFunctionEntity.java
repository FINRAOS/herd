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
 * A security function.
 */
@XmlRootElement
@XmlType
@Table(name = "scrty_fn_lk")
@Entity
public class SecurityFunctionEntity extends AuditableEntity
{
    /**
     * The code column.
     */
    @Id
    @Column(name = "scrty_fn_cd")
    private String code;

    @Column(name = "scrty_fn_dsply_nm")
    private String displayName;
    
    @Column(name = "scrty_fn_ds", length = 500)
    private String description;

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }

    public String getDisplayName()
    {
        return displayName;
    }

    public void setDisplayName(String displayName)
    {
        this.displayName = displayName;
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

        SecurityFunctionEntity securityFunction = (SecurityFunctionEntity) o;

        if (code != null ? !code.equals(securityFunction.code) : securityFunction.code != null)
        {
            return false;
        }
        if (displayName != null ? !displayName.equals(securityFunction.displayName) : securityFunction.displayName != null)
        {
            return false;
        }
        if (description != null ? !description.equals(securityFunction.description) : securityFunction.description != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = code != null ? code.hashCode() : 0;
        result = 31 * result + (displayName != null ? displayName.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }
}
