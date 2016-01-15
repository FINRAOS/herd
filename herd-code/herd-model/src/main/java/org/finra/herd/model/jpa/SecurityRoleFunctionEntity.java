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
 * A security role function mapping.
 */
@XmlRootElement
@XmlType
@Table(name = SecurityRoleFunctionEntity.TABLE_NAME)
@Entity
public class SecurityRoleFunctionEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "scrty_role_fn";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq")
    private Integer id;

    @ManyToOne
    @JoinColumn(name = "scrty_role_cd", referencedColumnName = "scrty_role_cd", nullable = false)
    private SecurityRoleEntity securityRole;

    @ManyToOne
    @JoinColumn(name = "scrty_fn_cd", referencedColumnName = "scrty_fn_cd", nullable = false)
    private SecurityFunctionEntity securityFunction;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public SecurityRoleEntity getSecurityRole()
    {
        return securityRole;
    }

    public void setSecurityRole(SecurityRoleEntity securityRole)
    {
        this.securityRole = securityRole;
    }

    public SecurityFunctionEntity getSecurityFunction()
    {
        return securityFunction;
    }

    public void setSecurityFunction(SecurityFunctionEntity securityFunction)
    {
        this.securityFunction = securityFunction;
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

        SecurityRoleFunctionEntity that = (SecurityRoleFunctionEntity) other;

        if (id != null ? !id.equals(that.id) : that.id != null)
        {
            return false;
        }
        if (securityRole != null ? !securityRole.equals(that.securityRole) : that.securityRole != null)
        {
            return false;
        }
        if (securityFunction != null ? !securityFunction.equals(that.securityFunction) : that.securityFunction != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (id != null ? id.hashCode() : 0);
    }
}
