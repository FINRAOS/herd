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
import javax.persistence.Id;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A namespace.
 */
@XmlRootElement
@XmlType
@Table(name = NamespaceEntity.TABLE_NAME)
@Entity
public class NamespaceEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "name_space";

    @Id
    @Column(name = "name_space_cd")
    private String code;

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
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

        NamespaceEntity that = (NamespaceEntity) other;

        return !(code != null ? !code.equals(that.code) : that.code != null);
    }

    @Override
    public int hashCode()
    {
        return (code != null ? code.hashCode() : 0);
    }
}
