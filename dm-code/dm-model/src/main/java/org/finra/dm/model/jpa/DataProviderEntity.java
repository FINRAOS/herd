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
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A data provider.
 */
@XmlRootElement
@XmlType
@Table(name = DataProviderEntity.TABLE_NAME)
@Entity
@NamedQuery(name = DataProviderEntity.QUERY_GET_DATA_PROVIDER_BY_NAME,
    query = "select dpe from DataProviderEntity dpe where upper(dpe.name) = upper(:" + DataProviderEntity.COLUMN_NAME + ")")
public class DataProviderEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "data_prvdr";

    /**
     * Query that gets an entity by the name.
     */
    public static final String QUERY_GET_DATA_PROVIDER_BY_NAME = "getDataProviderByName";

    /**
     * The name column.
     */
    public static final String COLUMN_NAME = "data_prvdr_cd";

    @Id
    @Column(name = COLUMN_NAME)
    private String name;

    public void setName(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
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

        DataProviderEntity that = (DataProviderEntity) o;

        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode()
    {
        return (name != null ? name.hashCode() : 0);
    }
}
