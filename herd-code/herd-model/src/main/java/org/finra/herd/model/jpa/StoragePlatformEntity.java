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
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A storage platform.
 */
@XmlRootElement
@XmlType
@Table(name = StoragePlatformEntity.TABLE_NAME)
@Entity
@NamedQueries(
    {@NamedQuery(name = StoragePlatformEntity.QUERY_GET_STORAGE_PLATFORM_BY_NAME, query = StoragePlatformEntity.GET_STORAGE_PLATFORM_BY_NAME_QUERY_STRING),
        @NamedQuery(name = StoragePlatformEntity.QUERY_GET_S3_STORAGE_PLATFORM,
            query = "select spe from StoragePlatformEntity spe where spe.name = '" + StoragePlatformEntity.S3 + "'")})
public class StoragePlatformEntity extends AuditableEntity
{
    /**
     * The query string for getting a storage platform by name.
     */
    public static final String GET_STORAGE_PLATFORM_BY_NAME_QUERY_STRING =
        "select spe from StoragePlatformEntity spe where upper(spe.name) = upper(:" + StoragePlatformEntity.COLUMN_NAME + ")";

    /**
     * The table name.
     */
    public static final String TABLE_NAME = "strge_pltfm";

    /**
     * Query that gets an entity by the name.
     */
    public static final String QUERY_GET_STORAGE_PLATFORM_BY_NAME = "getStoragePlatformByName";

    /**
     * Query that gets the S3 storage entity.
     */
    public static final String QUERY_GET_S3_STORAGE_PLATFORM = "getS3StoragePlatform";

    /**
     * The S3 storage platform.
     */
    public static final String S3 = "S3";

    /**
     * The Glacier storage platform.
     */
    public static final String GLACIER = "GLACIER";

    /**
     * The name column.
     */
    public static final String COLUMN_NAME = "strge_pltfm_cd";

    @Id
    @Column(name = COLUMN_NAME)
    private String name;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
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

        StoragePlatformEntity that = (StoragePlatformEntity) other;

        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode()
    {
        return (name != null ? name.hashCode() : 0);
    }
}
