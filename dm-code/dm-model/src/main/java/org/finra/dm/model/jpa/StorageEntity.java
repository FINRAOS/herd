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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A storage.
 */
@XmlRootElement
@XmlType
@Table(name = StorageEntity.TABLE_NAME)
@Entity
public class StorageEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "strge";

    /**
     * Our official S3 managed bucket for data management.
     */
    public static final String MANAGED_STORAGE = "S3_MANAGED";

    /**
     * Our official S3 managed "external" bucket for data management.
     */
    public static final String MANAGED_EXTERNAL_STORAGE = "S3_MANAGED_EXTERNAL";

    /**
     * Our official S3 managed "loading dock" bucket for data management.
     */
    public static final String MANAGED_LOADING_DOCK_STORAGE = "S3_MANAGED_LOADING_DOCK";

    /**
     * List all DM "managed" style S3 buckets.
     */
    public static final List<String> S3_MANAGED_STORAGES =
        Collections.unmodifiableList(Arrays.asList(MANAGED_STORAGE, MANAGED_LOADING_DOCK_STORAGE, MANAGED_EXTERNAL_STORAGE));

    @Id
    @Column(name = "strge_cd")
    private String name;

    @ManyToOne
    @JoinColumn(name = "strge_pltfm_cd", referencedColumnName = "strge_pltfm_cd", nullable = false)
    private StoragePlatformEntity storagePlatform;

    @OneToMany(mappedBy = "storage", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("name")
    private Collection<StorageAttributeEntity> attributes;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public StoragePlatformEntity getStoragePlatform()
    {
        return storagePlatform;
    }

    public void setStoragePlatform(StoragePlatformEntity storagePlatform)
    {
        this.storagePlatform = storagePlatform;
    }

    public Collection<StorageAttributeEntity> getAttributes()
    {
        return attributes;
    }

    public void setAttributes(Collection<StorageAttributeEntity> attributes)
    {
        this.attributes = attributes;
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

        StorageEntity storage = (StorageEntity) o;

        return !(name != null ? !name.equals(storage.name) : storage.name != null);
    }

    @Override
    public int hashCode()
    {
        return (name != null ? name.hashCode() : 0);
    }
}
