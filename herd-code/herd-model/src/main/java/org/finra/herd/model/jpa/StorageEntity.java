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

import java.util.Collection;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonManagedReference;

/**
 * A storage.
 */
@Table(name = StorageEntity.TABLE_NAME)
@Entity
public class StorageEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "strge";

    /**
     * Our official S3 managed bucket for herd.
     */
    public static final String MANAGED_STORAGE = "S3_MANAGED";

    /**
     * Our official S3 managed "external" bucket for herd.
     */
    public static final String MANAGED_EXTERNAL_STORAGE = "S3_MANAGED_EXTERNAL";

    /**
     * Our official S3 managed "loading dock" bucket for herd.
     */
    public static final String MANAGED_LOADING_DOCK_STORAGE = "S3_MANAGED_LOADING_DOCK";

    /**
     *  Our official S3 sample file
     */
    public static final String SAMPLE_DATA_FILE_STORAGE = "S3_MANAGED_SAMPLE_DATA";

    @Id
    @Column(name = "strge_cd")
    private String name;

    @ManyToOne
    @JoinColumn(name = "strge_pltfm_cd", referencedColumnName = "strge_pltfm_cd", nullable = false)
    private StoragePlatformEntity storagePlatform;

    @JsonManagedReference
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
}
