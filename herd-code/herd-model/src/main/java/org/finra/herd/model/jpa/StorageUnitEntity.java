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

import java.sql.Timestamp;
import java.util.Collection;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/**
 * A storage unit.
 */
@Table(name = StorageUnitEntity.TABLE_NAME)
@Entity
public class StorageUnitEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "strge_unit";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    @OneToMany(mappedBy = "storageUnit", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("path")
    private Collection<StorageFileEntity> storageFiles;

    /**
     * The storage column.
     */
    @ManyToOne
    @JoinColumn(name = "strge_cd", referencedColumnName = "strge_cd", nullable = false)
    private StorageEntity storage;

    @Column(name = "strge_cd", insertable = false, updatable = false)
    private String storageName;

    @ManyToOne
    @JoinColumn(name = "bus_objct_data_id", referencedColumnName = "bus_objct_data_id", nullable = false)
    private BusinessObjectDataEntity businessObjectData;

    @Column(name = "drcty_path_tx", length = 1024)
    private String directoryPath;

    @ManyToOne
    @JoinColumn(name = "strge_unit_stts_cd", referencedColumnName = "strge_unit_stts_cd", nullable = false)
    private StorageUnitStatusEntity status;

    @OneToMany(mappedBy = "storageUnit", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("createdOn")
    private Collection<StorageUnitStatusHistoryEntity> historicalStatuses;

    @Column(name = "rstr_xprtn_ts")
    private Timestamp restoreExpirationOn;

    @Column(name = "strge_plcy_trnsn_faild_atmpts_nb")
    private Integer storagePolicyTransitionFailedAttempts;

    @Column(name = "final_destroy_ts")
    private Timestamp finalDestroyOn;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public String getDirectoryPath()
    {
        return directoryPath;
    }

    public void setDirectoryPath(String directoryPath)
    {
        this.directoryPath = directoryPath;
    }

    public Collection<StorageFileEntity> getStorageFiles()
    {
        return storageFiles;
    }

    public void setStorageFiles(Collection<StorageFileEntity> storageFiles)
    {
        this.storageFiles = storageFiles;
    }

    public StorageEntity getStorage()
    {
        return storage;
    }

    public void setStorage(StorageEntity storage)
    {
        this.storage = storage;
    }

    public String getStorageName()
    {
        return storageName;
    }

    public void setStorageName(String storageName)
    {
        this.storageName = storageName;
    }

    public BusinessObjectDataEntity getBusinessObjectData()
    {
        return businessObjectData;
    }

    public void setBusinessObjectData(BusinessObjectDataEntity businessObjectData)
    {
        this.businessObjectData = businessObjectData;
    }

    public StorageUnitStatusEntity getStatus()
    {
        return status;
    }

    public void setStatus(StorageUnitStatusEntity status)
    {
        this.status = status;
    }

    public Collection<StorageUnitStatusHistoryEntity> getHistoricalStatuses()
    {
        return historicalStatuses;
    }

    public void setHistoricalStatuses(Collection<StorageUnitStatusHistoryEntity> historicalStatuses)
    {
        this.historicalStatuses = historicalStatuses;
    }

    public Timestamp getRestoreExpirationOn()
    {
        return restoreExpirationOn;
    }

    public void setRestoreExpirationOn(Timestamp restoreExpirationOn)
    {
        this.restoreExpirationOn = restoreExpirationOn;
    }

    public Integer getStoragePolicyTransitionFailedAttempts()
    {
        return storagePolicyTransitionFailedAttempts;
    }

    public void setStoragePolicyTransitionFailedAttempts(Integer storagePolicyTransitionFailedAttempts)
    {
        this.storagePolicyTransitionFailedAttempts = storagePolicyTransitionFailedAttempts;
    }

    public Timestamp getFinalDestroyOn()
    {
        return finalDestroyOn;
    }

    public void setFinalDestroyOn(Timestamp finalDestroyOn)
    {
        this.finalDestroyOn = finalDestroyOn;
    }
}
