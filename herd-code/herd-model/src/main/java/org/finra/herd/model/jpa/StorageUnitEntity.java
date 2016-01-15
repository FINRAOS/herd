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
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A storage unit.
 */
@XmlRootElement
@XmlType
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
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq")
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

    // This is an optional parent storage unit.
    @ManyToOne
    @JoinColumn(name = "prnt_strge_unit_id", referencedColumnName = TABLE_NAME + "_id")
    private StorageUnitEntity parentStorageUnit;

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

    public StorageUnitEntity getParentStorageUnit()
    {
        return parentStorageUnit;
    }

    public void setParentStorageUnit(StorageUnitEntity parentStorageUnit)
    {
        this.parentStorageUnit = parentStorageUnit;
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

        StorageUnitEntity that = (StorageUnitEntity) other;

        if (!businessObjectData.equals(that.businessObjectData))
        {
            return false;
        }
        if (directoryPath != null ? !directoryPath.equals(that.directoryPath) : that.directoryPath != null)
        {
            return false;
        }
        if (!id.equals(that.id))
        {
            return false;
        }
        if (!storage.equals(that.storage))
        {
            return false;
        }
        if (storageFiles != null ? !storageFiles.equals(that.storageFiles) : that.storageFiles != null)
        {
            return false;
        }
        if (!status.equals(that.status))
        {
            return false;
        }
        if (parentStorageUnit != null ? !parentStorageUnit.equals(that.parentStorageUnit) : that.parentStorageUnit != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = id.hashCode();
        result = 31 * result + (storageFiles != null ? storageFiles.hashCode() : 0);
        result = 31 * result + storage.hashCode();
        result = 31 * result + businessObjectData.hashCode();
        result = 31 * result + (directoryPath != null ? directoryPath.hashCode() : 0);
        return result;
    }
}
