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

/**
 * A storage file.
 */
@Table(name = StorageFileEntity.TABLE_NAME)
@Entity
public class StorageFileEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "strge_file";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Long id;

    /**
     * The path column.
     */
    @Column(name = "fully_qlfd_file_nm", length = 1024)
    private String path;

    @Column(name = "file_size_in_bytes_nb")
    private Long fileSizeBytes;

    @Column(name = "row_ct")
    private Long rowCount;

    @ManyToOne
    @JoinColumn(name = "strge_unit_id", referencedColumnName = "strge_unit_id", nullable = false)
    private StorageUnitEntity storageUnit;

    @Column(name = "strge_unit_id", insertable = false, updatable = false)
    private Integer storageUnitId;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public String getPath()
    {
        return path;
    }

    public void setFileSizeBytes(Long fileSizeBytes)
    {
        this.fileSizeBytes = fileSizeBytes;
    }

    public Long getFileSizeBytes()
    {
        return fileSizeBytes;
    }

    public Long getRowCount()
    {
        return rowCount;
    }

    public void setRowCount(Long rowCount)
    {
        this.rowCount = rowCount;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public StorageUnitEntity getStorageUnit()
    {
        return storageUnit;
    }

    public void setStorageUnit(StorageUnitEntity storageUnit)
    {
        this.storageUnit = storageUnit;
    }

    public Integer getStorageUnitId()
    {
        return storageUnitId;
    }

    public void setStorageUnitId(Integer storageUnitId)
    {
        this.storageUnitId = storageUnitId;
    }
}
