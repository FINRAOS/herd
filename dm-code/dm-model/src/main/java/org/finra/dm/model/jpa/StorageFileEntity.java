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
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A storage file.
 */
@XmlRootElement
@XmlType
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
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq")
    private Integer id;

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

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
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

        StorageFileEntity that = (StorageFileEntity) o;

        if (id != null ? !id.equals(that.id) : that.id != null)
        {
            return false;
        }
        if (path != null ? !path.equals(that.path) : that.path != null)
        {
            return false;
        }

        if (fileSizeBytes != null ? !fileSizeBytes.equals(that.fileSizeBytes) : that.fileSizeBytes != null)
        {
            return false;
        }

        if (rowCount != null ? !rowCount.equals(that.rowCount) : that.rowCount != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (path != null ? path.hashCode() : 0);
        result = 31 * result + (fileSizeBytes != null ? fileSizeBytes.hashCode() : 0);
        result = 31 * result + (rowCount != null ? rowCount.hashCode() : 0);
        return result;
    }
}
