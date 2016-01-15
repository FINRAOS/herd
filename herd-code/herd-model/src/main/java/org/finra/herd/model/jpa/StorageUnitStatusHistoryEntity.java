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
 * Status history associated with storage unit.
 */
@XmlRootElement
@XmlType
@Table(name = StorageUnitStatusHistoryEntity.TABLE_NAME)
@Entity
public class StorageUnitStatusHistoryEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "strge_unit_stts_hs";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq")
    private Integer id;

    @ManyToOne
    @JoinColumn(name = "strge_unit_id", referencedColumnName = "strge_unit_id", nullable = false)
    private StorageUnitEntity storageUnit;

    @ManyToOne
    @JoinColumn(name = "strge_unit_stts_cd", referencedColumnName = "strge_unit_stts_cd", nullable = false)
    private StorageUnitStatusEntity status;

    @Column(name = "rsn_tx", nullable = false)
    private String reason;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public StorageUnitEntity getStorageUnit()
    {
        return storageUnit;
    }

    public void setStorageUnit(StorageUnitEntity storageUnit)
    {
        this.storageUnit = storageUnit;
    }

    public StorageUnitStatusEntity getStatus()
    {
        return status;
    }

    public void setStatus(StorageUnitStatusEntity status)
    {
        this.status = status;
    }

    public String getReason()
    {
        return reason;
    }

    public void setReason(String reason)
    {
        this.reason = reason;
    }
}
