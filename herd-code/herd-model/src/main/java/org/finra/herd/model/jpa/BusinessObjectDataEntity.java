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
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import org.hibernate.annotations.Type;

/**
 * An instance of business object data.
 */
@Table(name = BusinessObjectDataEntity.TABLE_NAME)
@Entity
public class BusinessObjectDataEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "bus_objct_data";

    /**
     * Maximum number of subpartition values allowed.
     */
    public static final int MAX_SUBPARTITIONS = 4;

    /**
     * Position of the first partition column.
     */
    public static final int FIRST_PARTITION_COLUMN_POSITION = 1;

    /**
     * The initial version to use when creating a business object data record.
     */
    public static final int BUSINESS_OBJECT_DATA_INITIAL_VERSION = 0;

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    @Column(name = "prtn_value_tx")
    private String partitionValue;

    @Column(name = "prtn_value_2_tx")
    private String partitionValue2;

    @Column(name = "prtn_value_3_tx")
    private String partitionValue3;

    @Column(name = "prtn_value_4_tx")
    private String partitionValue4;

    @Column(name = "prtn_value_5_tx")
    private String partitionValue5;

    @Column(name = "vrsn_nb")
    private Integer version;

    @Column(name = "ltst_vrsn_fl")
    @Type(type = "yes_no")
    private Boolean latestVersion;

    @ManyToOne
    @JoinColumn(name = "bus_objct_frmt_id", referencedColumnName = "bus_objct_frmt_id", nullable = false)
    private BusinessObjectFormatEntity businessObjectFormat;

    @OneToMany(mappedBy = "businessObjectData", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("storage")
    private Collection<StorageUnitEntity> storageUnits;

    @OneToMany(mappedBy = "businessObjectData", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("name")
    private Collection<BusinessObjectDataAttributeEntity> attributes;

    // These are the parents (i.e. the data that was needed to create this data).
    @JoinTable(name = "bus_objct_data_prnt", joinColumns = {
        @JoinColumn(name = TABLE_NAME + "_id", referencedColumnName = TABLE_NAME + "_id")}, inverseJoinColumns = {
        @JoinColumn(name = "prnt_bus_objct_data_id", referencedColumnName = TABLE_NAME + "_id")})
    @ManyToMany
    private List<BusinessObjectDataEntity> businessObjectDataParents;

    // These are the children (i.e. the data that is dependent on this data).
    @ManyToMany(mappedBy = "businessObjectDataParents")
    private List<BusinessObjectDataEntity> businessObjectDataChildren;

    @ManyToOne
    @JoinColumn(name = "bus_objct_data_stts_cd", referencedColumnName = "bus_objct_data_stts_cd", nullable = false)
    private BusinessObjectDataStatusEntity status;

    @Column(name = "bus_objct_data_stts_cd", insertable = false, updatable = false)
    private String statusCode;

    @OneToMany(mappedBy = "businessObjectData", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("createdOn DESC")
    private Collection<BusinessObjectDataStatusHistoryEntity> historicalStatuses;

    @Column(name = "rtntn_xprtn_ts")
    private Timestamp retentionExpiration;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public String getPartitionValue()
    {
        return partitionValue;
    }

    public void setPartitionValue(String partitionValue)
    {
        this.partitionValue = partitionValue;
    }

    public String getPartitionValue2()
    {
        return partitionValue2;
    }

    public void setPartitionValue2(String partitionValue2)
    {
        this.partitionValue2 = partitionValue2;
    }

    public String getPartitionValue3()
    {
        return partitionValue3;
    }

    public void setPartitionValue3(String partitionValue3)
    {
        this.partitionValue3 = partitionValue3;
    }

    public String getPartitionValue4()
    {
        return partitionValue4;
    }

    public void setPartitionValue4(String partitionValue4)
    {
        this.partitionValue4 = partitionValue4;
    }

    public String getPartitionValue5()
    {
        return partitionValue5;
    }

    public void setPartitionValue5(String partitionValue5)
    {
        this.partitionValue5 = partitionValue5;
    }

    public Integer getVersion()
    {
        return version;
    }

    public void setVersion(Integer version)
    {
        this.version = version;
    }

    public Boolean getLatestVersion()
    {
        return latestVersion;
    }

    public void setLatestVersion(Boolean latestVersion)
    {
        this.latestVersion = latestVersion;
    }

    public BusinessObjectFormatEntity getBusinessObjectFormat()
    {
        return businessObjectFormat;
    }

    public void setBusinessObjectFormat(BusinessObjectFormatEntity businessObjectFormat)
    {
        this.businessObjectFormat = businessObjectFormat;
    }

    public Collection<StorageUnitEntity> getStorageUnits()
    {
        return storageUnits;
    }

    public void setStorageUnits(Collection<StorageUnitEntity> storageUnits)
    {
        this.storageUnits = storageUnits;
    }

    public Collection<BusinessObjectDataAttributeEntity> getAttributes()
    {
        return attributes;
    }

    public void setAttributes(Collection<BusinessObjectDataAttributeEntity> attributes)
    {
        this.attributes = attributes;
    }

    public List<BusinessObjectDataEntity> getBusinessObjectDataParents()
    {
        return businessObjectDataParents;
    }

    public void setBusinessObjectDataParents(List<BusinessObjectDataEntity> businessObjectDataParents)
    {
        this.businessObjectDataParents = businessObjectDataParents;
    }

    public List<BusinessObjectDataEntity> getBusinessObjectDataChildren()
    {
        return businessObjectDataChildren;
    }

    public void setBusinessObjectDataChildren(List<BusinessObjectDataEntity> businessObjectDataChildren)
    {
        this.businessObjectDataChildren = businessObjectDataChildren;
    }

    public BusinessObjectDataStatusEntity getStatus()
    {
        return status;
    }

    public void setStatus(BusinessObjectDataStatusEntity status)
    {
        this.status = status;
    }

    public String getStatusCode()
    {
        return statusCode;
    }

    public void setStatusCode(String statusCode)
    {
        this.statusCode = statusCode;
    }

    public Collection<BusinessObjectDataStatusHistoryEntity> getHistoricalStatuses()
    {
        return historicalStatuses;
    }

    public void setHistoricalStatuses(Collection<BusinessObjectDataStatusHistoryEntity> historicalStatuses)
    {
        this.historicalStatuses = historicalStatuses;
    }

    public Timestamp getRetentionExpiration()
    {
        return retentionExpiration;
    }

    public void setRetentionExpiration(Timestamp retentionExpiration)
    {
        this.retentionExpiration = retentionExpiration;
    }
}
