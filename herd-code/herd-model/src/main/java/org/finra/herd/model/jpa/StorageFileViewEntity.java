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

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "biz_dt_file_vw")
public class StorageFileViewEntity
{
    @Id
    @Column(name = "strge_file_id")
    private Long storageFileId;

    @Column(name = "namespace")
    private String namespaceCode;

    @Column(name = "bus_object_name")
    private String businessObjectDefinitionName;

    @Column(name = "data_provider")
    private String dataProviderCode;

    @Column(name = "storage_code")
    private String storageCode;

    @Column(name = "file_size_in_bytes_nb")
    private Long fileSizeInBytes;

    @Column(name = "creat_td")
    private Date createdDate;

    public Long getStorageFileId()
    {
        return storageFileId;
    }

    public void setStorageFileId(Long storageFileId)
    {
        this.storageFileId = storageFileId;
    }

    public String getNamespaceCode()
    {
        return namespaceCode;
    }

    public void setNamespaceCode(String namespaceCode)
    {
        this.namespaceCode = namespaceCode;
    }

    public String getBusinessObjectDefinitionName()
    {
        return businessObjectDefinitionName;
    }

    public void setBusinessObjectDefinitionName(String businessObjectDefinitionName)
    {
        this.businessObjectDefinitionName = businessObjectDefinitionName;
    }

    public String getDataProviderCode()
    {
        return dataProviderCode;
    }

    public void setDataProviderCode(String dataProviderCode)
    {
        this.dataProviderCode = dataProviderCode;
    }

    public String getStorageCode()
    {
        return storageCode;
    }

    public void setStorageCode(String storageCode)
    {
        this.storageCode = storageCode;
    }

    public Long getFileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    public void setFileSizeInBytes(Long fileSizeInBytes)
    {
        this.fileSizeInBytes = fileSizeInBytes;
    }

    public Date getCreatedDate()
    {
        return createdDate;
    }

    public void setCreatedDate(Date createdDate)
    {
        this.createdDate = createdDate;
    }
}
