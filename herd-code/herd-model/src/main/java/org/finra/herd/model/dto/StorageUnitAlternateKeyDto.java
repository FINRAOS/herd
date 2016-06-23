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
package org.finra.herd.model.dto;

import java.util.List;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;

/**
 * A DTO that contains the alternate key fields for storage unit.
 */
public class StorageUnitAlternateKeyDto
{
    protected String namespace;
    protected String businessObjectDefinitionName;
    protected String businessObjectFormatUsage;
    protected String businessObjectFormatFileType;
    protected Integer businessObjectFormatVersion;
    protected String partitionValue;
    protected List<String> subPartitionValues;
    protected Integer businessObjectDataVersion;
    protected String storageName;

    /**
     * Default no-arg constructor
     */
    public StorageUnitAlternateKeyDto()
    {
        super();
    }

    /**
     * Fully-initialising value constructor.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param businessObjectFormatUsage the usage of the business object format
     * @param businessObjectFormatFileType the file type of the business object format
     * @param businessObjectFormatVersion the version of the business object format
     * @param partitionValue the primary partition value
     * @param subPartitionValues the list of sub-partition values
     * @param businessObjectDataVersion the version of the business object data
     * @param storageName the name of the storage
     */
    public StorageUnitAlternateKeyDto(final String namespace, final String businessObjectDefinitionName, final String businessObjectFormatUsage,
        final String businessObjectFormatFileType, final Integer businessObjectFormatVersion, final String partitionValue,
        final List<String> subPartitionValues, final Integer businessObjectDataVersion, final String storageName)
    {
        this.namespace = namespace;
        this.businessObjectDefinitionName = businessObjectDefinitionName;
        this.businessObjectFormatUsage = businessObjectFormatUsage;
        this.businessObjectFormatFileType = businessObjectFormatFileType;
        this.businessObjectFormatVersion = businessObjectFormatVersion;
        this.partitionValue = partitionValue;
        this.subPartitionValues = subPartitionValues;
        this.businessObjectDataVersion = businessObjectDataVersion;
        this.storageName = storageName;
    }

    /**
     * Gets the value of the namespace property.
     *
     * @return possible object is {@link String }
     */
    public String getNamespace()
    {
        return namespace;
    }

    /**
     * Sets the value of the namespace property.
     *
     * @param value allowed object is {@link String }
     */
    public void setNamespace(String value)
    {
        this.namespace = value;
    }

    /**
     * Gets the value of the businessObjectDefinitionName property.
     *
     * @return possible object is {@link String }
     */
    public String getBusinessObjectDefinitionName()
    {
        return businessObjectDefinitionName;
    }

    /**
     * Sets the value of the businessObjectDefinitionName property.
     *
     * @param value allowed object is {@link String }
     */
    public void setBusinessObjectDefinitionName(String value)
    {
        this.businessObjectDefinitionName = value;
    }

    /**
     * Gets the value of the businessObjectFormatUsage property.
     *
     * @return possible object is {@link String }
     */
    public String getBusinessObjectFormatUsage()
    {
        return businessObjectFormatUsage;
    }

    /**
     * Sets the value of the businessObjectFormatUsage property.
     *
     * @param value allowed object is {@link String }
     */
    public void setBusinessObjectFormatUsage(String value)
    {
        this.businessObjectFormatUsage = value;
    }

    /**
     * Gets the value of the businessObjectFormatFileType property.
     *
     * @return possible object is {@link String }
     */
    public String getBusinessObjectFormatFileType()
    {
        return businessObjectFormatFileType;
    }

    /**
     * Sets the value of the businessObjectFormatFileType property.
     *
     * @param value allowed object is {@link String }
     */
    public void setBusinessObjectFormatFileType(String value)
    {
        this.businessObjectFormatFileType = value;
    }

    /**
     * Gets the value of the businessObjectFormatVersion property.
     *
     * @return possible object is {@link Integer }
     */
    public Integer getBusinessObjectFormatVersion()
    {
        return businessObjectFormatVersion;
    }

    /**
     * Sets the value of the businessObjectFormatVersion property.
     *
     * @param value allowed object is {@link Integer }
     */
    public void setBusinessObjectFormatVersion(Integer value)
    {
        this.businessObjectFormatVersion = value;
    }

    /**
     * Gets the value of the partitionValue property.
     *
     * @return possible object is {@link String }
     */
    public String getPartitionValue()
    {
        return partitionValue;
    }

    /**
     * Sets the value of the partitionValue property.
     *
     * @param value allowed object is {@link String }
     */
    public void setPartitionValue(String value)
    {
        this.partitionValue = value;
    }

    public List<String> getSubPartitionValues()
    {
        return subPartitionValues;
    }

    public void setSubPartitionValues(List<String> subPartitionValues)
    {
        this.subPartitionValues = subPartitionValues;
    }

    /**
     * Gets the value of the businessObjectDataVersion property.
     *
     * @return possible object is {@link Integer }
     */
    public Integer getBusinessObjectDataVersion()
    {
        return businessObjectDataVersion;
    }

    /**
     * Sets the value of the businessObjectDataVersion property.
     *
     * @param value allowed object is {@link Integer }
     */
    public void setBusinessObjectDataVersion(Integer value)
    {
        this.businessObjectDataVersion = value;
    }

    /**
     * Gets the value of the storageName property.
     *
     * @return possible object is {@link String }
     */
    public String getStorageName()
    {
        return storageName;
    }

    /**
     * Sets the value of the storageName property.
     *
     * @param value allowed object is {@link String }
     */
    public void setStorageName(String value)
    {
        this.storageName = value;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (!(object instanceof StorageUnitAlternateKeyDto))
        {
            return false;
        }

        StorageUnitAlternateKeyDto that = (StorageUnitAlternateKeyDto) object;

        BusinessObjectDataKey thisBusinessObjectDataKey =
            new BusinessObjectDataKey(this.getNamespace(), this.getBusinessObjectDefinitionName(), this.businessObjectFormatUsage,
                this.getBusinessObjectFormatFileType(), this.getBusinessObjectFormatVersion(), this.getPartitionValue(), this.getSubPartitionValues(),
                this.getBusinessObjectDataVersion());

        BusinessObjectDataKey thatBusinessObjectDataKey =
            new BusinessObjectDataKey(that.getNamespace(), that.getBusinessObjectDefinitionName(), that.businessObjectFormatUsage,
                that.getBusinessObjectFormatFileType(), that.getBusinessObjectFormatVersion(), that.getPartitionValue(), that.getSubPartitionValues(),
                that.getBusinessObjectDataVersion());

        if (!thisBusinessObjectDataKey.equals(thatBusinessObjectDataKey))
        {
            return false;
        }
        if (storageName != null ? !storageName.equals(that.storageName) : that.storageName != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = namespace != null ? namespace.hashCode() : 0;
        result = 31 * result + (businessObjectDefinitionName != null ? businessObjectDefinitionName.hashCode() : 0);
        result = 31 * result + (businessObjectFormatUsage != null ? businessObjectFormatUsage.hashCode() : 0);
        result = 31 * result + (businessObjectFormatFileType != null ? businessObjectFormatFileType.hashCode() : 0);
        result = 31 * result + (businessObjectFormatVersion != null ? businessObjectFormatVersion.hashCode() : 0);
        result = 31 * result + (partitionValue != null ? partitionValue.hashCode() : 0);
        result = 31 * result + (subPartitionValues != null ? subPartitionValues.hashCode() : 0);
        result = 31 * result + (businessObjectDataVersion != null ? businessObjectDataVersion.hashCode() : 0);
        result = 31 * result + (storageName != null ? storageName.hashCode() : 0);
        return result;
    }
}
