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

/**
 * A base class for the uploader and downloader manifests.
 */
public class DataBridgeBaseManifestDto
{
    protected String namespace;
    protected String businessObjectDefinitionName;
    protected String businessObjectFormatUsage;
    protected String businessObjectFormatFileType;
    protected String businessObjectFormatVersion;
    protected String partitionKey;
    protected String partitionValue;
    protected List<String> subPartitionValues;
    protected String storageName;

    public String getNamespace()
    {
        return namespace;
    }

    public void setNamespace(String namespace)
    {
        this.namespace = namespace;
    }

    public String getBusinessObjectDefinitionName()
    {
        return businessObjectDefinitionName;
    }

    public void setBusinessObjectDefinitionName(String businessObjectDefinitionName)
    {
        this.businessObjectDefinitionName = businessObjectDefinitionName;
    }

    public String getBusinessObjectFormatUsage()
    {
        return businessObjectFormatUsage;
    }

    public void setBusinessObjectFormatUsage(String businessObjectFormatUsage)
    {
        this.businessObjectFormatUsage = businessObjectFormatUsage;
    }

    public String getBusinessObjectFormatFileType()
    {
        return businessObjectFormatFileType;
    }

    public void setBusinessObjectFormatFileType(String businessObjectFormatFileType)
    {
        this.businessObjectFormatFileType = businessObjectFormatFileType;
    }

    public String getBusinessObjectFormatVersion()
    {
        return businessObjectFormatVersion;
    }

    public void setBusinessObjectFormatVersion(String businessObjectFormatVersion)
    {
        this.businessObjectFormatVersion = businessObjectFormatVersion;
    }

    public String getPartitionKey()
    {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey)
    {
        this.partitionKey = partitionKey;
    }

    public String getPartitionValue()
    {
        return partitionValue;
    }

    public void setPartitionValue(String partitionValue)
    {
        this.partitionValue = partitionValue;
    }

    public List<String> getSubPartitionValues()
    {
        return subPartitionValues;
    }

    public void setSubPartitionValues(List<String> subPartitionValues)
    {
        this.subPartitionValues = subPartitionValues;
    }

    public String getStorageName()
    {
        return storageName;
    }

    public void setStorageName(String storageName)
    {
        this.storageName = storageName;
    }
}
