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
package org.finra.dm.model.dto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.finra.dm.model.api.xml.BusinessObjectDataKey;

/**
 * Implements output JSON manifest file for the downloader.
 */
public class DownloaderOutputManifestDto extends DataBridgeBaseManifestDto
{
    private String businessObjectDataVersion;
    private List<ManifestFile> manifestFiles = new ArrayList<>();
    private HashMap<String, String> attributes = new HashMap<>();
    private List<BusinessObjectDataKey> businessObjectDataParents = new ArrayList<>();
    private List<BusinessObjectDataKey> businessObjectDataChildren = new ArrayList<>();

    public String getBusinessObjectDataVersion()
    {
        return businessObjectDataVersion;
    }

    public void setBusinessObjectDataVersion(String businessObjectDataVersion)
    {
        this.businessObjectDataVersion = businessObjectDataVersion;
    }

    public List<ManifestFile> getManifestFiles()
    {
        return manifestFiles;
    }

    public void setManifestFiles(List<ManifestFile> manifestFiles)
    {
        this.manifestFiles = manifestFiles;
    }

    public HashMap<String, String> getAttributes()
    {
        return this.attributes;
    }

    public void setAttributes(HashMap<String, String> attributes)
    {
        this.attributes = attributes;
    }

    public List<BusinessObjectDataKey> getBusinessObjectDataParents()
    {
        return businessObjectDataParents;
    }

    public void setBusinessObjectDataParents(List<BusinessObjectDataKey> businessObjectDataParents)
    {
        this.businessObjectDataParents = businessObjectDataParents;
    }

    public List<BusinessObjectDataKey> getBusinessObjectDataChildren()
    {
        return businessObjectDataChildren;
    }

    public void setBusinessObjectDataChildren(List<BusinessObjectDataKey> businessObjectDataChildren)
    {
        this.businessObjectDataChildren = businessObjectDataChildren;
    }
}
