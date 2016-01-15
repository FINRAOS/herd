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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;

/**
 * An input manifest for the uploader.
 */
public class UploaderInputManifestDto extends DataBridgeBaseManifestDto
{
    private List<String> files = new ArrayList<>();
    private List<ManifestFile> manifestFiles = new ArrayList<>();
    private HashMap<String, String> attributes = new HashMap<>();
    private List<BusinessObjectDataKey> businessObjectDataParents = new ArrayList<>();

    /**
     * Gets a list of files (i.e. file names).
     *
     * @return the list of files.
     * @deprecated Use the getManifestFiles method instead which supports file names as well as other properties.
     */
    public List<String> getFiles()
    {
        return files;
    }

    /**
     * Sets a list of files (i.e. file names).
     *
     * @param files the list of file files.
     *
     * @deprecated Use the setManifestFiles method instead which supports file names as well as other properties.
     */
    public void setFiles(List<String> files)
    {
        this.files = files;
    }

    /**
     * Gets the list of manifest files. If the manifest files are empty, then the files will be returned, empty or not. However, if both the manifest files and
     * files are both not empty, an IllegalArgumentException will be thrown since only one list of files is permitted. This method will be modified to just
     * return the list of manifest files when the deprecated setFiles method is removed.
     *
     * @return the list of manifest files.
     */
    public List<ManifestFile> getManifestFiles()
    {
        if (manifestFiles.isEmpty())
        {
            // "Manifest files" have not been specified so use the "files".
            List<ManifestFile> returnManifestFiles = new ArrayList<>();
            for (String fileName : files)
            {
                ManifestFile manifestFile = new ManifestFile();
                returnManifestFiles.add(manifestFile);
                manifestFile.setFileName(fileName.trim());
            }
            return returnManifestFiles;
        }
        else
        {
            if (!files.isEmpty())
            {
                // "Manifest files" and "files" can't both be present so throw an exception.
                throw new IllegalArgumentException("\"Manifest files\" and \"files\" can't both be specified.");
            }

            // Only the manifest files have been specified (i.e. no files) so return them.
            return manifestFiles;
        }
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
}
