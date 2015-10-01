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

/**
 * A manifest file.
 */
public class ManifestFile
{
    private String fileName;
    private Long fileSizeBytes;
    private Long rowCount;

    public String getFileName()
    {
        return fileName;
    }

    public void setFileName(String fileName)
    {
        this.fileName = fileName;
    }

    public Long getRowCount()
    {
        return rowCount;
    }

    public void setRowCount(Long rowCount)
    {
        this.rowCount = rowCount;
    }

    public Long getFileSizeBytes()
    {
        return fileSizeBytes;
    }

    public void setFileSizeBytes(Long fileSizeBytes)
    {
        this.fileSizeBytes = fileSizeBytes;
    }

    /**
     * Manifest files are only considered equal if their fileName is equal. Other properties such as file size and row count are not considered.
     *
     * @param object the other object.
     *
     * @return true if the object is equal or false if not.
     */
    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }

        ManifestFile that = (ManifestFile) object;

        // Consider 2 manifest files "equal" if their names are equal.
        // Row count isn't being considered in case one file has it and the other doesn't since it is an optional field.
        if (fileName != null ? !fileName.equals(that.fileName) : that.fileName != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (fileName != null ? fileName.hashCode() : 0);
        result = 31 * result + (rowCount != null ? rowCount.hashCode() : 0);
        return result;
    }
}
