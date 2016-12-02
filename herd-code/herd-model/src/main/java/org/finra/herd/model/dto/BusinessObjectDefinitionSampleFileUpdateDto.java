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

/*
 * A Dto class for Business Object Definition Sample File Update
 */
public class BusinessObjectDefinitionSampleFileUpdateDto
{
    private String path;
    private String fileName;
    private long fileSize;

    /**
     * constructor
     * @param path path
     * @param fileName file name
     * @param fileSize file size
     */
    public BusinessObjectDefinitionSampleFileUpdateDto(String path, String fileName, long fileSize)
    {
        this.path = path;
        this.fileName = fileName;
        this.fileSize = fileSize;
    }
    
    /**
     * get path
     * @return path path
     */
    public String getPath()
    {
        return path;
    }

    /**
     * set path
     * @param path path
     */
    public void setPath(String path)
    {
        this.path = path;
    }

    /**
     * get file name
     * @return file name
     */
    public String getFileName()
    {
        return fileName;
    }

    /**
     * set file name
     * @param fileName file name
     */
    public void setFileName(String fileName)
    {
        this.fileName = fileName;
    }

    /**
     * get file size
     * @return file size
     */
    public long getFileSize()
    {
        return fileSize;
    }

    /**
     * set file size
     * @param fileSize file size
     */
    public void setFileSize(long fileSize)
    {
        this.fileSize = fileSize;
    }
}
