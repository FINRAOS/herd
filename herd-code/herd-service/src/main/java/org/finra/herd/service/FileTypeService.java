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
package org.finra.herd.service;

import org.finra.herd.model.api.xml.FileType;
import org.finra.herd.model.api.xml.FileTypeCreateRequest;
import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.api.xml.FileTypeKeys;

/**
 * The file type service.
 */
public interface FileTypeService
{
    /**
     * Creates a new file type.
     *
     * @param fileTypeCreateRequest the file type create request
     *
     * @return the created file type
     */
    public FileType createFileType(FileTypeCreateRequest fileTypeCreateRequest);

    /**
     * Gets a file type for the specified key.
     *
     * @param fileTypeKey the file type key
     *
     * @return the file type
     */
    public FileType getFileType(FileTypeKey fileTypeKey);

    /**
     * Deletes a file type for the specified name.
     *
     * @param fileTypeKey the file type key
     *
     * @return the file type that was deleted
     */
    public FileType deleteFileType(FileTypeKey fileTypeKey);

    /**
     * Gets a list of file type keys for all file types defined in the system.
     *
     * @return the file type keys
     */
    public FileTypeKeys getFileTypes();
}
