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
package org.finra.dm.service.helper;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import org.finra.dm.model.api.xml.StorageFile;

/**
 * A helper class for StorageFile related data management code.
 */
@Component
public class StorageFileHelper
{
    /**
     * Returns a list of file paths extracted from the specified list of storage files.
     *
     * @param storageFiles the list of storage files
     *
     * @return the list of file paths
     */
    public List<String> getFilePaths(List<StorageFile> storageFiles)
    {
        List<String> filePaths = new ArrayList<>();

        for (StorageFile storageFile : storageFiles)
        {
            filePaths.add(storageFile.getFilePath());
        }

        return filePaths;
    }
}
