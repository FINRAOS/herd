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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.FileTypeDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.FileTypeEntity;

/**
 * Helper for file type related operations which require DAO.
 */
@Component
public class FileTypeDaoHelper
{
    @Autowired
    private FileTypeDao fileTypeDao;

    /**
     * Gets the file type entity and ensure it exists.
     *
     * @param fileType the file type (case insensitive)
     *
     * @return the file type entity
     * @throws ObjectNotFoundException if the file type entity doesn't exist
     */
    public FileTypeEntity getFileTypeEntity(String fileType) throws ObjectNotFoundException
    {
        FileTypeEntity fileTypeEntity = fileTypeDao.getFileTypeByCode(fileType);

        if (fileTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("File type with code \"%s\" doesn't exist.", fileType));
        }

        return fileTypeEntity;
    }
}
