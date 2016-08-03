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
package org.finra.herd.dao;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.jpa.FileTypeEntity;

@Component
public class FileTypeDaoTestHelper
{
    @Autowired
    private FileTypeDao fileTypeDao;

    /**
     * Creates and persists a new file type entity.
     *
     * @param fileTypeCode the file type code value
     * @param fileTypeDescription the description of this file type
     *
     * @return the newly created file type entity.
     */
    public FileTypeEntity createFileTypeEntity(String fileTypeCode, String fileTypeDescription)
    {
        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(fileTypeCode);
        fileTypeEntity.setDescription(fileTypeDescription);
        return fileTypeDao.saveAndRefresh(fileTypeEntity);
    }

    /**
     * Creates and persists a new file type entity.
     *
     * @param fileTypeCode the file type code value
     *
     * @return the newly created file type entity.
     */
    public FileTypeEntity createFileTypeEntity(String fileTypeCode)
    {
        return createFileTypeEntity(fileTypeCode, String.format("Description of \"%s\" file type.", fileTypeCode));
    }

    /**
     * Creates and persists a new file type entity.
     *
     * @return the newly created file type entity.
     */
    public FileTypeEntity createFileTypeEntity()
    {
        String randomNumber = AbstractDaoTest.getRandomSuffix();
        return createFileTypeEntity("FileType" + randomNumber, "File Type " + randomNumber);
    }

    /**
     * Returns a list of test file type keys.
     *
     * @return the list of test file type keys
     */
    public List<FileTypeKey> getTestFileTypeKeys()
    {
        // Get a list of test file type keys.
        return Arrays.asList(new FileTypeKey(AbstractDaoTest.FORMAT_FILE_TYPE_CODE), new FileTypeKey(AbstractDaoTest.FORMAT_FILE_TYPE_CODE_2));
    }
}
