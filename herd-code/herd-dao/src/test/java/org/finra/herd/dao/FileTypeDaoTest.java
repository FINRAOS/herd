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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.jpa.FileTypeEntity;

public class FileTypeDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetFileTypeByCode()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        // Retrieve file type entity.
        FileTypeEntity fileTypeEntity = fileTypeDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE);

        // Validate the results.
        assertNotNull(fileTypeEntity);
        assertTrue(fileTypeEntity.getCode().equals(FORMAT_FILE_TYPE_CODE));
        assertTrue(fileTypeEntity.getDescription().equals("Description of " + FORMAT_FILE_TYPE_CODE));
    }

    @Test
    public void testGetFileTypeByCodeInUpperCase()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toLowerCase(), "Description of " + FORMAT_FILE_TYPE_CODE.toLowerCase());

        // Retrieve file type entity.
        FileTypeEntity fileTypeEntity = fileTypeDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE.toUpperCase());

        // Validate the results.
        assertNotNull(fileTypeEntity);
        assertTrue(fileTypeEntity.getCode().equals(FORMAT_FILE_TYPE_CODE.toLowerCase()));
        assertTrue(fileTypeEntity.getDescription().equals("Description of " + FORMAT_FILE_TYPE_CODE.toLowerCase()));
    }

    @Test
    public void testGetFileTypeByCodeInLowerCase()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toUpperCase(), "Description of " + FORMAT_FILE_TYPE_CODE.toUpperCase());

        // Retrieve file type entity.
        FileTypeEntity fileTypeEntity = fileTypeDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE.toLowerCase());

        // Validate the results.
        assertNotNull(fileTypeEntity);
        assertTrue(fileTypeEntity.getCode().equals(FORMAT_FILE_TYPE_CODE.toUpperCase()));
        assertTrue(fileTypeEntity.getDescription().equals("Description of " + FORMAT_FILE_TYPE_CODE.toUpperCase()));
    }

    @Test
    public void testGetFileTypeByCodeInvalidCode()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE, "Description of " + FORMAT_FILE_TYPE_CODE);

        // Try to retrieve file type entity using an invalid code value.
        assertNull(fileTypeDao.getFileTypeByCode("I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetFileTypeByCodeMultipleRecordsFound()
    {
        // Create relative database entities.
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toUpperCase(), "Description of " + FORMAT_FILE_TYPE_CODE.toUpperCase());
        createFileTypeEntity(FORMAT_FILE_TYPE_CODE.toLowerCase(), "Description of " + FORMAT_FILE_TYPE_CODE.toLowerCase());

        try
        {
            // Try retrieve file type entity.
            fileTypeDao.getFileTypeByCode(FORMAT_FILE_TYPE_CODE);
            fail("Should throw an IllegalArgumentException if finds more than one file type entities with the same code.");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().startsWith("Found more than one file type"));
        }
    }

    @Test
    public void testGetFileTypes() throws Exception
    {
        // Create and persist file type entities.
        for (FileTypeKey key : getTestFileTypeKeys())
        {
            createFileTypeEntity(key.getFileTypeCode());
        }

        // Retrieve a list of file type keys.
        List<FileTypeKey> resultFileTypeKeys = fileTypeDao.getFileTypes();

        // Validate the returned object.
        assertNotNull(resultFileTypeKeys);
        assertTrue(resultFileTypeKeys.containsAll(getTestFileTypeKeys()));
    }
}
