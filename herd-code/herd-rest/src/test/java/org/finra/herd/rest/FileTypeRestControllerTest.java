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
package org.finra.herd.rest;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.api.xml.FileTypeKeys;

/**
 * This class tests various functionality within the file type REST controller.
 */
public class FileTypeRestControllerTest extends AbstractRestTest
{
    @Test
    public void testGetFileTypes() throws Exception
    {
        // Get a list of test file type keys.
        List<FileTypeKey> testFileTypeKeys = Arrays.asList(new FileTypeKey(FORMAT_FILE_TYPE_CODE), new FileTypeKey(FORMAT_FILE_TYPE_CODE_2));

        // Create and persist file type entities.
        for (FileTypeKey key : testFileTypeKeys)
        {
            fileTypeDaoTestHelper.createFileTypeEntity(key.getFileTypeCode());
        }

        // Retrieve a list of file type keys.
        FileTypeKeys resultFileTypeKeys = fileTypeRestController.getFileTypes();

        // Validate the returned object.
        assertNotNull(resultFileTypeKeys);
        assertNotNull(resultFileTypeKeys.getFileTypeKeys());
        assertTrue(resultFileTypeKeys.getFileTypeKeys().size() >= testFileTypeKeys.size());
        for (FileTypeKey key : testFileTypeKeys)
        {
            assertTrue(resultFileTypeKeys.getFileTypeKeys().contains(key));
        }
    }
}
