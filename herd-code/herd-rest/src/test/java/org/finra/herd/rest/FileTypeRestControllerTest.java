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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.FileType;
import org.finra.herd.model.api.xml.FileTypeCreateRequest;
import org.finra.herd.model.api.xml.FileTypeKey;
import org.finra.herd.model.api.xml.FileTypeKeys;
import org.finra.herd.service.FileTypeService;

/**
 * unit tests for class {@link FileTypeRestController}.
 */
public class FileTypeRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private FileTypeRestController fileTypeRestController;

    @Mock
    private FileTypeService fileTypeService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateFileType() throws Exception
    {
        FileTypeCreateRequest fileTypeCreateRequest = new FileTypeCreateRequest(FORMAT_FILE_TYPE_CODE);
        // Create a file type.
        FileType fileType = new FileType(FORMAT_FILE_TYPE_CODE);
        when(fileTypeService.createFileType(fileTypeCreateRequest)).thenReturn(fileType);

        FileType resultFileType = fileTypeRestController.createFileType(new FileTypeCreateRequest(FORMAT_FILE_TYPE_CODE));

        // Validate the returned object.
        assertEquals(new FileType(FORMAT_FILE_TYPE_CODE), resultFileType);

        // Verify the external calls.
        verify(fileTypeService).createFileType(fileTypeCreateRequest);
        verifyNoMoreInteractions(fileTypeService);
        // Validate the returned object.
        assertEquals(fileType, resultFileType);
    }

    @Test
    public void testDeleteFileType() throws Exception
    {
        // Create a file type.
        FileType fileType = new FileType(FORMAT_FILE_TYPE_CODE);

        when(fileTypeService.deleteFileType(new FileTypeKey(FORMAT_FILE_TYPE_CODE))).thenReturn(fileType);

        FileType deletedFileType = fileTypeRestController.deleteFileType(FORMAT_FILE_TYPE_CODE);

        // Verify the external calls.
        verify(fileTypeService).deleteFileType(new FileTypeKey(FORMAT_FILE_TYPE_CODE));
        verifyNoMoreInteractions(fileTypeService);
        // Validate the returned object.
        assertEquals(fileType, deletedFileType);
    }

    @Test
    public void testGetFileType() throws Exception
    {
        FileType fileType = new FileType(FORMAT_FILE_TYPE_CODE);
        when(fileTypeService.getFileType(new FileTypeKey(FORMAT_FILE_TYPE_CODE))).thenReturn(fileType);

        // Retrieve the file type.
        FileType resultFileType = fileTypeRestController.getFileType(FORMAT_FILE_TYPE_CODE);

        // Verify the external calls.
        verify(fileTypeService).getFileType(new FileTypeKey(FORMAT_FILE_TYPE_CODE));
        verifyNoMoreInteractions(fileTypeService);
        // Validate the returned object.
        assertEquals(fileType, resultFileType);
    }

    @Test
    public void testGetFileTypes() throws Exception
    {
        // Get a list of test file type keys.
        List<FileTypeKey> testFileTypeKeys = Arrays.asList(new FileTypeKey(FORMAT_FILE_TYPE_CODE), new FileTypeKey(FORMAT_FILE_TYPE_CODE_2));
        FileTypeKeys fileTypeKeys = new FileTypeKeys(testFileTypeKeys);
        // Create and persist file type entities.
        when(fileTypeService.getFileTypes()).thenReturn(fileTypeKeys);

        // Retrieve a list of file type keys.
        FileTypeKeys resultFileTypeKeys = fileTypeRestController.getFileTypes();

        // Verify the external calls.
        verify(fileTypeService).getFileTypes();
        verifyNoMoreInteractions(fileTypeService);
        // Validate the returned object.
        assertEquals(fileTypeKeys, resultFileTypeKeys);
    }
}
