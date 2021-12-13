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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.S3Dao;
import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferResultsDto;
import org.finra.herd.service.impl.S3ServiceImpl;

/**
 * This class tests functionality within the S3 service.
 */
public class S3ServiceTest extends AbstractServiceTest
{
    @Mock
    private S3Dao s3Dao;

    @InjectMocks
    private S3ServiceImpl s3Service;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCopyFile() throws InterruptedException
    {
        // Create an S3 file copy request parameters DTO.
        S3FileCopyRequestParamsDto s3FileCopyRequestParamsDto = new S3FileCopyRequestParamsDto();

        // Create an S3 file transfer result DTO.
        S3FileTransferResultsDto s3FileTransferResultsDto = new S3FileTransferResultsDto();

        // Mock the external calls.
        when(s3Dao.copyFile(s3FileCopyRequestParamsDto)).thenReturn(s3FileTransferResultsDto);

        // Call the method under test.
        S3FileTransferResultsDto result = s3Service.copyFile(s3FileCopyRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).copyFile(s3FileCopyRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);

        // Validate the returned object.
        assertEquals(s3FileTransferResultsDto, result);
    }

    @Test
    public void testCreateDirectory()
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Call the method under test.
        s3Service.createDirectory(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).createDirectory(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void testDeleteDirectory()
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Call the method under test.
        s3Service.deleteDirectory(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).deleteDirectory(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void testDeleteDirectoryIgnoreException()
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Call the method under test.
        s3Service.deleteDirectoryIgnoreException(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).deleteDirectory(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void testDeleteDirectoryIgnoreExceptionWhenRuntimeExceptionOccurs()
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Mock the external calls.
        doThrow(new RuntimeException()).when(s3Dao).deleteDirectory(s3FileTransferRequestParamsDto);

        // Call the method under test.
        s3Service.deleteDirectoryIgnoreException(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).deleteDirectory(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void testDeleteFileList()
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Call the method under test.
        s3Service.deleteFileList(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).deleteFileList(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void testDownloadDirectory() throws InterruptedException
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an S3 file transfer result DTO.
        S3FileTransferResultsDto s3FileTransferResultsDto = new S3FileTransferResultsDto();

        // Mock the external calls.
        when(s3Dao.downloadDirectory(s3FileTransferRequestParamsDto)).thenReturn(s3FileTransferResultsDto);

        // Call the method under test.
        S3FileTransferResultsDto result = s3Service.downloadDirectory(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).downloadDirectory(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);

        // Validate the returned object.
        assertEquals(s3FileTransferResultsDto, result);
    }

    @Test
    public void testDownloadFile() throws InterruptedException
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an S3 file transfer result DTO.
        S3FileTransferResultsDto s3FileTransferResultsDto = new S3FileTransferResultsDto();

        // Mock the external calls.
        when(s3Dao.downloadFile(s3FileTransferRequestParamsDto)).thenReturn(s3FileTransferResultsDto);

        // Call the method under test.
        S3FileTransferResultsDto result = s3Service.downloadFile(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).downloadFile(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);

        // Validate the returned object.
        assertEquals(s3FileTransferResultsDto, result);
    }

    @Test
    public void testListDirectory()
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create a list of S3 object summaries.
        List<S3ObjectSummary> s3ObjectSummaries = Collections.singletonList(new S3ObjectSummary());

        // Mock the external calls.
        when(s3Dao.listDirectory(s3FileTransferRequestParamsDto, false)).thenReturn(s3ObjectSummaries);

        // Call the method under test.
        List<S3ObjectSummary> result = s3Service.listDirectory(s3FileTransferRequestParamsDto);

        // Verify the external calls. By default, we do not ignore 0 byte objects that represent S3 directories.
        verify(s3Dao).listDirectory(s3FileTransferRequestParamsDto, false);
        verifyNoMoreInteractions(s3Dao);

        // Validate the returned object.
        assertEquals(s3ObjectSummaries, result);
    }

    @Test
    public void testListDirectoryIgnoreZeroByteDirectoryMarkers()
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create a list of S3 object summaries.
        List<S3ObjectSummary> s3ObjectSummaries = Collections.singletonList(new S3ObjectSummary());

        // Mock the external calls.
        when(s3Dao.listDirectory(s3FileTransferRequestParamsDto, true)).thenReturn(s3ObjectSummaries);

        // Call the method under test.
        List<S3ObjectSummary> result = s3Service.listDirectory(s3FileTransferRequestParamsDto, true);

        // Verify the external calls.
        verify(s3Dao).listDirectory(s3FileTransferRequestParamsDto, true);
        verifyNoMoreInteractions(s3Dao);

        // Validate the returned object.
        assertEquals(s3ObjectSummaries, result);
    }

    @Test
    public void testRestoreObjects()
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Call the method under test.
        s3Service.restoreObjects(s3FileTransferRequestParamsDto, INTEGER_VALUE, ARCHIVE_RETRIEVAL_OPTION);

        // Verify the external calls.
        verify(s3Dao).restoreObjects(s3FileTransferRequestParamsDto, INTEGER_VALUE, ARCHIVE_RETRIEVAL_OPTION);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void testTagObjects()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an S3 file transfer request parameters DTO to tag S3 objects.
        S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto = new S3FileTransferRequestParamsDto();
        s3ObjectTaggerParamsDto.setAwsAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        s3ObjectTaggerParamsDto.setAwsSecretKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        s3ObjectTaggerParamsDto.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

        // Create an S3 object summary.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(S3_KEY);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Call the method under test.
        s3Service.tagObjects(s3FileTransferRequestParamsDto, s3ObjectTaggerParamsDto, Collections.singletonList(s3ObjectSummary), tag);

        // Verify the external calls.
        verify(s3Dao).tagObjects(s3FileTransferRequestParamsDto, s3ObjectTaggerParamsDto, Collections.singletonList(s3ObjectSummary), tag);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void testUploadDirectory() throws InterruptedException
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an S3 file transfer result DTO.
        S3FileTransferResultsDto s3FileTransferResultsDto = new S3FileTransferResultsDto();

        // Mock the external calls.
        when(s3Dao.uploadDirectory(s3FileTransferRequestParamsDto)).thenReturn(s3FileTransferResultsDto);

        // Call the method under test.
        S3FileTransferResultsDto result = s3Service.uploadDirectory(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).uploadDirectory(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);

        // Validate the returned object.
        assertEquals(s3FileTransferResultsDto, result);
    }

    @Test
    public void testUploadFile() throws InterruptedException
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an S3 file transfer result DTO.
        S3FileTransferResultsDto s3FileTransferResultsDto = new S3FileTransferResultsDto();

        // Mock the external calls.
        when(s3Dao.uploadFile(s3FileTransferRequestParamsDto)).thenReturn(s3FileTransferResultsDto);

        // Call the method under test.
        S3FileTransferResultsDto result = s3Service.uploadFile(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).uploadFile(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);

        // Validate the returned object.
        assertEquals(s3FileTransferResultsDto, result);
    }

    @Test
    public void testUploadFileList() throws InterruptedException
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Create an S3 file transfer result DTO.
        S3FileTransferResultsDto s3FileTransferResultsDto = new S3FileTransferResultsDto();

        // Mock the external calls.
        when(s3Dao.uploadFileList(s3FileTransferRequestParamsDto)).thenReturn(s3FileTransferResultsDto);

        // Call the method under test.
        S3FileTransferResultsDto result = s3Service.uploadFileList(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).uploadFileList(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);

        // Validate the returned object.
        assertEquals(s3FileTransferResultsDto, result);
    }

    @Test
    public void testValidateGlacierS3FilesRestored() throws RuntimeException
    {
        // Create an S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        // Call the method under test.
        s3Service.validateGlacierS3FilesRestored(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(s3Dao).validateGlacierS3FilesRestored(s3FileTransferRequestParamsDto);
        verifyNoMoreInteractions(s3Dao);
    }
}
