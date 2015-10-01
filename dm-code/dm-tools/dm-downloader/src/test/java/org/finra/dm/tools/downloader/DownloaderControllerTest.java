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
package org.finra.dm.tools.downloader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import org.finra.dm.dao.impl.MockHttpClientOperationsImpl;
import org.finra.dm.model.dto.DmRegServerAccessParamsDto;
import org.finra.dm.model.dto.DownloaderInputManifestDto;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.dto.UploaderInputManifestDto;
import org.finra.dm.tools.common.databridge.DataBridgeWebClient;

/**
 * Unit tests for DownloaderController class.
 */
public class DownloaderControllerTest extends AbstractDownloaderTest
{
    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();

        uploadAndRegisterTestData(S3_SIMPLE_TEST_PATH);

        // Set the web client logger to warn level so we don't get unnecessary info level logging on the output.
        Logger.getLogger(DataBridgeWebClient.class).setLevel(Level.WARN);
        Logger.getLogger(DownloaderWebClient.class).setLevel(Level.WARN);
    }

    @Test
    public void testPerformDownload() throws Exception
    {
        runDownload();
    }

    @Test(expected = IOException.class)
    public void testPerformDownloadWithIoException() throws Exception
    {
        runDownload(getTestDownloaderInputManifestDto(), LOCAL_TEMP_PATH_OUTPUT.toString(), DownloaderController.MIN_THREADS,
            MockHttpClientOperationsImpl.HOSTNAME_THROW_IO_EXCEPTION_DURING_GET_STORAGES);
    }

    @Test
    public void testPerformDownloadTargetLocalDirectoryNoExists() throws Exception
    {
        Path localPath = Paths.get(LOCAL_TEMP_PATH_OUTPUT.toString(), "folder1" + RANDOM_SUFFIX, "folder2" + RANDOM_SUFFIX);
        runDownload(getTestDownloaderInputManifestDto(), localPath.toString(), DownloaderController.MIN_THREADS, null);
    }

    @Test
    public void testPerformDownloadInvalidLocalPath() throws Exception
    {
        File localFile = createLocalFile(LOCAL_TEMP_PATH_OUTPUT.toString(), LOCAL_FILE, FILE_SIZE_1_KB);
        try
        {
            // Try to download business object data when target local directory does not exist and cannot be created due to an invalid local path.
            runDownload(getTestDownloaderInputManifestDto(), localFile.toString(), DownloaderController.MIN_THREADS, null);
            fail("Should throw an IllegalArgumentException when local directory does not exist and cannot be created.");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().startsWith("Failed to create target local directory "));
        }
    }

    @Test
    public void testPerformDownloadTargetLocalDirectoryAlreadyExists() throws Exception
    {
        // Create an empty target local directory.
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH_OUTPUT.toString(), S3_SIMPLE_TEST_PATH).toFile();
        assertTrue(targetLocalDirectory.mkdirs());

        runDownload();
    }

    @Test
    public void testPerformDownloadTargetLocalDirectoryNotEmpty() throws Exception
    {
        // Create a target local directory with a subfolder.
        File targetLocalDirectory = Paths.get(LOCAL_TEMP_PATH_OUTPUT.toString(), S3_SIMPLE_TEST_PATH).toFile();
        assertTrue(Paths.get(targetLocalDirectory.toString(), "folder" + RANDOM_SUFFIX).toFile().mkdirs());

        // Try to perform a download when the specified target local directory already exists and it is not empty.
        try
        {
            runDownload();
            fail("Suppose to throw an IllegalArgumentException when the specified target local directory already exists and it is not empty.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("The target local directory \"%s\" is not empty.", targetLocalDirectory.toString()), e.getMessage());
        }
    }

    @Test
    public void testPerformDownloadWithLoggerLevelSetToWarn() throws Exception
    {
        Logger logger = Logger.getLogger(DownloaderController.class);
        Level origLoggerLevel = logger.getEffectiveLevel();
        logger.setLevel(Level.WARN);

        try
        {
            runDownload();
        }
        finally
        {
            logger.setLevel(origLoggerLevel);
        }
    }

    @Test
    public void testPerformDownloadBusinessObjectDataHasAttributes() throws Exception
    {
        // Add attributes to the test business object data.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        HashMap<String, String> attributes = new HashMap<>();
        uploaderInputManifestDto.setAttributes(attributes);
        attributes.put(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);
        attributes.put(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2);

        // Prepare test data and runs a normal download scenario.
        runDownload();
    }

    @Test
    public void testPerformDownloadZeroByteDirectoryMarkersPresent() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(downloaderWebClient);

        // Upload and register the initial version if of the test business object data.
        uploadAndRegisterTestData(S3_TEST_PATH_V0, testManifestFiles, S3_DIRECTORY_MARKERS);

        // Create a downloader input manifest file in LOCAL_TEMP_PATH_INPUT directory
        File downloaderInputManifestFile = createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), getTestDownloaderInputManifestDto());

        // Adjust the S3 file transfer parameters to be passed to the downloader controller.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setLocalPath(LOCAL_TEMP_PATH_OUTPUT.toString());
        s3FileTransferRequestParamsDto.setMaxThreads(DownloaderController.MIN_THREADS);

        // Perform the download.
        DmRegServerAccessParamsDto dmRegServerAccessParamsDto =
            DmRegServerAccessParamsDto.builder().dmRegServerHost(WEB_SERVICE_HOSTNAME).dmRegServerPort(WEB_SERVICE_HTTPS_PORT).useSsl(true)
                .username(WEB_SERVICE_HTTPS_USERNAME).password(WEB_SERVICE_HTTPS_PASSWORD).build();
        downloaderController.performDownload(dmRegServerAccessParamsDto, downloaderInputManifestFile, s3FileTransferRequestParamsDto);
    }

    /**
     * Prepares test data and runs a normal download scenario.
     *
     * @param downloaderInputManifestDto the downloader input manifest object instance
     * @param localPath the local target directory
     * @param numOfThreads the maximum number of threads to use for file transfer to S3
     * @param hostname optional override of the default web service hostname.
     */
    protected void runDownload(DownloaderInputManifestDto downloaderInputManifestDto, String localPath, Integer numOfThreads, String hostname) throws Exception
    {
        String hostnameToUse = hostname == null ? WEB_SERVICE_HOSTNAME : hostname;

        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(downloaderWebClient);

        // Upload and register the initial version if of the test business object data.
        uploadAndRegisterTestData(S3_TEST_PATH_V0);

        // Create a downloader input manifest file in LOCAL_TEMP_PATH_INPUT directory
        File downloaderInputManifestFile = createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), downloaderInputManifestDto);

        // Perform the download.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(S3_TEST_PATH_V0);
        s3FileTransferRequestParamsDto.setLocalPath(localPath);
        s3FileTransferRequestParamsDto.setMaxThreads(numOfThreads);
        DmRegServerAccessParamsDto dmRegServerAccessParamsDto =
            DmRegServerAccessParamsDto.builder().dmRegServerHost(hostnameToUse).dmRegServerPort(WEB_SERVICE_HTTPS_PORT).useSsl(true)
                .username(WEB_SERVICE_HTTPS_USERNAME).password(WEB_SERVICE_HTTPS_PASSWORD).build();
        downloaderController.performDownload(dmRegServerAccessParamsDto, downloaderInputManifestFile, s3FileTransferRequestParamsDto);
    }

    /**
     * Prepares test data and runs a normal download scenario using test output directory and minimum allowed number of threads.
     */
    protected void runDownload() throws Exception
    {
        runDownload(getTestDownloaderInputManifestDto(), LOCAL_TEMP_PATH_OUTPUT.toString(), DownloaderController.MIN_THREADS, null);
    }
}
