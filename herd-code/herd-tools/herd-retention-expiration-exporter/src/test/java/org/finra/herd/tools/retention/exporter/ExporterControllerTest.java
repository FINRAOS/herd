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
package org.finra.herd.tools.retention.exporter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.dao.impl.MockHttpClientOperationsImpl;
import org.finra.herd.dao.impl.S3DaoImpl;
import org.finra.herd.model.dto.ManifestFile;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.UploaderInputManifestDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

/**
 * Unit tests for UploaderController class.
 */
public class ExporterControllerTest extends AbstractExporterTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExporterControllerTest.class);

    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();

        // Set the web client logger to warn level so we don't get unnecessary info level logging on the output.
        setLogLevel(DataBridgeWebClient.class, LogLevel.WARN);
        setLogLevel(ExporterWebClient.class, LogLevel.WARN);
        setLogLevel(S3DaoImpl.class, LogLevel.WARN);
    }

    @Test
    public void testPerformUpload() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(exporterWebClient);

        runUpload(ExporterController.MIN_THREADS);
    }

    @Test
    public void testPerformUploadCreateNewVersionTrue() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(exporterWebClient);

        runUpload(ExporterController.MIN_THREADS, true, false);
    }

    @Test
    public void testPerformUploadLatestBusinessObjectDataVersionExists() throws Exception
    {
        runUpload(ExporterController.MIN_THREADS, null, false, false, MockHttpClientOperationsImpl.HOSTNAME_LATEST_BDATA_VERSION_EXISTS, null);
    }

    @Test
    public void testPerformUploadLatestBusinessObjectDataVersionExistsInUploadingState() throws Exception
    {
        try
        {
            runUpload(ExporterController.MIN_THREADS, null, false, false, MockHttpClientOperationsImpl.HOSTNAME_LATEST_BDATA_VERSION_EXISTS_IN_UPLOADING_STATE,
                null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Unable to register business object data because the latest business object data version is detected in UPLOADING state. " +
                    "Please use -force option to invalidate the latest business object version and allow upload to proceed. Business object data {" +
                    "namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                    "businessObjectFormatVersion: 0, businessObjectDataPartitionValue: \"2014-01-31\", businessObjectDataSubPartitionValues: \"\", " +
                    "businessObjectDataVersion: 0}", TEST_NAMESPACE, TEST_BUSINESS_OBJECT_DEFINITION, TEST_BUSINESS_OBJECT_FORMAT_USAGE,
                TEST_BUSINESS_OBJECT_FORMAT_FILE_TYPE), e.getMessage());
        }
    }

    @Test
    public void testPerformUploadMaxThreads() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(exporterWebClient);

        // Calling to run the upload using number of threads just above the upper threshold,
        // that should result in UploaderController adjusting the number of threads to MAX_THREADS value.
        runUpload(ExporterController.MAX_THREADS + 1);
    }

    @Test
    public void testPerformUploadMinThreads() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(exporterWebClient);

        // Calling to run the upload using number of threads just below the low threshold,
        // that should result in UploaderController adjusting the number of threads to MIN_THREADS value.
        runUpload(ExporterController.MIN_THREADS - 1);
    }

    /**
     * TODO: We need the herd web service mocking done and this test case rewritten, so it would fail right at the end of performUpload() method (on the
     * business object data registration step) and triggered the rollbackUpload() to occur.
     */
    @Test(expected = RuntimeException.class)
    public void testPerformUploadRegistrationError() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(exporterWebClient);

        runUpload(ExporterController.MIN_THREADS);

        // Clean up the local directory.
        FileUtils.deleteDirectory(LOCAL_TEMP_PATH_INPUT.toFile());

        // Clean up the destination S3 folder.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto(S3_TEST_PATH_V0);
        if (!s3Service.listDirectory(s3FileTransferRequestParamsDto).isEmpty())
        {
            s3Service.deleteDirectory(s3FileTransferRequestParamsDto);
        }

        runUpload(ExporterController.MIN_THREADS);
    }

    @Test
    public void testPerformUploadTargetS3FolderIsNotEmpty() throws Exception
    {
        // Upload test data files to S3 test path.
        uploadTestDataFilesToS3(S3_TEST_PATH_V0);

        // Try to run the upload task.
        try
        {
            runUpload(ExporterController.MIN_THREADS);
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage().startsWith("The destination S3 folder is not empty."));
        }
    }

    @Test
    public void testPerformUploadWithAttributes() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(exporterWebClient);

        HashMap<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");

        runUpload(ExporterController.MIN_THREADS, attributes);
    }

    @Test
    public void testPerformUploadWithForceFlagEnabled() throws Exception
    {
        runUpload(ExporterController.MIN_THREADS, null, false, true, MockHttpClientOperationsImpl.HOSTNAME_LATEST_BDATA_VERSION_EXISTS_IN_UPLOADING_STATE,
            null);
    }

    @Test
    public void testPerformUploadWithInfoLoggingEnabled() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(exporterWebClient);

        // Get the logger and the current logger level.
        LogLevel origLogLevel = getLogLevel(ExporterController.class);

        // Set logging level to INFO.
        setLogLevel(ExporterController.class, LogLevel.INFO);

        // Run the upload and reset the logging level back to the original value.
        try
        {
            runUpload(ExporterController.MIN_THREADS);
        }
        finally
        {
            setLogLevel(ExporterController.class, origLogLevel);
        }
    }

    @Test(expected = IOException.class)
    public void testPerformUploadWithIoExceptionDuringAddStorageFiles() throws Exception
    {
        runUpload(ExporterController.MIN_THREADS, null, false, false, MockHttpClientOperationsImpl.HOSTNAME_THROW_IO_EXCEPTION_DURING_ADD_STORAGE_FILES, null);
    }

    @Test(expected = IOException.class)
    public void testPerformUploadWithIoExceptionDuringGetStorage() throws Exception
    {
        runUpload(ExporterController.MIN_THREADS, null, false, false, MockHttpClientOperationsImpl.HOSTNAME_THROW_IO_EXCEPTION_DURING_GET_STORAGE, null);
    }

    @Test(expected = IOException.class)
    public void testPerformUploadWithIoExceptionDuringRegisterBusinessObjectData() throws Exception
    {
        runUpload(ExporterController.MIN_THREADS, null, false, false, MockHttpClientOperationsImpl.HOSTNAME_THROW_IO_EXCEPTION_DURING_REGISTER_BDATA, null);
    }

    @Test(expected = IOException.class)
    public void testPerformUploadWithIoExceptionDuringUpdateBusinessObjectDataStatus() throws Exception
    {
        // Turn off logging since this test will log a stack trace as a warning.
        LogLevel originalLogLevel = getLogLevel(ExporterWebClient.class);
        setLogLevel(ExporterWebClient.class, LogLevel.OFF);
        try
        {
            runUpload(ExporterController.MIN_THREADS, null, false, false, MockHttpClientOperationsImpl.HOSTNAME_THROW_IO_EXCEPTION_DURING_UPDATE_BDATA_STATUS,
                null);
        }
        finally
        {
            setLogLevel(ExporterWebClient.class, originalLogLevel);
        }
    }

    @Test
    public void testPerformUploadWithKmsStorageName() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(exporterWebClient);

        runUpload(ExporterController.MIN_THREADS, null, false, false, null, "S3_MANAGED_KMS");
    }

    @Test
    public void testPerformUploadWithLoggerLevelSetToWarn() throws Exception
    {
        LogLevel origLoggerLevel = getLogLevel(ExporterController.class);
        setLogLevel(ExporterController.class, LogLevel.WARN);

        try
        {
            // Upload and register business object data parents.
            uploadAndRegisterTestDataParents(exporterWebClient);

            runUpload(ExporterController.MIN_THREADS);
        }
        finally
        {
            setLogLevel(ExporterController.class, origLoggerLevel);
        }
    }

    @Test
    public void testPerformUploadWithStorageName() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(exporterWebClient);

        runUpload(ExporterController.MIN_THREADS, null, false, false, null, "S3_MANAGED");
    }

    /**
     * Runs a normal upload scenario.
     *
     * @param numOfThreads the maximum number of threads to use for file transfer to S3
     * @param attributes the attributes to be associated with the test data being uploaded
     * @param createNewVersion if not set, only initial version of the business object data is allowed to be created
     * @param force if set, allows upload to proceed when the latest version of the business object data has UPLOADING status by invalidating that version
     * @param hostname optional override of the default web service hostname.
     * @param storageName optional storage name
     */
    protected void runUpload(Integer numOfThreads, HashMap<String, String> attributes, Boolean createNewVersion, Boolean force, String hostname,
        String storageName) throws Exception
    {
        String hostnameToUse = hostname == null ? WEB_SERVICE_HOSTNAME : hostname;

        // Create local data files in LOCAL_TEMP_PATH_INPUT directory
        for (ManifestFile manifestFile : testManifestFiles)
        {
            createLocalFile(LOCAL_TEMP_PATH_INPUT.toString(), manifestFile.getFileName(), FILE_SIZE_1_KB);
        }

        // Create uploader input manifest file in LOCAL_TEMP_PATH_INPUT directory
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        uploaderInputManifestDto.setAttributes(attributes);
        uploaderInputManifestDto.setStorageName(storageName);
        File manifestFile = createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), uploaderInputManifestDto);
        Assert.assertTrue(manifestFile.isFile());

        // Perform the upload.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setLocalPath(LOCAL_TEMP_PATH_INPUT.toString());
        s3FileTransferRequestParamsDto.setMaxThreads(numOfThreads);
    }

    /**
     * Runs a normal upload scenario.
     *
     * @param numOfThreads the maximum number of threads to use for file transfer to S3
     * @param attributes the attributes to be associated with the test data being uploaded
     * @param createNewVersion if not set, only initial version of the business object data is allowed to be created
     * @param force if set, allows upload to proceed when the latest version of the business object data has UPLOADING status by invalidating that version
     */
    protected void runUpload(Integer numOfThreads, HashMap<String, String> attributes, Boolean createNewVersion, Boolean force) throws Exception
    {
        runUpload(numOfThreads, attributes, createNewVersion, force, null, null);
    }

    /**
     * Runs a normal upload scenario without business object data attributes.
     *
     * @param numOfThreads the maximum number of threads to use for file transfer to S3
     * @param createNewVersion if not set, only initial version of the business object data is allowed to be created
     * @param force if set, allows upload to proceed when the latest version of the business object data has UPLOADING status by invalidating that version
     */
    protected void runUpload(Integer numOfThreads, Boolean createNewVersion, Boolean force) throws Exception
    {
        runUpload(numOfThreads, null, createNewVersion, force);
    }

    /**
     * Runs a normal upload scenario with createNewVersion and force flags both set to "false".
     *
     * @param numOfThreads the maximum number of threads to use for file transfer to S3
     * @param attributes the attributes to be associated with the test data being uploaded
     */
    protected void runUpload(Integer numOfThreads, HashMap<String, String> attributes) throws Exception
    {
        runUpload(numOfThreads, attributes, false, false);
    }

    /**
     * Runs a normal upload scenario without business object data attributes and with createNewVersion flag set to False.
     *
     * @param numOfThreads the maximum number of threads to use for file transfer to S3
     */
    protected void runUpload(Integer numOfThreads) throws Exception
    {
        runUpload(numOfThreads, null, Boolean.FALSE);
    }
}
