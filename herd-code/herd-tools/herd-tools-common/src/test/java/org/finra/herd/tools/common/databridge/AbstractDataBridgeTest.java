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
package org.finra.herd.tools.common.databridge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.core.Command;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.ManifestFile;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferResultsDto;
import org.finra.herd.model.dto.UploaderInputManifestDto;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.tools.common.config.DataBridgeTestSpringModuleConfig;

/**
 * This is an abstract base class that provides useful methods for DAO test drivers.
 */
@ContextConfiguration(classes = DataBridgeTestSpringModuleConfig.class, inheritLocations = false)
public abstract class AbstractDataBridgeTest extends AbstractCoreTest
{
    private static Logger logger = LoggerFactory.getLogger(AbstractDataBridgeTest.class);

    protected static final String WEB_SERVICE_HOSTNAME = "testWebServiceHostname";

    protected static final Integer WEB_SERVICE_PORT = 80;

    protected static final Integer WEB_SERVICE_HTTPS_PORT = 1234;

    protected static final String WEB_SERVICE_HTTPS_USERNAME = "testHttpsUsername";

    protected static final String WEB_SERVICE_HTTPS_PASSWORD = "testHttpsPassword";

    public static final String HTTP_PROXY_HOST = "testProxyHostname";

    public static final Integer HTTP_PROXY_PORT = 80;

    protected static final String S3_BUCKET_NAME = "testBucket";

    protected static final String S3_ACCESS_KEY = "testAccessKey";

    protected static final String S3_SECRET_KEY = "testSecretKey";

    protected static final String S3_ENDPOINT_US_STANDARD = "s3.amazonaws.com";

    protected static final String RANDOM_SUFFIX = getRandomSuffix();

    protected static final String TEST_NAMESPACE = "APP_A";

    protected static final String TEST_BUSINESS_OBJECT_DEFINITION = "NEW_ORDERS";

    protected static final String TEST_BUSINESS_OBJECT_FORMAT_USAGE = "PRC";

    protected static final String TEST_BUSINESS_OBJECT_FORMAT_FILE_TYPE = "TXT";

    protected static final Integer TEST_BUSINESS_OBJECT_FORMAT_VERSION = 0;

    protected static final String TEST_BUSINESS_OBJECT_FORMAT_PARTITION_KEY = "PROCESS_DATE";

    protected static final String TEST_PARENT_PARTITION_VALUE = "2014-07-09" + RANDOM_SUFFIX;

    protected static final String TEST_PARTITION_VALUE = "2014-07-10" + RANDOM_SUFFIX;

    protected static final String TEST_SUB_PARTITION_VALUE_1 = "2014-07-11" + RANDOM_SUFFIX;

    protected static final String TEST_SUB_PARTITION_VALUE_2 = "2014-07-12" + RANDOM_SUFFIX;

    protected static final String TEST_SUB_PARTITION_VALUE_3 = "2014-07-13" + RANDOM_SUFFIX;

    protected static final String TEST_SUB_PARTITION_VALUE_4 = "2014-07-14" + RANDOM_SUFFIX;

    protected static final List<String> TEST_SUB_PARTITION_VALUES =
        Arrays.asList(TEST_SUB_PARTITION_VALUE_1, TEST_SUB_PARTITION_VALUE_2, TEST_SUB_PARTITION_VALUE_3, TEST_SUB_PARTITION_VALUE_4);

    protected static final Integer TEST_DATA_VERSION_V0 = 0;

    protected static final Integer TEST_DATA_VERSION_V1 = 1;

    protected static final List<String> LOCAL_FILES =
        Arrays.asList("foo1.dat", "Foo2.dat", "FOO3.DAT", "folder/foo3.dat", "folder/foo2.dat", "folder/foo1.dat");

    protected static final String LOCAL_FILE = "foo.dat";

    protected static final List<String> S3_DIRECTORY_MARKERS = Arrays.asList("", "folder");

    protected static final String ATTRIBUTE_NAME_1_MIXED_CASE = "Attribute Name 1";

    protected static final String ATTRIBUTE_VALUE_1 = "Attribute Value 1";

    protected static final String ATTRIBUTE_NAME_2_MIXED_CASE = "Attribute Name 2";

    protected static final String ATTRIBUTE_VALUE_2 = "   Attribute Value 2  ";

    protected static final String ATTRIBUTE_NAME_3_MIXED_CASE = "Attribute Name 3";

    protected static final String BLANK_TEXT = "   \n   \t\t ";

    protected static final String NAMESPACE_CD = "UT_Namespace" + RANDOM_SUFFIX;

    protected static final String STRING_VALUE = "UT_SomeText" + RANDOM_SUFFIX;

    protected static List<ManifestFile> testManifestFiles;

    protected static final String S3_TEST_PARENT_PATH_V0 =
        "app-a/exchange-a/prc/txt/new-orders/frmt-v0/data-v" + TEST_DATA_VERSION_V0 + "/process-date=" + TEST_PARENT_PARTITION_VALUE + "/spk1=" +
            TEST_SUB_PARTITION_VALUE_1 + "/spk2=" + TEST_SUB_PARTITION_VALUE_2 + "/spk3=" + TEST_SUB_PARTITION_VALUE_3 + "/spk4=" + TEST_SUB_PARTITION_VALUE_4;

    protected static final String S3_TEST_PARENT_PATH_V1 =
        "app-a/exchange-a/prc/txt/new-orders/frmt-v0/data-v" + TEST_DATA_VERSION_V1 + "/process-date=" + TEST_PARENT_PARTITION_VALUE + "/spk1=" +
            TEST_SUB_PARTITION_VALUE_1 + "/spk2=" + TEST_SUB_PARTITION_VALUE_2 + "/spk3=" + TEST_SUB_PARTITION_VALUE_3 + "/spk4=" + TEST_SUB_PARTITION_VALUE_4;

    protected static final String S3_TEST_PATH_V0 =
        "app-a/exchange-a/prc/txt/new-orders/frmt-v0/data-v" + TEST_DATA_VERSION_V0 + "/process-date=" + TEST_PARTITION_VALUE + "/spk1=" +
            TEST_SUB_PARTITION_VALUE_1 +
            "/spk2=" + TEST_SUB_PARTITION_VALUE_2 + "/spk3=" + TEST_SUB_PARTITION_VALUE_3 + "/spk4=" + TEST_SUB_PARTITION_VALUE_4;

    protected static final String S3_TEST_PATH_V1 =
        "app-a/exchange-a/prc/txt/new-orders/frmt-v0/data-v" + TEST_DATA_VERSION_V1 + "/process-date=" + TEST_PARTITION_VALUE + "/spk1=" +
            TEST_SUB_PARTITION_VALUE_1 +
            "/spk2=" + TEST_SUB_PARTITION_VALUE_2 + "/spk3=" + TEST_SUB_PARTITION_VALUE_3 + "/spk4=" + TEST_SUB_PARTITION_VALUE_4;

    protected static final String S3_SIMPLE_TEST_PATH = "app-a/exchange-a/prc/txt/new-orders/frmt-v0/data-v0/process-date=2014-01-31";

    /**
     * The counter value to generate ID values unique across multiple threads.
     */
    protected AtomicInteger counter = new AtomicInteger(0);

    protected static final Path LOCAL_TEMP_PATH_INPUT = Paths.get(System.getProperty("java.io.tmpdir"), "herd-databridge-test", "input");

    protected static final Path LOCAL_TEMP_PATH_OUTPUT = Paths.get(System.getProperty("java.io.tmpdir"), "herd-databridge-test", "output");

    @Autowired
    protected ApplicationContext applicationContext;

    @Autowired
    protected BusinessObjectDataHelper businessObjectDataHelper;

    /**
     * Provide easy access to the S3Service for all test methods.
     */
    @Autowired
    protected S3Service s3Service;

    @Autowired
    protected XmlHelper xmlHelper;

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        if (testManifestFiles == null)
        {
            testManifestFiles = getManifestFilesFromFileNames(LOCAL_FILES, FILE_SIZE_1_KB);
        }

        // Create local temp directories.
        LOCAL_TEMP_PATH_INPUT.toFile().mkdirs();
        LOCAL_TEMP_PATH_OUTPUT.toFile().mkdirs();
    }

    /**
     * Gets a list of manifest files from a list of file names.
     *
     * @param fileNames the list of file names.
     * @param fileSizeBytes the file size in bytes for each manifest file.
     *
     * @return the list of manifest files.
     */
    protected List<ManifestFile> getManifestFilesFromFileNames(List<String> fileNames, long fileSizeBytes)
    {
        List<ManifestFile> manifestFiles = new ArrayList<>();
        for (int i = 0; i < fileNames.size(); i++)
        {
            String fileName = fileNames.get(i);
            ManifestFile manifestFile = new ManifestFile();
            manifestFiles.add(manifestFile);
            manifestFile.setFileName(fileName);
            manifestFile.setRowCount((long) i);
            manifestFile.setFileSizeBytes(fileSizeBytes);
        }
        return manifestFiles;
    }

    /**
     * Cleans up the test environment.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the local directory.
        FileUtils.deleteDirectory(LOCAL_TEMP_PATH_INPUT.toFile());
        FileUtils.deleteDirectory(LOCAL_TEMP_PATH_OUTPUT.toFile());

        // Clean up the destination S3 folders.
        cleanupS3();
    }

    /**
     * Returns an S3 file transfer request parameters DTO instance initialized using hard coded test values. This DTO is required for testing and clean up
     * activities.
     *
     * @return the newly created S3 file transfer request parameters DTO
     */
    protected S3FileTransferRequestParamsDto getTestS3FileTransferRequestParamsDto()
    {
        return getTestS3FileTransferRequestParamsDto(null);
    }

    /**
     * Returns an S3 file transfer request parameters DTO instance initialized using hard coded test values. This DTO is required for testing and clean up
     * activities.
     *
     * @param s3KeyPrefix the S3 key prefix
     *
     * @return the newly created S3 file transfer request parameters DTO
     */
    protected S3FileTransferRequestParamsDto getTestS3FileTransferRequestParamsDto(String s3KeyPrefix)
    {
        return S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME).s3KeyPrefix(s3KeyPrefix).s3AccessKey(S3_ACCESS_KEY)
            .s3SecretKey(S3_SECRET_KEY).httpProxyHost(HTTP_PROXY_HOST).httpProxyPort(HTTP_PROXY_PORT).localPath(LOCAL_TEMP_PATH_INPUT.toString()).build();
    }

    /**
     * Runs a data bridge application (i.e. uploader or downloader) with the specified arguments and validates the response against an expected return value. An
     * optional "no logging class" can also be specified.
     *
     * @param dataBridgeApp the Data Bridge application.
     * @param args the application arguments.
     * @param noLoggingClass an optional class that will have logging turned off.
     * @param expectedReturnValue the expected application return value.
     *
     * @throws Exception if any errors were found during the execution of the application.
     */
    protected void runDataBridgeAndCheckReturnValue(DataBridgeApp dataBridgeApp, String[] args, Class<?> noLoggingClass,
        DataBridgeApp.ReturnValue expectedReturnValue) throws Exception
    {
        runDataBridgeAndCheckReturnValue(dataBridgeApp, args, noLoggingClass, expectedReturnValue, null);
    }

    /**
     * Runs a data bridge application (i.e. uploader or downloader) with the specified arguments and verifies that an expected exception will be thrown. An
     * optional "no logging class" can also be specified.
     *
     * @param dataBridgeApp the Data Bridge application.
     * @param args the application arguments.
     * @param noLoggingClass an optional class that will have logging turned off.
     * @param expectedException an instance of an expected exception that should be thrown. If this is an instance of HttpErrorResponseException, then the
     * response status will also be compared.
     *
     * @throws Exception if any errors were found during the execution of the application.
     */
    protected void runDataBridgeAndCheckReturnValue(DataBridgeApp dataBridgeApp, String[] args, Class<?> noLoggingClass, Object expectedException)
        throws Exception
    {
        runDataBridgeAndCheckReturnValue(dataBridgeApp, args, noLoggingClass, null, expectedException);
    }

    /**
     * Runs a data bridge application (i.e. uploader or downloader) with the specified arguments and validates the response against an expected return value. An
     * optional "no logging class" can also be specified.
     *
     * @param dataBridgeApp the Data Bridge application.
     * @param args the application arguments.
     * @param noLoggingClass an optional class that will have logging turned off.
     * @param expectedReturnValue the expected application return value or null if an exception is expected.
     * @param expectedException an instance of an expected exception that should be thrown or null if no exception is expected. If this is null, then an
     * expected return value should be populated. If this is an instance of HttpErrorResponseException, then the response status will also be compared.
     *
     * @throws Exception if any errors were found during the execution of the application.
     */
    private void runDataBridgeAndCheckReturnValue(final DataBridgeApp dataBridgeApp, final String[] args, Class<?> noLoggingClass,
        final DataBridgeApp.ReturnValue expectedReturnValue, final Object expectedException) throws Exception
    {
        try
        {
            executeWithoutLogging(noLoggingClass, new Command()
            {
                @Override
                public void execute() throws Exception
                {
                    DataBridgeApp.ReturnValue returnValue = dataBridgeApp.go(args);
                    if (expectedException != null)
                    {
                        fail("Expected exception of class " + expectedException.getClass().getName() + " that was not thrown.");
                    }
                    else
                    {
                        assertEquals(expectedReturnValue, returnValue);
                        assertEquals(expectedReturnValue.getReturnCode(), returnValue.getReturnCode());
                    }
                }
            });
        }
        catch (Exception ex)
        {
            if (expectedException != null)
            {
                if (!(ex.getClass().equals(expectedException.getClass())))
                {
                    logger.error("Error running Data Bridge.", ex);
                    fail("Expected exception with class " + expectedException.getClass().getName() + ", but got an exception with class " +
                        ex.getClass().getName());
                }
                if (ex instanceof HttpErrorResponseException)
                {
                    // This will ensure the returned status code matches what we are expecting.
                    HttpErrorResponseException httpErrorResponseException = (HttpErrorResponseException) ex;
                    HttpErrorResponseException expectedHttpErrorResponseException = (HttpErrorResponseException) expectedException;
                    assertTrue("Expecting HTTP response status of " + expectedHttpErrorResponseException.getStatusCode() + ", but got " +
                        httpErrorResponseException.getStatusCode(), expectedException.equals(httpErrorResponseException));
                }
            }
            else
            {
                // Throw the original exception, since we are not expecting any exception.
                throw ex;
            }
        }
    }

    /**
     * Validates actualBusinessObjectData contents against specified arguments and expected (hard coded) test values.
     *
     * @param expectedDataVersion the expected business object data version
     * @param actualBusinessObjectData the BusinessObjectData object instance to be validated
     */
    protected void assertBusinessObjectData(Integer expectedDataVersion, BusinessObjectData actualBusinessObjectData)
    {
        assertBusinessObjectData(expectedDataVersion, getTestAttributes(), getTestBusinessObjectDataParents(), actualBusinessObjectData);
    }

    /**
     * Validates actualBusinessObjectData contents against specified arguments and expected (hard coded) test values.
     *
     * @param expectedDataVersion the expected business object data version
     * @param expectedAttributes the expected attributes
     * @param expectedParents the expected business object data parents
     * @param actualBusinessObjectData the BusinessObjectData object instance to be validated
     */
    protected void assertBusinessObjectData(Integer expectedDataVersion, List<Attribute> expectedAttributes, List<BusinessObjectDataKey> expectedParents,
        BusinessObjectData actualBusinessObjectData)
    {
        assertNotNull(actualBusinessObjectData);
        assertEquals(TEST_BUSINESS_OBJECT_DEFINITION, actualBusinessObjectData.getBusinessObjectDefinitionName());
        assertEquals(TEST_BUSINESS_OBJECT_FORMAT_USAGE, actualBusinessObjectData.getBusinessObjectFormatUsage());
        assertEquals(TEST_BUSINESS_OBJECT_FORMAT_FILE_TYPE, actualBusinessObjectData.getBusinessObjectFormatFileType());
        assertEquals(TEST_BUSINESS_OBJECT_FORMAT_VERSION.intValue(), actualBusinessObjectData.getBusinessObjectFormatVersion());
        assertEquals(TEST_BUSINESS_OBJECT_FORMAT_PARTITION_KEY, actualBusinessObjectData.getPartitionKey());
        assertEquals(TEST_PARTITION_VALUE, actualBusinessObjectData.getPartitionValue());
        assertEquals(expectedDataVersion.intValue(), actualBusinessObjectData.getVersion());
        assertEquals(1, actualBusinessObjectData.getStorageUnits().size());
        assertEquals(StorageEntity.MANAGED_STORAGE, actualBusinessObjectData.getStorageUnits().get(0).getStorage().getName());
        assertEquals(testManifestFiles.size(), actualBusinessObjectData.getStorageUnits().get(0).getStorageFiles().size());
        assertEquals(expectedAttributes, actualBusinessObjectData.getAttributes());
        assertEquals(expectedParents, actualBusinessObjectData.getBusinessObjectDataParents());
    }

    /**
     * Returns a next ID value to be unique across multiple threads.
     *
     * @return the next ID value
     */
    protected int getNextUniqueIndex()
    {
        return counter.getAndIncrement();
    }

    /**
     * Cleans up the destination S3 key prefixes used by the uploader unit tests.
     */
    protected void cleanupS3()
    {
        // Delete the test business object data versions.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        for (String s3KeyPrefix : new String[] {S3_TEST_PARENT_PATH_V0, S3_TEST_PARENT_PATH_V1, S3_TEST_PATH_V0, S3_TEST_PATH_V1, S3_SIMPLE_TEST_PATH})
        {
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            if (!s3Service.listDirectory(s3FileTransferRequestParamsDto).isEmpty())
            {
                s3Service.deleteDirectory(s3FileTransferRequestParamsDto);
            }
        }
    }

    /**
     * Creates local test data files in LOCAL_TEMP_PATH_INPUT directory.
     *
     * @param localPath the local path relative to which the test data files will be created
     * @param manifestFiles the list of the test data files
     */
    protected void createTestDataFiles(Path localPath, List<ManifestFile> manifestFiles) throws Exception
    {
        // Create local test files.
        for (ManifestFile manifestFile : manifestFiles)
        {
            createLocalFile(localPath.toString(), manifestFile.getFileName(), manifestFile.getFileSizeBytes());
        }
    }

    /**
     * Creates and uploads to S3 test data files.
     *
     * @param s3KeyPrefix the destination S3 key prefix
     */
    protected void uploadTestDataFilesToS3(String s3KeyPrefix) throws Exception
    {
        uploadTestDataFilesToS3(s3KeyPrefix, testManifestFiles);
    }

    /**
     * Creates locally and uploads to S3 the specified list of test data files.
     *
     * @param s3KeyPrefix the destination S3 key prefix
     * @param manifestFiles the list of test data files to be created and uploaded to S3
     */
    protected void uploadTestDataFilesToS3(String s3KeyPrefix, List<ManifestFile> manifestFiles) throws Exception
    {
        uploadTestDataFilesToS3(s3KeyPrefix, manifestFiles, new ArrayList<String>());
    }

    /**
     * Creates locally specified list of files and uploads them to the test S3 bucket. This method also creates 0 byte S3 directory markers relative to the s3
     * key prefix.
     *
     * @param s3KeyPrefix the destination S3 key prefix
     * @param manifestFiles the list of test data files to be created and uploaded to S3
     * @param directoryPaths the list of directory paths to be created in S3 relative to the S3 key prefix
     * <p/>
     * TODO: This method is basically a copy of prepareTestS3Files() from BusinessObjectDataServiceCreateBusinessObjectDataTest.java, so they both should be
     * replaced by a common helper method in AbstractDaoTest.java (a common parent class).
     */
    protected void uploadTestDataFilesToS3(String s3KeyPrefix, List<ManifestFile> manifestFiles, List<String> directoryPaths) throws Exception
    {
        // Create local test data files.
        createTestDataFiles(LOCAL_TEMP_PATH_INPUT, manifestFiles);

        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        String s3KeyPrefixWithTrailingSlash = s3KeyPrefix + "/";

        // Upload test file to S3.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefixWithTrailingSlash);
        s3FileTransferRequestParamsDto.setLocalPath(LOCAL_TEMP_PATH_INPUT.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Service.uploadDirectory(s3FileTransferRequestParamsDto);

        // Validate the transfer result.
        assertEquals(Long.valueOf(manifestFiles.size()), results.getTotalFilesTransferred());

        // Create 0 byte S3 directory markers.
        for (String directoryPath : directoryPaths)
        {
            // Create 0 byte directory marker.
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix + "/" + directoryPath);
            s3Service.createDirectory(s3FileTransferRequestParamsDto);
        }

        // Restore the S3 key prefix value.
        s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefixWithTrailingSlash);

        // Validate the uploaded S3 files and created directory markers, if any.
        assertEquals(manifestFiles.size() + directoryPaths.size(), s3Service.listDirectory(s3FileTransferRequestParamsDto).size());
    }

    /**
     * Serializes provided manifest instance as JSON output, written to a file in the specified directory.
     *
     * @param baseDir the local parent directory path, relative to which the manifest file should be created
     * @param manifest the manifest instance to serialize
     *
     * @return the resulting file
     */
    protected File createManifestFile(String baseDir, Object manifest) throws IOException
    {
        // Create result file object
        Path resultFilePath = Paths.get(baseDir, String.format("manifest-%d.json", getNextUniqueIndex()));
        File resultFile = new File(resultFilePath.toString());

        // Convert Java object to JSON format
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(resultFile, manifest);

        return resultFile;
    }

    /**
     * Returns an instance of the uploader input manifest object initialized per hard coded test values.
     *
     * @return the resulting UploaderManifest instance
     */
    protected UploaderInputManifestDto getTestUploaderInputManifestDto()
    {
        return getTestUploaderInputManifestDto(TEST_PARTITION_VALUE, TEST_SUB_PARTITION_VALUES, true);
    }

    /**
     * Returns an instance of the uploader input manifest object initialized per hard coded test values.
     *
     * @return the resulting UploaderManifest instance
     */
    protected UploaderInputManifestDto getTestUploaderInputManifestDto(String partitionValue, List<String> subPartitionValues, boolean includeParents)
    {
        UploaderInputManifestDto manifest = new UploaderInputManifestDto();

        manifest.setNamespace(TEST_NAMESPACE);
        manifest.setBusinessObjectDefinitionName(TEST_BUSINESS_OBJECT_DEFINITION);
        manifest.setBusinessObjectFormatUsage(TEST_BUSINESS_OBJECT_FORMAT_USAGE);
        manifest.setBusinessObjectFormatFileType(TEST_BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        manifest.setBusinessObjectFormatVersion(TEST_BUSINESS_OBJECT_FORMAT_VERSION.toString());
        manifest.setPartitionKey(TEST_BUSINESS_OBJECT_FORMAT_PARTITION_KEY);
        manifest.setPartitionValue(partitionValue);
        manifest.setSubPartitionValues(subPartitionValues);
        manifest.setManifestFiles(testManifestFiles);

        // Add attributes to the uploader manifest.
        HashMap<String, String> attributes = new HashMap<>();
        manifest.setAttributes(attributes);
        for (Attribute attribute : getTestAttributes())
        {
            attributes.put(attribute.getName(), attribute.getValue());
        }

        // Add business object data parents.
        if (includeParents)
        {
            manifest.setBusinessObjectDataParents(getTestBusinessObjectDataParents());
        }

        return manifest;
    }

    /**
     * Returns a list of business object data attributes created using hard coded test values.
     *
     * @return the newly created list of business object data attributes
     */
    protected List<Attribute> getTestAttributes()
    {
        List<Attribute> attributes = new ArrayList<>();

        Attribute attribute1 = new Attribute();
        attributes.add(attribute1);
        attribute1.setName(ATTRIBUTE_NAME_1_MIXED_CASE);
        attribute1.setValue(ATTRIBUTE_VALUE_1);

        Attribute attribute2 = new Attribute();
        attributes.add(attribute2);
        attribute2.setName(ATTRIBUTE_NAME_2_MIXED_CASE);
        attribute2.setValue(ATTRIBUTE_VALUE_2);

        return attributes;
    }

    /**
     * Returns a list of business object data parents created using hard coded test values.
     *
     * @return the newly created list of business object data parents.
     */
    protected List<BusinessObjectDataKey> getTestBusinessObjectDataParents()
    {
        List<BusinessObjectDataKey> businessObjectDataparents = new ArrayList<>();

        BusinessObjectDataKey parent1 = new BusinessObjectDataKey();
        businessObjectDataparents.add(parent1);
        parent1.setNamespace(TEST_NAMESPACE);
        parent1.setBusinessObjectDefinitionName(TEST_BUSINESS_OBJECT_DEFINITION);
        parent1.setBusinessObjectFormatUsage(TEST_BUSINESS_OBJECT_FORMAT_USAGE);
        parent1.setBusinessObjectFormatFileType(TEST_BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        parent1.setBusinessObjectFormatVersion(TEST_BUSINESS_OBJECT_FORMAT_VERSION);
        parent1.setPartitionValue(TEST_PARENT_PARTITION_VALUE);
        parent1.setSubPartitionValues(TEST_SUB_PARTITION_VALUES);
        parent1.setBusinessObjectDataVersion(TEST_DATA_VERSION_V0);

        BusinessObjectDataKey parent2 = new BusinessObjectDataKey();
        businessObjectDataparents.add(parent2);
        parent2.setNamespace(TEST_NAMESPACE);
        parent2.setBusinessObjectDefinitionName(TEST_BUSINESS_OBJECT_DEFINITION);
        parent2.setBusinessObjectFormatUsage(TEST_BUSINESS_OBJECT_FORMAT_USAGE);
        parent2.setBusinessObjectFormatFileType(TEST_BUSINESS_OBJECT_FORMAT_FILE_TYPE);
        parent2.setBusinessObjectFormatVersion(TEST_BUSINESS_OBJECT_FORMAT_VERSION);
        parent2.setPartitionValue(TEST_PARENT_PARTITION_VALUE);
        parent2.setSubPartitionValues(TEST_SUB_PARTITION_VALUES);
        parent2.setBusinessObjectDataVersion(TEST_DATA_VERSION_V1);

        return businessObjectDataparents;
    }

    /**
     * Uploads and registers a version if of the test business object data that will be used as a parent.
     *
     * @param dataBridgeWebClient the databridge web client instance
     */
    protected void uploadAndRegisterTestDataParents(DataBridgeWebClient dataBridgeWebClient) throws Exception
    {
        uploadAndRegisterTestDataParent(S3_TEST_PARENT_PATH_V0, dataBridgeWebClient);
        uploadAndRegisterTestDataParent(S3_TEST_PARENT_PATH_V1, dataBridgeWebClient);
    }

    /**
     * Uploads and registers a version if of the test business object data that will be used as a parent.
     *
     * @param s3KeyPrefix the destination S3 key prefix that must comply with the S3 naming conventions including the expected data version value
     * @param dataBridgeWebClient the databridge web client instance
     */
    protected void uploadAndRegisterTestDataParent(String s3KeyPrefix, DataBridgeWebClient dataBridgeWebClient) throws Exception
    {
        uploadTestDataFilesToS3(s3KeyPrefix, testManifestFiles, new ArrayList<String>());
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto(TEST_PARENT_PARTITION_VALUE, TEST_SUB_PARTITION_VALUES, false);
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix + "/");
        BusinessObjectData businessObjectData =
            dataBridgeWebClient.preRegisterBusinessObjectData(uploaderInputManifestDto, StorageEntity.MANAGED_STORAGE, true);
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectData);
        dataBridgeWebClient.addStorageFiles(businessObjectDataKey, uploaderInputManifestDto, s3FileTransferRequestParamsDto, StorageEntity.MANAGED_STORAGE);
        dataBridgeWebClient.updateBusinessObjectDataStatus(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID);
        // Clean up the local input directory used for the test data files upload.
        FileUtils.cleanDirectory(LOCAL_TEMP_PATH_INPUT.toFile());
    }
}
