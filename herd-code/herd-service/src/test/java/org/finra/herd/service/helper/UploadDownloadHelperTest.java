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

import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME;
import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME_2;
import static org.finra.herd.dao.AbstractDaoTest.DATA_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE_2;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE_2;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.LOCAL_FILE;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE_2;
import static org.finra.herd.dao.AbstractDaoTest.PARTITION_VALUE;
import static org.finra.herd.dao.AbstractDaoTest.PARTITION_VALUE_2;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_NAME;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_NAME_2;
import static org.finra.herd.dao.AbstractDaoTest.SUBPARTITION_VALUES;
import static org.finra.herd.dao.AbstractDaoTest.SUBPARTITION_VALUES_2;
import static org.finra.herd.dao.AbstractDaoTest.TEST_S3_KEY_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDataStorageFileKey;
import org.finra.herd.model.api.xml.DownloadBusinessObjectDataStorageFileSingleInitiationRequest;

public class UploadDownloadHelperTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @InjectMocks
    private UploadDownloadHelper uploadDownloadHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testValidateAndTrimDownloadBusinessObjectDataStorageFileSingleInitiationRequest()
    {
        // Create a business object data storage file key.
        BusinessObjectDataStorageFileKey businessObjectDataStorageFileKey =
            new BusinessObjectDataStorageFileKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE + "    ");

        // Create a business object data storage file single initiation request.
        DownloadBusinessObjectDataStorageFileSingleInitiationRequest downloadBusinessObjectDataStorageFileSingleInitiationRequest =
            new DownloadBusinessObjectDataStorageFileSingleInitiationRequest(businessObjectDataStorageFileKey);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE)).thenReturn(NAMESPACE_2);
        when(alternateKeyHelper.validateStringParameter("business object definition name", BDEF_NAME)).thenReturn(BDEF_NAME_2);
        when(alternateKeyHelper.validateStringParameter("business object format usage", FORMAT_USAGE_CODE)).thenReturn(FORMAT_USAGE_CODE_2);
        when(alternateKeyHelper.validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE)).thenReturn(FORMAT_FILE_TYPE_CODE_2);
        when(alternateKeyHelper.validateStringParameter("partition value", PARTITION_VALUE)).thenReturn(PARTITION_VALUE_2);
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            when(alternateKeyHelper.validateStringParameter("sub partition value", SUBPARTITION_VALUES.get(i))).thenReturn(SUBPARTITION_VALUES_2.get(i));
        }
        when(alternateKeyHelper.validateStringParameter("storage name", STORAGE_NAME)).thenReturn(STORAGE_NAME_2);

        // Call the method under test.
        uploadDownloadHelper
            .validateAndTrimDownloadBusinessObjectDataStorageFileSingleInitiationRequest(downloadBusinessObjectDataStorageFileSingleInitiationRequest);

        // Validate the results.
        assertEquals(new DownloadBusinessObjectDataStorageFileSingleInitiationRequest(
                new BusinessObjectDataStorageFileKey(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION, PARTITION_VALUE_2,
                    SUBPARTITION_VALUES_2, DATA_VERSION, STORAGE_NAME_2, TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE)),
            downloadBusinessObjectDataStorageFileSingleInitiationRequest);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("business object definition name", BDEF_NAME);
        verify(alternateKeyHelper).validateStringParameter("business object format usage", FORMAT_USAGE_CODE);
        verify(alternateKeyHelper).validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE);
        verify(alternateKeyHelper).validateStringParameter("partition value", PARTITION_VALUE);
        for (String subPartitionValue : SUBPARTITION_VALUES)
        {
            verify(alternateKeyHelper).validateStringParameter("sub partition value", subPartitionValue);
        }
        verify(alternateKeyHelper).validateStringParameter("storage name", STORAGE_NAME);
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimDownloadBusinessObjectDataStorageFileSingleInitiationRequestIsNull()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A download business object data storage file single initiation request must be specified.");

        // Call the method under test.
        uploadDownloadHelper.validateAndTrimDownloadBusinessObjectDataStorageFileSingleInitiationRequest(null);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimBusinessObjectStorageFileKey()
    {
        // Create a business object data storage file key.
        BusinessObjectDataStorageFileKey businessObjectDataStorageFileKey =
            new BusinessObjectDataStorageFileKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME, TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE + "    ");

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE)).thenReturn(NAMESPACE_2);
        when(alternateKeyHelper.validateStringParameter("business object definition name", BDEF_NAME)).thenReturn(BDEF_NAME_2);
        when(alternateKeyHelper.validateStringParameter("business object format usage", FORMAT_USAGE_CODE)).thenReturn(FORMAT_USAGE_CODE_2);
        when(alternateKeyHelper.validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE)).thenReturn(FORMAT_FILE_TYPE_CODE_2);
        when(alternateKeyHelper.validateStringParameter("partition value", PARTITION_VALUE)).thenReturn(PARTITION_VALUE_2);
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            when(alternateKeyHelper.validateStringParameter("sub partition value", SUBPARTITION_VALUES.get(i))).thenReturn(SUBPARTITION_VALUES_2.get(i));
        }
        when(alternateKeyHelper.validateStringParameter("storage name", STORAGE_NAME)).thenReturn(STORAGE_NAME_2);

        // Call the method under test.
        uploadDownloadHelper.validateAndTrimBusinessObjectDataStorageFileKey(businessObjectDataStorageFileKey);

        // Validate the results.
        assertEquals(
            new BusinessObjectDataStorageFileKey(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES_2, DATA_VERSION, STORAGE_NAME_2, TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE), businessObjectDataStorageFileKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("business object definition name", BDEF_NAME);
        verify(alternateKeyHelper).validateStringParameter("business object format usage", FORMAT_USAGE_CODE);
        verify(alternateKeyHelper).validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE);
        verify(alternateKeyHelper).validateStringParameter("partition value", PARTITION_VALUE);
        for (String subPartitionValue : SUBPARTITION_VALUES)
        {
            verify(alternateKeyHelper).validateStringParameter("sub partition value", subPartitionValue);
        }
        verify(alternateKeyHelper).validateStringParameter("storage name", STORAGE_NAME);
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimBusinessObjectStorageFileKeyIsNull()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A business object data storage file key must be specified.");

        // Call the method under test.
        uploadDownloadHelper.validateAndTrimBusinessObjectDataStorageFileKey(null);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }
}
