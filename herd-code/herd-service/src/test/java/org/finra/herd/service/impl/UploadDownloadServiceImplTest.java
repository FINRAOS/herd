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
package org.finra.herd.service.impl;

import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import javax.persistence.OptimisticLockException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.StsDao;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.UploadDownloadHelperService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.AttributeHelper;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

/**
 * This class tests functionality within the upload download service implementation.
 */
public class UploadDownloadServiceImplTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private AttributeHelper attributeHelper;

    @Mock
    private AwsHelper awsHelper;

    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Mock
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Mock
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Mock
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Mock
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private S3Dao s3Dao;

    @Mock
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Mock
    private StorageDaoHelper storageDaoHelper;

    @Mock
    private StorageHelper storageHelper;

    @Mock
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Mock
    private StsDao stsDao;

    @Mock
    private UploadDownloadHelperService uploadDownloadHelperService;

    @InjectMocks
    private UploadDownloadServiceImpl uploadDownloadServiceImpl;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPerformCompleteUploadSingleMessageImplOptimisticLockException()
    {
        // Create an object key.
        String objectKey = STRING_VALUE;

        // Mock the external calls.
        doThrow(new OptimisticLockException(ERROR_MESSAGE)).when(uploadDownloadHelperService).prepareForFileMove(eq(objectKey), any());
        when(jsonHelper.objectToJson(any())).thenReturn(JSON_STRING);

        // Call the method being tested.
        UploadDownloadServiceImpl.CompleteUploadSingleMessageResult response = uploadDownloadServiceImpl.performCompleteUploadSingleMessageImpl(objectKey);

        // Verify the external calls.
        verify(uploadDownloadHelperService).prepareForFileMove(eq(objectKey), any());
        verify(jsonHelper, times(2)).objectToJson(any());
        verifyNoMoreInteractions(alternateKeyHelper, attributeHelper, awsHelper, businessObjectDataDaoHelper, businessObjectDataHelper,
            businessObjectDefinitionDao, businessObjectDefinitionDaoHelper, businessObjectDefinitionHelper, businessObjectFormatDaoHelper,
            businessObjectFormatHelper, configurationHelper, jsonHelper, s3Dao, s3KeyPrefixHelper, storageDaoHelper, storageHelper, storageUnitDaoHelper,
            stsDao, uploadDownloadHelperService);

        // Validate the returned object.
        assertNull(response);
    }
}
