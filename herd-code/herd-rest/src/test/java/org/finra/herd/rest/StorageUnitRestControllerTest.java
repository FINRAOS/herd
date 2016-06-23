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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Arrays;

import com.google.common.base.Objects;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageUnitDownloadCredential;
import org.finra.herd.model.api.xml.StorageUnitUploadCredential;
import org.finra.herd.service.StorageUnitService;

public class StorageUnitRestControllerTest
{
    @InjectMocks
    private StorageUnitRestController storageUnitRestController;

    @Mock
    private StorageUnitService storageUnitService;

    @Before
    public void before()
    {
        initMocks(this);
    }

    @Test
    public void getStorageUnitUploadCredential()
    {
        String namespace = "namespace";
        String businessObjectDefinitionName = "businessObjectDefinitionName";
        String businessObjectFormatUsage = "businessObjectFormatUsage";
        String businessObjectFormatFileType = "businessObjectFormatFileType";
        Integer businessObjectFormatVersion = 1234;
        String partitionValue = "partitionValue";
        Integer businessObjectDataVersion = 2345;
        String storageName = "storageName";
        DelimitedFieldValues subPartitionValues = new DelimitedFieldValues();
        subPartitionValues.setValues(Arrays.asList("a", "b", "c", "d"));

        StorageUnitUploadCredential expectedResult = new StorageUnitUploadCredential();

        when(storageUnitService.getStorageUnitUploadCredential(any(), any(), any())).thenReturn(expectedResult);

        StorageUnitUploadCredential actualResult = storageUnitRestController
            .getStorageUnitUploadCredential(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, businessObjectDataVersion, storageName, subPartitionValues);

        verify(storageUnitService).getStorageUnitUploadCredential(
            businessObjectDataKeyEq(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, businessObjectDataVersion, subPartitionValues), isNull(Boolean.class), eq(storageName));
        verifyNoMoreInteractions(storageUnitService);

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void getStorageUnitDownloadCredential()
    {
        String namespace = "namespace";
        String businessObjectDefinitionName = "businessObjectDefinitionName";
        String businessObjectFormatUsage = "businessObjectFormatUsage";
        String businessObjectFormatFileType = "businessObjectFormatFileType";
        Integer businessObjectFormatVersion = 1234;
        String partitionValue = "partitionValue";
        Integer businessObjectDataVersion = 2345;
        String storageName = "storageName";
        DelimitedFieldValues subPartitionValues = new DelimitedFieldValues();
        subPartitionValues.setValues(Arrays.asList("a", "b", "c", "d"));

        StorageUnitDownloadCredential expectedResult = new StorageUnitDownloadCredential();

        when(storageUnitService.getStorageUnitDownloadCredential(any(), any())).thenReturn(expectedResult);

        StorageUnitDownloadCredential actualResult = storageUnitRestController
            .getStorageUnitDownloadCredential(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, businessObjectDataVersion, storageName, subPartitionValues);

        verify(storageUnitService).getStorageUnitDownloadCredential(
            businessObjectDataKeyEq(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, businessObjectDataVersion, subPartitionValues), eq(storageName));
        verifyNoMoreInteractions(storageUnitService);

        assertEquals(expectedResult, actualResult);
    }

    /**
     * Returns an argument matcher which matches a BusinessObjectDataKey
     *
     * @param namespace The namespace
     * @param businessObjectDefinitionName The business object definition name
     * @param businessObjectFormatUsage The business object format usage
     * @param businessObjectFormatFileType The business obejct format file type
     * @param businessObjectFormatVersion The business object format version
     * @param partitionValue The partition value
     * @param businessObjectDataVersion The business object data version
     * @param subPartitionValues The sub-partition values
     *
     * @return The argument matcher
     */
    private BusinessObjectDataKey businessObjectDataKeyEq(String namespace, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue, Integer businessObjectDataVersion,
        DelimitedFieldValues subPartitionValues)
    {
        return argThat(new ArgumentMatcher<BusinessObjectDataKey>()
        {
            @Override
            public boolean matches(Object argument)
            {
                BusinessObjectDataKey businessObjectDataKey = (BusinessObjectDataKey) argument;

                return Objects.equal(namespace, businessObjectDataKey.getNamespace()) &&
                    Objects.equal(businessObjectDefinitionName, businessObjectDataKey.getBusinessObjectDefinitionName()) &&
                    Objects.equal(businessObjectFormatUsage, businessObjectDataKey.getBusinessObjectFormatUsage()) &&
                    Objects.equal(businessObjectFormatFileType, businessObjectDataKey.getBusinessObjectFormatFileType()) &&
                    Objects.equal(businessObjectFormatVersion, businessObjectDataKey.getBusinessObjectFormatVersion()) &&
                    Objects.equal(partitionValue, businessObjectDataKey.getPartitionValue()) &&
                    Objects.equal(businessObjectDataVersion, businessObjectDataKey.getBusinessObjectDataVersion()) &&
                    Objects.equal(subPartitionValues.getValues(), businessObjectDataKey.getSubPartitionValues());
            }
        });
    }
}
