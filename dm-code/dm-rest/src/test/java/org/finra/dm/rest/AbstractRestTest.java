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
package org.finra.dm.rest;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.web.WebAppConfiguration;

import org.finra.dm.rest.config.RestTestSpringModuleConfig;
import org.finra.dm.ui.AbstractUiTest;

/**
 * This is an abstract base class that provides useful methods for REST test drivers.
 */
@ContextConfiguration(classes = RestTestSpringModuleConfig.class, inheritLocations = false)
@WebAppConfiguration
public abstract class AbstractRestTest extends AbstractUiTest
{
    @Autowired
    protected DmRestController dmRestController;

    @Autowired
    protected StoragePlatformRestController storagePlatformRestController;

    @Autowired
    protected StorageRestController storageRestController;

    @Autowired
    protected NamespaceRestController namespaceRestController;

    @Autowired
    protected BusinessObjectDataRestController businessObjectDataRestController;

    @Autowired
    protected BusinessObjectDataAttributeRestController businessObjectDataAttributeRestController;

    @Autowired
    protected BusinessObjectDataStatusRestController businessObjectDataStatusRestController;

    @Autowired
    protected BusinessObjectDataNotificationRegistrationRestController businessObjectDataNotificationRegistrationRestController;

    @Autowired
    protected JobDefinitionRestController jobDefinitionRestController;

    @Autowired
    protected JobRestController jobRestController;

    @Autowired
    protected BusinessObjectDefinitionRestController businessObjectDefinitionRestController;

    @Autowired
    protected PartitionKeyGroupRestController partitionKeyGroupRestController;

    @Autowired
    protected ExpectedPartitionValueRestController expectedPartitionValueRestController;

    @Autowired
    protected FileTypeRestController fileTypeRestController;

    @Autowired
    protected BusinessObjectFormatRestController businessObjectFormatRestController;

    @Autowired
    protected CustomDdlRestController customDdlRestController;

    @Autowired
    protected EmrRestController emrRestController;

    @Autowired
    protected EmrClusterDefinitionRestController emrClusterDefinitionRestController;

    @Autowired
    protected BusinessObjectDataStorageFileRestController businessObjectDataStorageFileRestController;

    @Autowired
    protected UploadDownloadRestController uploadDownloadRestController;

    @Autowired
    protected SystemJobRestController systemJobRestController;

    /**
     * Returns a DelimitedFieldValues instance initiated with the list values.
     *
     * @param list the list of string values
     *
     * @return the newly created DelimitedFieldValues instance
     */
    protected DelimitedFieldValues getDelimitedFieldValues(List<String> list)
    {
        DelimitedFieldValues delimitedFieldValues = new DelimitedFieldValues();

        delimitedFieldValues.setValues(list);

        return delimitedFieldValues;
    }
}
