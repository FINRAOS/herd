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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.DataProviderDaoTestHelper;
import org.finra.herd.dao.FileTypeDaoTestHelper;
import org.finra.herd.dao.NamespaceDaoTestHelper;
import org.finra.herd.dao.StorageDaoTestHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;

@Component
class RelationalTableRegistrationServiceTestHelper
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private DataProviderDaoTestHelper dataProviderDaoTestHelper;

    @Autowired
    private FileTypeDaoTestHelper fileTypeDaoTestHelper;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    @Autowired
    private StorageDaoTestHelper storageDaoTestHelper;

    /**
     * Creates database entities required for relational table registration unit tests.
     *
     * @param namespace the namespace
     * @param dataProviderName the data provider name
     * @param storageName the storage name
     */
    void createDatabaseEntitiesForRelationalTableRegistrationTesting(String namespace, String dataProviderName, String storageName)
    {
        // Create a namespace.
        namespaceDaoTestHelper.createNamespaceEntity(namespace);

        // Create a data provider entity.
        dataProviderDaoTestHelper.createDataProviderEntity(dataProviderName);

        createDatabaseEntitiesForRelationalTableRegistrationTesting(storageName);
    }

    /**
     * Creates database entities required for relational table registration unit tests.
     *
     * @param storageName the storage name
     */
    void createDatabaseEntitiesForRelationalTableRegistrationTesting(String storageName)
    {
        // Create RELATIONAL_TABLE file type entity.
        fileTypeDaoTestHelper.createFileTypeEntity(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, AbstractServiceTest.FORMAT_FILE_TYPE_DESCRIPTION);

        // Create a RELATIONAL storage with attributes required for relational table registration testing.
        storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.RELATIONAL, getStorageAttributes());
    }

    /**
     * Gets a list of expected schema columns.
     *
     * @return the list of schema columns
     */
    List<SchemaColumn> getExpectedSchemaColumns()
    {
        List<SchemaColumn> expectedSchemaColumns = new ArrayList<>();

        expectedSchemaColumns.add(new SchemaColumn("BUS_OBJCT_DFNTN_ID", "BIGINT", "19", true, null, null));
        expectedSchemaColumns.add(new SchemaColumn("CREAT_USER_ID", "VARCHAR", "255", false, null, null));
        expectedSchemaColumns.add(new SchemaColumn("CREAT_TS", "TIMESTAMP", "23", false, null, null));
        expectedSchemaColumns.add(new SchemaColumn("UPDT_USER_ID", "VARCHAR", "255", false, null, null));
        expectedSchemaColumns.add(new SchemaColumn("UPDT_TS", "TIMESTAMP", "23", false, null, null));
        expectedSchemaColumns.add(new SchemaColumn("DESC_TX", "VARCHAR", "500", false, null, null));
        expectedSchemaColumns.add(new SchemaColumn("DSPLY_NAME_TX", "VARCHAR", "255", false, null, null));
        expectedSchemaColumns.add(new SchemaColumn("NAME_TX", "VARCHAR", "255", false, null, null));
        expectedSchemaColumns.add(new SchemaColumn("DATA_PRVDR_CD", "VARCHAR", "255", true, null, null));
        expectedSchemaColumns.add(new SchemaColumn("DESC_BUS_OBJCT_FRMT_ID", "BIGINT", "19", false, null, null));
        expectedSchemaColumns.add(new SchemaColumn("NAME_SPACE_CD", "VARCHAR", "255", true, null, null));

        return expectedSchemaColumns;
    }

    /**
     * Gets storage attributes required for relational table registration unit tests.
     *
     * @return the list of attributes
     */
    List<Attribute> getStorageAttributes()
    {
        // The storage attributes specify JDBC connection to point to the in-memory database setup as part of DAO mocks.
        // For simplify validation, the attributes are listed alphabetically by attribute name ascending.
        return Arrays.asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.STORAGE_ATTRIBUTE_NAME_JDBC_URL), AbstractServiceTest.JDBC_URL),
            new Attribute(configurationHelper.getProperty(ConfigurationValue.STORAGE_ATTRIBUTE_NAME_JDBC_USER_CREDENTIAL_NAME),
                AbstractServiceTest.EMPTY_STRING),
            new Attribute(configurationHelper.getProperty(ConfigurationValue.STORAGE_ATTRIBUTE_NAME_JDBC_USERNAME), AbstractServiceTest.EMPTY_STRING));
    }
}
