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

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.service.BusinessObjectFormatService;

@Component
public class RelationalTableRegistrationDaoHelper
{
    /**
     * RDS Format Usage
     */
    public static final String RDS_FORMAT_USAGE = "SRC";

    /**
     * RDS_FORMAT_ATTRIBUTE_TABLE_NAME
     */
    public static final String RDS_FORMAT_ATTRIBUTE_TABLE_NAME = "dbTableName";

    /**
     * The partition key value for business object data without partitioning.
     */
    public static final String NO_PARTITIONING_PARTITION_KEY = "partition";

    /**
     * The partition value for business object data without partitioning.
     */
    public static final String NO_PARTITIONING_PARTITION_VALUE = "none";

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private DataProviderDaoHelper dataProviderDaoHelper;

    @Autowired
    private NamespaceDaoHelper namespaceDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectFormatService businessObjectFormatService;

    /**
     * Create relational table registration in Herd
     *  Includes create business object definition, business object format and business object data
     * @param createRequest relational table registration create request
     * @return business object data
     */
    public BusinessObjectData createRelationalTableRegistration(RelationalTableRegistrationCreateRequest createRequest)
    {
        validateRelationalTableRegistrationCreateRequest(createRequest);

        DataProviderEntity dataProviderEntity = dataProviderDaoHelper.getDataProviderEntity(createRequest.getDataProviderName());
        NamespaceEntity namespaceEntity = namespaceDaoHelper.getNamespaceEntity(createRequest.getNamespace());
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(createRequest.getStorageName());
        Assert.isTrue(storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.RELATIONAL), "Only RELATIONAL storage platform is supported.");

        // Create Business Object Definition
        BusinessObjectDefinitionCreateRequest businessObjectDefinitionCreateRequest = new BusinessObjectDefinitionCreateRequest();
        businessObjectDefinitionCreateRequest.setNamespace(namespaceEntity.getCode());
        businessObjectDefinitionCreateRequest.setDataProviderName(dataProviderEntity.getName());
        businessObjectDefinitionCreateRequest.setBusinessObjectDefinitionName(createRequest.getBusinessObjectDefinitionName());
        businessObjectDefinitionCreateRequest.setDisplayName(createRequest.getBusinessObjectDefinitionDisplayName());
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.createBusinessObjectDefinitionEntity(businessObjectDefinitionCreateRequest);

        // Create Business Object Format
        BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest = new BusinessObjectFormatCreateRequest();
        businessObjectFormatCreateRequest.setNamespace(namespaceEntity.getCode());
        businessObjectFormatCreateRequest.setBusinessObjectDefinitionName(createRequest.getBusinessObjectDefinitionName());
        businessObjectFormatCreateRequest.setBusinessObjectFormatFileType(FileTypeEntity.DB_TABLE_FILE_TYPE);
        businessObjectFormatCreateRequest.setBusinessObjectFormatUsage(RDS_FORMAT_USAGE);
        businessObjectFormatCreateRequest.setPartitionKey(NO_PARTITIONING_PARTITION_KEY);
        businessObjectFormatCreateRequest.setAttributes(Arrays.asList(new Attribute(RDS_FORMAT_ATTRIBUTE_TABLE_NAME, createRequest.getRelationalTableName())));
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(businessObjectFormatCreateRequest);

        BusinessObjectFormatEntity businessObjectFormatEntity =
            businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormat));

        // Create Business Object Data
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = new BusinessObjectDataCreateRequest();
        businessObjectDataCreateRequest.setNamespace(businessObjectDefinitionEntity.getNamespace().getCode());
        businessObjectDataCreateRequest.setBusinessObjectDefinitionName(businessObjectDefinitionEntity.getName());
        businessObjectDataCreateRequest.setBusinessObjectFormatFileType(FileTypeEntity.DB_TABLE_FILE_TYPE);
        businessObjectDataCreateRequest.setBusinessObjectFormatUsage(RDS_FORMAT_USAGE);
        businessObjectDataCreateRequest.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());
        businessObjectDataCreateRequest.setPartitionKey(NO_PARTITIONING_PARTITION_KEY);
        businessObjectDataCreateRequest.setPartitionValue(NO_PARTITIONING_PARTITION_VALUE);

        StorageUnitCreateRequest storageUnitCreateRequest = new StorageUnitCreateRequest();
        storageUnitCreateRequest.setStorageName(storageEntity.getName());
        businessObjectDataCreateRequest.setStorageUnits(Arrays.asList(storageUnitCreateRequest));

        BusinessObjectData businessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(businessObjectDataCreateRequest);

        return businessObjectData;
    }

    /**
     * Validate relational table registration create request
     * @param createRequest relational table registration create request
     */
    public void validateRelationalTableRegistrationCreateRequest(RelationalTableRegistrationCreateRequest createRequest)
    {
        Assert.notNull(createRequest, "A relational table registration create request must be specified.");
        createRequest.setNamespace(alternateKeyHelper.validateStringParameter("namespace", createRequest.getNamespace()));
        createRequest.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", createRequest.getBusinessObjectDefinitionName()));

        createRequest
            .setRelationalTableName(alternateKeyHelper.validateStringParameter("business object definition name", createRequest.getRelationalTableName()));

        createRequest.setDataProviderName(alternateKeyHelper.validateStringParameter("business object data provider", createRequest.getDataProviderName()));
        createRequest.setStorageName(alternateKeyHelper.validateStringParameter("storage name", createRequest.getStorageName()));
    }
}
