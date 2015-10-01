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
package org.finra.dm.service.helper;

import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.CustomDdlEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.dm.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.dm.model.api.xml.BusinessObjectFormatDdlRequest;

/**
 * Base abstract class for DDL generation. All DDL generators will extend this class.
 */
public abstract class DdlGenerator
{
    public static final String TABLE_NAME_CUSTOM_DDL_TOKEN = "${table.name}";

    public static final String NON_PARTITIONED_TABLE_LOCATION_CUSTOM_DDL_TOKEN = "${non-partitioned.table.location}";

    /**
     * Gets the supported DDL output format that this generator can support.
     *
     * @return BusinessObjectDataDdlOutputFormatEnum supported output format.
     */
    public abstract BusinessObjectDataDdlOutputFormatEnum getDdlOutputFormat();

    /**
     * This method generates the create table DDL as per specified business object format DDL request.
     *
     * @param request the business object format DDL request
     * @param businessObjectFormatEntity the business object format entity
     * @param customDdlEntity the optional custom DDL entity
     *
     * @return the generated DDL
     */
    public abstract String generateCreateTableDdl(BusinessObjectFormatDdlRequest request, BusinessObjectFormatEntity businessObjectFormatEntity,
        CustomDdlEntity customDdlEntity);

    /**
     * This method generates the create table DDL as per specified business object data DDL request.
     *
     * @param request the business object data DDL request
     * @param businessObjectFormatEntity the business object format entity
     * @param customDdlEntity the optional custom DDL entity
     * @param storageEntity the storage entity
     * @param s3BucketName the S3 bucket name
     *
     * @return the generated DDL
     */
    public abstract String generateCreateTableDdl(BusinessObjectDataDdlRequest request, BusinessObjectFormatEntity businessObjectFormatEntity,
        CustomDdlEntity customDdlEntity, StorageEntity storageEntity, String s3BucketName);
}
