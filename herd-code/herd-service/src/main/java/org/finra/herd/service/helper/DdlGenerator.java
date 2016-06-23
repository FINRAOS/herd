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

import java.util.List;
import java.util.Map;

import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.StorageEntity;

/**
 * Base abstract class for DDL generation. All DDL generators will extend this class.
 */
public abstract class DdlGenerator
{
    public static final String NON_PARTITIONED_TABLE_LOCATION_CUSTOM_DDL_TOKEN = "${non-partitioned.table.location}";

    public static final String TABLE_NAME_CUSTOM_DDL_TOKEN = "${table.name}";

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
     * @param storageNames the list of storage names
     * @param storageEntities the list of storage entities
     * @param s3BucketNames the map of storage entities to the relative S3 bucket names
     *
     * @return the generated DDL
     */
    public abstract String generateCreateTableDdl(BusinessObjectDataDdlRequest request, BusinessObjectFormatEntity businessObjectFormatEntity,
        CustomDdlEntity customDdlEntity, List<String> storageNames, List<StorageEntity> storageEntities, Map<StorageEntity, String> s3BucketNames);

    public abstract String generateReplaceColumnsStatement(BusinessObjectFormatDdlRequest request, BusinessObjectFormatEntity businessObjectFormatEntity);

    /**
     * Gets the supported DDL output format that this generator can support.
     *
     * @return BusinessObjectDataDdlOutputFormatEnum supported output format.
     */
    public abstract BusinessObjectDataDdlOutputFormatEnum getDdlOutputFormat();
}
