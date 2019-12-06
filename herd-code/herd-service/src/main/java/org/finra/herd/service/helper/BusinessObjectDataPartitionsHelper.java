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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.Partition;
import org.finra.herd.model.dto.StorageUnitAvailabilityDto;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;

@Component
public class BusinessObjectDataPartitionsHelper
{
    @Autowired
    private BusinessObjectDataDdlPartitionsHelper businessObjectDataDdlPartitionsHelper;

    /**
     * Generates the partitions information as per specified business object data partitions request.
     *
     * @param request the business object data DDL request
     * @param businessObjectFormatEntity the business object format entity
     * @param storageNames the list of storage names
     * @param requestedStorageEntities the list of storage entities per storage names specified in the request
     * @param cachedStorageEntities the map of storage names in upper case to the relative storage entities
     * @param cachedS3BucketNames the map of storage names in upper case to the relative S3 bucket names
     *
     * @return the business object data partitions list
     */
    public List<Partition> generatePartitions(BusinessObjectDataDdlRequest request, BusinessObjectFormatEntity businessObjectFormatEntity,
        List<String> storageNames, List<StorageEntity> requestedStorageEntities, Map<String, StorageEntity> cachedStorageEntities,
        Map<String, String> cachedS3BucketNames)
    {

        BusinessObjectDataDdlPartitionsHelper.GenerateDdlRequestWrapper generateDdlRequestWrapper = businessObjectDataDdlPartitionsHelper
            .buildGenerateDdlPartitionsWrapper(request, businessObjectFormatEntity, null, storageNames, requestedStorageEntities, cachedStorageEntities,
                cachedS3BucketNames);
        BusinessObjectFormat businessObjectFormatForSchema = businessObjectDataDdlPartitionsHelper.validatePartitionFiltersAndFormat(generateDdlRequestWrapper);
        List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos =
            businessObjectDataDdlPartitionsHelper.processPartitionFiltersForGenerateDdlPartitions(generateDdlRequestWrapper);

        List<Partition> partitions = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        HashMap<String, String> replacements = new HashMap<>();
        businessObjectDataDdlPartitionsHelper
            .processStorageUnitsForGenerateDdlPartitions(generateDdlRequestWrapper, sb, partitions, replacements, businessObjectFormatForSchema, null,
                storageUnitAvailabilityDtos);
        return partitions;
    }
}
