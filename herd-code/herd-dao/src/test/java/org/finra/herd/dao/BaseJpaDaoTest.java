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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.jpa.StoragePlatformEntity;

/**
 * This class tests various functionality in the base JPA DAO.
 */
public class BaseJpaDaoTest extends AbstractDaoTest
{
    // The "name" property for the storage platform entity.
    private static final String NAME_PROPERTY = "name";

    // The "createdBy" property for the auditable entities.
    private static final String CREATED_BY_PROPERTY = "createdBy";

    // Although HerdDao extends BaseJpaDao, we will autowire it so we can use it directly and not take a chance that any base functionality is overridden.
    @Qualifier(value = "baseJpaDaoImpl")
    @Autowired
    protected BaseJpaDao baseJpaDao;

    @Test
    public void testGetEntityManager()
    {
        assertNotNull(baseJpaDao.getEntityManager());
    }

    @Test
    public void testFindById()
    {
        StoragePlatformEntity storagePlatformEntity = baseJpaDao.findById(StoragePlatformEntity.class, StoragePlatformEntity.S3);
        assertNotNull(storagePlatformEntity);
        assertEquals(StoragePlatformEntity.S3, storagePlatformEntity.getName());
    }

    @Test
    public void testFindByNamedProperties()
    {
        Map<String, String> namedProperties = new HashMap<>();
        namedProperties.put(NAME_PROPERTY, StoragePlatformEntity.S3);
        List<StoragePlatformEntity> storagePlatformEntities = baseJpaDao.findByNamedProperties(StoragePlatformEntity.class, namedProperties);
        validateSingleS3StorageEntity(storagePlatformEntities);
    }

    @Test
    public void testFindUniqueByNamedProperties()
    {
        // Retrieve the existing entity to get the createdBy value needed to use more than one property to query against.
        StoragePlatformEntity storagePlatformEntity = baseJpaDao.findById(StoragePlatformEntity.class, StoragePlatformEntity.S3);
        assertNotNull(storagePlatformEntity);

        Map<String, String> namedProperties = new HashMap<>();
        namedProperties.put(NAME_PROPERTY, StoragePlatformEntity.S3);
        namedProperties.put(CREATED_BY_PROPERTY, storagePlatformEntity.getCreatedBy());
        storagePlatformEntity = baseJpaDao.findUniqueByNamedProperties(StoragePlatformEntity.class, namedProperties);
        assertNotNull(storagePlatformEntity);
        assertEquals(StoragePlatformEntity.S3, storagePlatformEntity.getName());
    }

    @Test
    public void testFindUniqueByNamedPropertiesNoExist()
    {
        Map<String, String> namedProperties = new HashMap<>();
        namedProperties.put(NAME_PROPERTY, UUID.randomUUID().toString()); // Search for a random entity name that doesn't exist.
        StoragePlatformEntity storagePlatformEntity = baseJpaDao.findUniqueByNamedProperties(StoragePlatformEntity.class, namedProperties);
        assertNull(storagePlatformEntity);
    }

    /**
     * Validates that the list contains a single S3 storage platform entity.
     *
     * @param storagePlatformEntities the list of storage platform entities to test.
     */
    private void validateSingleS3StorageEntity(List<StoragePlatformEntity> storagePlatformEntities)
    {
        assertNotNull(storagePlatformEntities);
        assertEquals(1, storagePlatformEntities.size());
        assertEquals(StoragePlatformEntity.S3, storagePlatformEntities.get(0).getName());
    }
}
