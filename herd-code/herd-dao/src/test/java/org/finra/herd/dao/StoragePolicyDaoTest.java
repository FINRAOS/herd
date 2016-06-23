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
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;

public class StoragePolicyDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetStoragePolicyByAltKey()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity =
            createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Retrieve this storage policy by alternate key.
        StoragePolicyEntity resultStoragePolicyEntity = storagePolicyDao.getStoragePolicyByAltKey(storagePolicyKey);

        // Validate the returned object.
        assertNotNull(resultStoragePolicyEntity);
        assertEquals(storagePolicyEntity.getId(), resultStoragePolicyEntity.getId());

        // Retrieve this storage policy by alternate key in upper case.
        resultStoragePolicyEntity =
            storagePolicyDao.getStoragePolicyByAltKey(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toUpperCase(), STORAGE_POLICY_NAME.toUpperCase()));

        // Validate the returned object.
        assertNotNull(resultStoragePolicyEntity);
        assertEquals(storagePolicyEntity.getId(), resultStoragePolicyEntity.getId());

        // Retrieve this storage policy by alternate key in lower case.
        resultStoragePolicyEntity =
            storagePolicyDao.getStoragePolicyByAltKey(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toLowerCase(), STORAGE_POLICY_NAME.toLowerCase()));

        // Validate the returned object.
        assertNotNull(resultStoragePolicyEntity);
        assertEquals(storagePolicyEntity.getId(), resultStoragePolicyEntity.getId());
    }

    @Test
    public void testGetStoragePolicyByAltKeyMultipleLatestVersions()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist two versions of a storage policy both marked as being the latest version.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, SECOND_VERSION, LATEST_VERSION_FLAG_SET);

        // Try to retrieve the latest version of this storage policy by alternate key.
        try
        {
            storagePolicyDao.getStoragePolicyByAltKey(storagePolicyKey);
            fail("Should throw an IllegalArgumentException if finds more than one storage policy versions marked as latest.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Found more than one storage policy with with parameters {namespace=\"%s\", storagePolicyName=\"%s\", " + "storagePolicyVersion=\"null\"}.",
                STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME), e.getMessage());
        }
    }

    @Test
    public void testGetStoragePolicyByAltKeyAndVersion()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist two versions of a storage policy.
        List<StoragePolicyEntity> storagePolicyEntities = Arrays.asList(
            createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, NO_LATEST_VERSION_FLAG_SET),
            createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, SECOND_VERSION, LATEST_VERSION_FLAG_SET));

        // Retrieve the relative storage policy versions by alternate key and version.
        for (StoragePolicyEntity storagePolicyEntity : storagePolicyEntities)
        {
            StoragePolicyEntity resultStoragePolicyEntity =
                storagePolicyDao.getStoragePolicyByAltKeyAndVersion(storagePolicyKey, storagePolicyEntity.getVersion());

            // Validate the returned object.
            assertEquals(storagePolicyEntity, resultStoragePolicyEntity);
        }

        // Try to retrieve a non-existing storage policy version.
        assertNull(storagePolicyDao.getStoragePolicyByAltKeyAndVersion(storagePolicyKey, THIRD_VERSION));
    }
}
