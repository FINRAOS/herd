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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Test cases for {@link StorageDaoHelper#getStorageAttributeIntegerValueByName(String, org.finra.herd.model.jpa.StorageEntity, boolean, boolean)
 * getStorageAttributeIntegerValueByName} and {@link StorageDaoHelper#getStorageAttributeIntegerValueByName(String, org.finra.herd.model.jpa.StorageEntity,
 * Integer) getStorageAttributeIntegerValueByName with default}
 */
public class StorageDaoHelperGetStorageAttributeIntegerValueByNameTest extends AbstractServiceTest
{
    /**
     * Storage attribute: 1234 Attribute required: false Attribute required if exists: true Assert result = 1234
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_1()
    {
        String attributeName = "test";
        String attributeValue = "1234";
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = true;
        Integer value = storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
        assertEquals(new Integer(attributeValue), value);
    }

    /**
     * Storage attribute: 1234 Attribute required: true Attribute required if exists: false Assert result = 1234
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_2()
    {
        String attributeName = "test";
        String attributeValue = "1234";
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = true;
        boolean attributeValueRequiredIfExists = false;
        Integer value = storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
        assertEquals(new Integer(attributeValue), value);
    }

    /**
     * Storage attribute: 1234 Attribute required: false Attribute required if exists: false Assert result = 1234
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_3()
    {
        String attributeName = "test";
        String attributeValue = "1234";
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = false;
        Integer value = storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
        assertEquals(new Integer(attributeValue), value);
    }

    /**
     * Storage attribute: abcd Attribute required: false Attribute required if exists: false Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_4()
    {
        String attributeName = "test";
        String attributeValue = "abcd";
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = false;
        try
        {
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Storage attribute \"" + attributeName + "\" must be a valid integer. Actual value is \"" + attributeValue + "\"", e.getMessage());
        }
    }

    /**
     * Storage attribute: blank Attribute required: false Attribute required if exists: true Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_5()
    {
        String attributeName = "test";
        String attributeValue = BLANK_TEXT;
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = true;
        try
        {
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Attribute \"" + attributeName + "\" for \"" + storageEntity.getName() + "\" storage must have a value that is not blank.",
                e.getMessage());
        }
    }

    /**
     * Storage attribute: blank Attribute required: true Attribute required if exists: false Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_6()
    {
        String attributeName = "test";
        String attributeValue = BLANK_TEXT;
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = true;
        boolean attributeValueRequiredIfExists = false;
        try
        {
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Storage attribute \"" + attributeName + "\" must be a valid integer. Actual value is \"" + attributeValue + "\"", e.getMessage());
        }
    }

    /**
     * Storage attribute: blank Attribute required: false Attribute required if exists: false Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_7()
    {
        String attributeName = "test";
        String attributeValue = BLANK_TEXT;
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = false;
        try
        {
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Storage attribute \"" + attributeName + "\" must be a valid integer. Actual value is \"" + attributeValue + "\"", e.getMessage());
        }
    }

    /**
     * Storage attribute: non-existent Attribute required: false Attribute required if exists: true Assert return null
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_8()
    {
        String attributeName = "test";
        StorageEntity storageEntity = createStorageEntity();
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = true;
        Integer value = storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
        assertEquals(null, value);
    }

    /**
     * Storage attribute: non-existent Attribute required: true Attribute required if exists: false Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_9()
    {
        String attributeName = "test";
        StorageEntity storageEntity = createStorageEntity();
        boolean attributeRequired = true;
        boolean attributeValueRequiredIfExists = false;
        try
        {
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Attribute \"" + attributeName + "\" for \"" + storageEntity.getName() + "\" storage must be configured.", e.getMessage());
        }
    }

    /**
     * Storage attribute: non-existent Attribute required: false Attribute required if exists: false Assert return null
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_10()
    {
        String attributeName = "test";
        StorageEntity storageEntity = createStorageEntity();
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = true;
        Integer value = storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
        assertEquals(null, value);
    }

    /**
     * Storage attribute: null Attribute required: false Attribute required if exists: true Assert return null
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_11()
    {
        String attributeName = "test";
        String attributeValue = null;
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = true;
        try
        {
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Attribute \"" + attributeName + "\" for \"" + storageEntity.getName() + "\" storage must have a value that is not blank.",
                e.getMessage());
        }
    }

    /**
     * Storage attribute: null Attribute required: true Attribute required if exists: false Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_12()
    {
        String attributeName = "test";
        String attributeValue = null;
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = true;
        boolean attributeValueRequiredIfExists = false;
        Integer value = storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
        assertEquals(attributeValue, value);
    }

    /**
     * Storage attribute: null Attribute required: false Attribute required if exists: false Assert return null
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_13()
    {
        String attributeName = "test";
        String attributeValue = null;
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = true;
        boolean attributeValueRequiredIfExists = false;
        assertEquals(attributeValue,
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists));
    }

    /**
     * Storage attribute: non-existent Default value: 2345 Assert return default value
     */
    @Test
    public void testGetStorageAttributeIntegerValueByNameWithDefault_1()
    {
        String attributeName = "test";
        StorageEntity storageEntity = createStorageEntity();
        Integer defaultValue = 2345;
        Integer value = storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
        assertEquals(defaultValue, value);
    }

    /**
     * Storage attribute: non-existent Default value: null Assert return default value
     */
    @Test
    public void testGetStorageAttributeIntegerValueByNameWithDefault_2()
    {
        String attributeName = "test";
        StorageEntity storageEntity = createStorageEntity();
        Integer defaultValue = null;
        Integer value = storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
        assertEquals(defaultValue, value);
    }

    /**
     * Storage attribute: 1234 Default value: 2345 Assert return default value
     */
    @Test
    public void testGetStorageAttributeIntegerValueByNameWithDefault_3()
    {
        String attributeName = "test";
        String attributeValue = "1234";
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = 2345;
        Integer value = storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
        assertEquals(new Integer(attributeValue), value);
    }

    /**
     * Storage attribute: 1234 Default value: null Assert return default value
     */
    @Test
    public void testGetStorageAttributeIntegerValueByNameWithDefault_4()
    {
        String attributeName = "test";
        String attributeValue = "1234";
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = null;
        Integer value = storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
        assertEquals(new Integer(attributeValue), value);
    }

    /**
     * Storage attribute: abcd Default value: 2345 Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByNameWithDefault_5()
    {
        String attributeName = "test";
        String attributeValue = "abcd";
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = 2345;
        try
        {
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Storage attribute \"" + attributeName + "\" must be a valid integer. Actual value is \"" + attributeValue + "\"", e.getMessage());
        }
    }

    /**
     * Storage attribute: abcd Default value: null Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByNameWithDefault_6()
    {
        String attributeName = "test";
        String attributeValue = "abcd";
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = null;
        try
        {
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Storage attribute \"" + attributeName + "\" must be a valid integer. Actual value is \"" + attributeValue + "\"", e.getMessage());
        }
    }

    /**
     * Storage attribute: blank Default value: 2345 Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByNameWithDefault_7()
    {
        String attributeName = "test";
        String attributeValue = BLANK_TEXT;
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = 2345;
        try
        {
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Storage attribute \"" + attributeName + "\" must be a valid integer. Actual value is \"" + attributeValue + "\"", e.getMessage());
        }
    }

    /**
     * Storage attribute: blank Default value: null Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByNameWithDefault_8()
    {
        String attributeName = "test";
        String attributeValue = BLANK_TEXT;
        StorageEntity storageEntity = createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = null;
        try
        {
            storageDaoHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Storage attribute \"" + attributeName + "\" must be a valid integer. Actual value is \"" + attributeValue + "\"", e.getMessage());
        }
    }
}
