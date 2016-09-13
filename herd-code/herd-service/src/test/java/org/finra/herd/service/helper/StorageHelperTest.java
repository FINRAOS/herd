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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.service.AbstractServiceTest;

public class StorageHelperTest extends AbstractServiceTest
{
    @Test
    public void testGetStorageAttributeValueByNameFromStorage()
    {
        final String testStorageName = "MY_TEST_STORAGE";
        final String testAttributeNameNoExists = "I_DO_NOT_EXIST";

        Storage testStorage = new Storage();
        testStorage.setName(testStorageName);
        testStorage.setAttributes(getNewAttributes());

        assertEquals(ATTRIBUTE_VALUE_1, storageHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE, testStorage, Boolean.FALSE));
        assertEquals(ATTRIBUTE_VALUE_2, storageHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_2_MIXED_CASE, testStorage, Boolean.TRUE));

        // Testing attribute name case insensitivity.
        assertEquals(ATTRIBUTE_VALUE_1, storageHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), testStorage, Boolean.TRUE));
        assertEquals(ATTRIBUTE_VALUE_1, storageHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), testStorage, Boolean.TRUE));

        assertNull(storageHelper.getStorageAttributeValueByName(testAttributeNameNoExists, testStorage, Boolean.FALSE));

        // Try to get a required attribute value what does not exist.
        try
        {
            storageHelper.getStorageAttributeValueByName(testAttributeNameNoExists, testStorage, Boolean.TRUE);
            fail("Suppose to throw a RuntimeException when required storage attribute does not exist or has a blank value.");
        }
        catch (RuntimeException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.", testAttributeNameNoExists, testStorage.getName()),
                e.getMessage());
        }
    }

    @Test
    public void testGetStorageAttributeValueByNameFromStorageEntity()
    {
        // Create an S3 storage entity.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(ATTRIBUTE_NAME_2_MIXED_CASE, BLANK_TEXT));
        attributes.add(new Attribute(ATTRIBUTE_NAME_3_MIXED_CASE, null));
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Retrieve optional attribute values.
        Assert.assertEquals(ATTRIBUTE_VALUE_1, storageHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE, storageEntity, false));
        Assert.assertEquals(BLANK_TEXT, storageHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_2_MIXED_CASE, storageEntity, false));
        Assert.assertNull(storageHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_3_MIXED_CASE, storageEntity, false));

        // Validate case insensitivity.
        Assert.assertEquals(ATTRIBUTE_VALUE_1, storageHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), storageEntity, false));
        Assert.assertEquals(ATTRIBUTE_VALUE_1, storageHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), storageEntity, false));

        // Retrieve a required attribute value.
        Assert.assertEquals(ATTRIBUTE_VALUE_1, storageHelper.getStorageAttributeValueByName(ATTRIBUTE_NAME_1_MIXED_CASE, storageEntity, true));

        // Try to retrieve a missing required attribute values when
        // - attribute does not exist
        // - attribute exists with a blank text value
        // - attribute exists with a null value
        String attributeNoExist = "I_DO_NOT_EXIST";
        for (String attributeName : Arrays.asList(attributeNoExist, ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_NAME_3_MIXED_CASE))
        {
            try
            {
                storageHelper.getStorageAttributeValueByName(attributeName, storageEntity, true);
            }
            catch (IllegalStateException e)
            {
                if (attributeName.equals(attributeNoExist))
                {
                    Assert.assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.", attributeName, STORAGE_NAME), e.getMessage());
                }
                else
                {
                    Assert.assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must have a value that is not blank.", attributeName, STORAGE_NAME),
                        e.getMessage());
                }
            }
        }
    }

    /**
     * Storage attribute: 1234 Attribute required: false Attribute required if exists: true Assert result = 1234
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_1()
    {
        String attributeName = "test";
        String attributeValue = "1234";
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = true;
        Integer value = storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = true;
        boolean attributeValueRequiredIfExists = false;
        Integer value = storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = false;
        Integer value = storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = false;
        try
        {
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = true;
        try
        {
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = true;
        boolean attributeValueRequiredIfExists = false;
        try
        {
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = false;
        try
        {
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = true;
        Integer value = storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
        assertEquals(null, value);
    }

    /**
     * Storage attribute: non-existent Attribute required: true Attribute required if exists: false Assert throw
     */
    @Test
    public void testGetStorageAttributeIntegerValueByName_9()
    {
        String attributeName = "test";
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();
        boolean attributeRequired = true;
        boolean attributeValueRequiredIfExists = false;
        try
        {
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = true;
        Integer value = storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = false;
        boolean attributeValueRequiredIfExists = true;
        try
        {
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = true;
        boolean attributeValueRequiredIfExists = false;
        Integer value = storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        boolean attributeRequired = true;
        boolean attributeValueRequiredIfExists = false;
        assertEquals(attributeValue,
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, attributeRequired, attributeValueRequiredIfExists));
    }

    /**
     * Storage attribute: non-existent Default value: 2345 Assert return default value
     */
    @Test
    public void testGetStorageAttributeIntegerValueByNameWithDefault_1()
    {
        String attributeName = "test";
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();
        Integer defaultValue = 2345;
        Integer value = storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
        assertEquals(defaultValue, value);
    }

    /**
     * Storage attribute: non-existent Default value: null Assert return default value
     */
    @Test
    public void testGetStorageAttributeIntegerValueByNameWithDefault_2()
    {
        String attributeName = "test";
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();
        Integer defaultValue = null;
        Integer value = storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = 2345;
        Integer value = storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = null;
        Integer value = storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = 2345;
        try
        {
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = null;
        try
        {
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = 2345;
        try
        {
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
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
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntityWithAttributes(attributeName, attributeValue);
        Integer defaultValue = null;
        try
        {
            storageHelper.getStorageAttributeIntegerValueByName(attributeName, storageEntity, defaultValue);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(IllegalStateException.class, e.getClass());
            assertEquals("Storage attribute \"" + attributeName + "\" must be a valid integer. Actual value is \"" + attributeValue + "\"", e.getMessage());
        }
    }
}
