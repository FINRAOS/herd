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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.DataProvider;
import org.finra.herd.model.api.xml.DataProviderCreateRequest;
import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.api.xml.DataProviderKeys;

/**
 * This class tests various functionality within the data provider REST controller.
 */
public class DataProviderServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateDataProvider() throws Exception
    {
        // Create a data provider.
        DataProvider resultDataProvider = dataProviderService.createDataProvider(new DataProviderCreateRequest(DATA_PROVIDER_NAME));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), resultDataProvider);
    }

    @Test
    public void testCreateDataProviderMissingRequiredParameters()
    {
        // Try to create a data provider instance when data provider name is not specified.
        try
        {
            dataProviderService.createDataProvider(new DataProviderCreateRequest(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when data provider name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A data provider name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateDataProviderTrimParameters()
    {
        // Create a data provider using input parameters with leading and trailing empty spaces.
        DataProvider resultDataProvider = dataProviderService.createDataProvider(new DataProviderCreateRequest(addWhitespace(DATA_PROVIDER_NAME)));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), resultDataProvider);
    }

    @Test
    public void testCreateDataProviderUpperCaseParameters()
    {
        // Create a data provider using upper case input parameters.
        DataProvider resultDataProvider = dataProviderService.createDataProvider(new DataProviderCreateRequest(DATA_PROVIDER_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME.toUpperCase()), resultDataProvider);
    }

    @Test
    public void testCreateDataProviderLowerCaseParameters()
    {
        // Create a data provider using lower case input parameters.
        DataProvider resultDataProvider = dataProviderService.createDataProvider(new DataProviderCreateRequest(DATA_PROVIDER_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME.toLowerCase()), resultDataProvider);
    }

    @Test
    public void testCreateDataProviderAlreadyExists() throws Exception
    {
        // Create and persist a data provider.
        createDataProviderEntity(DATA_PROVIDER_NAME);

        // Try to create a data provider when it already exists.
        try
        {
            dataProviderService.createDataProvider(new DataProviderCreateRequest(DATA_PROVIDER_NAME));
            fail("Should throw an AlreadyExistsException when data provider already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create data provider \"%s\" because it already exists.", DATA_PROVIDER_NAME), e.getMessage());
        }
    }

    @Test
    public void testGetDataProvider() throws Exception
    {
        // Create and persist a data provider entity.
        createDataProviderEntity(DATA_PROVIDER_NAME);

        // Retrieve the data provider.
        DataProvider resultDataProvider = dataProviderService.getDataProvider(new DataProviderKey(DATA_PROVIDER_NAME));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), resultDataProvider);
    }

    @Test
    public void testGetDataProviderMissingRequiredParameters()
    {
        // Try to get a data provider when data provider name is not specified.
        try
        {
            dataProviderService.getDataProvider(new DataProviderKey(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when data provider name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A data provider name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetDataProviderTrimParameters()
    {
        // Create and persist a data provider entity.
        createDataProviderEntity(DATA_PROVIDER_NAME);

        // Retrieve the data provider using input parameters with leading and trailing empty spaces.
        DataProvider resultDataProvider = dataProviderService.getDataProvider(new DataProviderKey(addWhitespace(DATA_PROVIDER_NAME)));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), resultDataProvider);
    }

    @Test
    public void testGetDataProviderUpperCaseParameters()
    {
        // Create and persist a data provider entity.
        createDataProviderEntity(DATA_PROVIDER_NAME);

        // Retrieve the data provider using upper case input parameters.
        DataProvider resultDataProvider = dataProviderService.getDataProvider(new DataProviderKey(DATA_PROVIDER_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), resultDataProvider);
    }

    @Test
    public void testGetDataProviderLowerCaseParameters()
    {
        // Create and persist a data provider entity.
        createDataProviderEntity(DATA_PROVIDER_NAME);

        // Retrieve the data provider using lower case input parameters.
        DataProvider resultDataProvider = dataProviderService.getDataProvider(new DataProviderKey(DATA_PROVIDER_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), resultDataProvider);
    }

    @Test
    public void testGetDataProviderNoExists() throws Exception
    {
        // Try to get a non-existing data provider.
        try
        {
            dataProviderService.getDataProvider(new DataProviderKey(DATA_PROVIDER_NAME));
            fail("Should throw an ObjectNotFoundException when data provider doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Data provider with name \"%s\" doesn't exist.", DATA_PROVIDER_NAME), e.getMessage());
        }
    }

    @Test
    public void testDeleteDataProvider() throws Exception
    {
        // Create a data provider key.
        DataProviderKey dataProviderKey = new DataProviderKey(DATA_PROVIDER_NAME);

        // Create and persist a data provider entity.
        createDataProviderEntity(DATA_PROVIDER_NAME);

        // Validate that this data provider exists.
        assertNotNull(dataProviderDao.getDataProviderByKey(dataProviderKey));

        // Delete this data provider.
        DataProvider deletedDataProvider = dataProviderService.deleteDataProvider(new DataProviderKey(DATA_PROVIDER_NAME));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), deletedDataProvider);

        // Ensure that this data provider is no longer there.
        assertNull(dataProviderDao.getDataProviderByKey(dataProviderKey));
    }

    @Test
    public void testDeleteDataProviderMissingRequiredParameters()
    {
        // Try to delete a data provider instance when data provider name is not specified.
        try
        {
            dataProviderService.deleteDataProvider(new DataProviderKey(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when data provider name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A data provider name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteDataProviderTrimParameters()
    {
        // Create a data provider key.
        DataProviderKey dataProviderKey = new DataProviderKey(DATA_PROVIDER_NAME);

        // Create and persist a data provider entity.
        createDataProviderEntity(DATA_PROVIDER_NAME);

        // Validate that this data provider exists.
        assertNotNull(dataProviderDao.getDataProviderByKey(dataProviderKey));

        // Delete this data provider using input parameters with leading and trailing empty spaces.
        DataProvider deletedDataProvider = dataProviderService.deleteDataProvider(new DataProviderKey(addWhitespace(DATA_PROVIDER_NAME)));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), deletedDataProvider);

        // Ensure that this data provider is no longer there.
        assertNull(dataProviderDao.getDataProviderByKey(dataProviderKey));
    }

    @Test
    public void testDeleteDataProviderUpperCaseParameters()
    {
        // Create a data provider key.
        DataProviderKey dataProviderKey = new DataProviderKey(DATA_PROVIDER_NAME);

        // Create and persist a data provider entity.
        createDataProviderEntity(DATA_PROVIDER_NAME);

        // Validate that this data provider exists.
        assertNotNull(dataProviderDao.getDataProviderByKey(dataProviderKey));

        // Delete this data provider using upper case input parameters.
        DataProvider deletedDataProvider = dataProviderService.deleteDataProvider(new DataProviderKey(DATA_PROVIDER_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), deletedDataProvider);

        // Ensure that this data provider is no longer there.
        assertNull(dataProviderDao.getDataProviderByKey(dataProviderKey));
    }

    @Test
    public void testDeleteDataProviderLowerCaseParameters()
    {
        // Create a data provider key.
        DataProviderKey dataProviderKey = new DataProviderKey(DATA_PROVIDER_NAME);

        // Create and persist a data provider entity.
        createDataProviderEntity(DATA_PROVIDER_NAME);

        // Validate that this data provider exists.
        assertNotNull(dataProviderDao.getDataProviderByKey(dataProviderKey));

        // Delete the data provider using lower case input parameters.
        DataProvider deletedDataProvider = dataProviderService.deleteDataProvider(new DataProviderKey(DATA_PROVIDER_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new DataProvider(DATA_PROVIDER_NAME), deletedDataProvider);

        // Ensure that this data provider is no longer there.
        assertNull(dataProviderDao.getDataProviderByKey(dataProviderKey));
    }

    @Test
    public void testDeleteDataProviderNoExists() throws Exception
    {
        // Try to delete a non-existing data provider.
        try
        {
            dataProviderService.deleteDataProvider(new DataProviderKey(DATA_PROVIDER_NAME));
            fail("Should throw an ObjectNotFoundException when data provider doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Data provider with name \"%s\" doesn't exist.", DATA_PROVIDER_NAME), e.getMessage());
        }
    }

    @Test
    public void testGetDataProviders() throws Exception
    {
        // Create and persist data provider entities.
        for (DataProviderKey key : DATA_PROVIDER_KEYS)
        {
            createDataProviderEntity(key.getDataProviderName());
        }

        // Retrieve a list of data provider keys.
        DataProviderKeys resultDataProviderKeys = dataProviderService.getDataProviders();

        // Validate the returned object.
        assertEquals(DATA_PROVIDER_KEYS, resultDataProviderKeys.getDataProviderKeys());
    }
}
