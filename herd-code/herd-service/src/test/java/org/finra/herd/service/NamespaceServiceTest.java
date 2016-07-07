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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespaceKeys;

/**
 * This class tests various functionality within the namespace REST controller.
 */
public class NamespaceServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateNamespace() throws Exception
    {
        // Create a namespace.
        Namespace resultNamespace = namespaceService.createNamespace(createNamespaceCreateRequest(NAMESPACE));

        // Validate the returned object.
        validateNamespace(NAMESPACE, resultNamespace);
    }

    @Test
    public void testCreateNamespaceMissingRequiredParameters()
    {
        // Try to create a namespace instance when namespace code is not specified.
        try
        {
            namespaceService.createNamespace(createNamespaceCreateRequest(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateNamespaceTrimParameters()
    {
        // Create a namespace using input parameters with leading and trailing empty spaces.
        Namespace resultNamespace = namespaceService.createNamespace(createNamespaceCreateRequest(addWhitespace(NAMESPACE)));

        // Validate the returned object.
        validateNamespace(NAMESPACE, resultNamespace);
    }

    @Test
    public void testCreateNamespaceUpperCaseParameters()
    {
        // Create a namespace using upper case input parameters.
        Namespace resultNamespace = namespaceService.createNamespace(createNamespaceCreateRequest(NAMESPACE.toUpperCase()));

        // Validate the returned object.
        validateNamespace(NAMESPACE.toUpperCase(), resultNamespace);
    }

    @Test
    public void testCreateNamespaceLowerCaseParameters()
    {
        // Create a namespace using lower case input parameters.
        Namespace resultNamespace = namespaceService.createNamespace(createNamespaceCreateRequest(NAMESPACE.toLowerCase()));

        // Validate the returned object.
        validateNamespace(NAMESPACE.toLowerCase(), resultNamespace);
    }

    @Test
    public void testCreateNamespaceInvalidParameters()
    {
        // Try to create a namespace instance when namespace contains a slash character.
        try
        {
            namespaceService.createNamespace(createNamespaceCreateRequest(addSlash(NAMESPACE)));
            fail("Should throw an IllegalArgumentException when namespace contains a slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateNamespaceAlreadyExists() throws Exception
    {
        // Create and persist a namespace.
        createNamespaceEntity(NAMESPACE);

        // Try to create a namespace when it already exists.
        try
        {
            namespaceService.createNamespace(createNamespaceCreateRequest(NAMESPACE));
            fail("Should throw an AlreadyExistsException when namespace already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create namespace \"%s\" because it already exists.", NAMESPACE), e.getMessage());
        }
    }

    @Test
    public void testGetNamespace() throws Exception
    {
        // Create and persist a namespace entity.
        createNamespaceEntity(NAMESPACE);

        // Retrieve the namespace.
        Namespace resultNamespace = namespaceService.getNamespace(new NamespaceKey(NAMESPACE));

        // Validate the returned object.
        validateNamespace(NAMESPACE, resultNamespace);
    }

    @Test
    public void testGetNamespaceMissingRequiredParameters()
    {
        // Try to get a namespace instance when namespace code is not specified.
        try
        {
            namespaceService.getNamespace(new NamespaceKey(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when namespace code is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetNamespaceTrimParameters()
    {
        // Create and persist a namespace entity.
        createNamespaceEntity(NAMESPACE);

        // Retrieve the namespace using input parameters with leading and trailing empty spaces.
        Namespace resultNamespace = namespaceService.getNamespace(new NamespaceKey(addWhitespace(NAMESPACE)));

        // Validate the returned object.
        validateNamespace(NAMESPACE, resultNamespace);
    }

    @Test
    public void testGetNamespaceUpperCaseParameters()
    {
        // Create and persist a namespace entity using lower case values.
        createNamespaceEntity(NAMESPACE.toLowerCase());

        // Retrieve the namespace using upper case input parameters.
        Namespace resultNamespace = namespaceService.getNamespace(new NamespaceKey(NAMESPACE.toUpperCase()));

        // Validate the returned object.
        validateNamespace(NAMESPACE.toLowerCase(), resultNamespace);
    }

    @Test
    public void testGetNamespaceLowerCaseParameters()
    {
        // Create and persist a namespace entity using upper case values.
        createNamespaceEntity(NAMESPACE.toUpperCase());

        // Retrieve the namespace using lower case input parameters.
        Namespace resultNamespace = namespaceService.getNamespace(new NamespaceKey(NAMESPACE.toLowerCase()));

        // Validate the returned object.
        validateNamespace(NAMESPACE.toUpperCase(), resultNamespace);
    }

    @Test
    public void testGetNamespaceNoExists() throws Exception
    {
        // Try to get a non-existing namespace.
        try
        {
            namespaceService.getNamespace(new NamespaceKey(NAMESPACE));
            fail("Should throw an ObjectNotFoundException when namespace doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", NAMESPACE), e.getMessage());
        }
    }

    @Test
    public void testGetNamespaces() throws Exception
    {
        // Create and persist namespace entities.
        for (NamespaceKey key : getTestNamespaceKeys())
        {
            createNamespaceEntity(key.getNamespaceCode());
        }

        // Retrieve a list of namespace keys.
        NamespaceKeys resultNamespaceKeys = namespaceService.getNamespaces();

        // Validate the returned object.
        assertNotNull(resultNamespaceKeys);
        assertNotNull(resultNamespaceKeys.getNamespaceKeys());
        assertTrue(resultNamespaceKeys.getNamespaceKeys().size() >= getTestNamespaceKeys().size());
        for (NamespaceKey key : getTestNamespaceKeys())
        {
            assertTrue(resultNamespaceKeys.getNamespaceKeys().contains(key));
        }
    }

    @Test
    public void testDeleteNamespace() throws Exception
    {
        // Create and persist a namespace entity.
        createNamespaceEntity(NAMESPACE);

        // Validate that this namespace exists.
        NamespaceKey namespaceKey = new NamespaceKey(NAMESPACE);
        assertNotNull(namespaceDao.getNamespaceByKey(namespaceKey));

        // Delete this namespace.
        Namespace deletedNamespace = namespaceService.deleteNamespace(new NamespaceKey(NAMESPACE));

        // Validate the returned object.
        validateNamespace(NAMESPACE, deletedNamespace);

        // Ensure that this namespace is no longer there.
        assertNull(namespaceDao.getNamespaceByKey(namespaceKey));
    }

    @Test
    public void testDeleteNamespaceMissingRequiredParameters()
    {
        // Try to delete a namespace instance when namespace code is not specified.
        try
        {
            namespaceService.deleteNamespace(new NamespaceKey(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when namespace code is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteNamespaceTrimParameters()
    {
        // Create and persist a namespace entity.
        createNamespaceEntity(NAMESPACE);

        // Validate that this namespace exists.
        NamespaceKey namespaceKey = new NamespaceKey(NAMESPACE);
        assertNotNull(namespaceDao.getNamespaceByKey(namespaceKey));

        // Delete this namespace using input parameters with leading and trailing empty spaces.
        Namespace deletedNamespace = namespaceService.deleteNamespace(new NamespaceKey(addWhitespace(NAMESPACE)));

        // Validate the returned object.
        validateNamespace(NAMESPACE, deletedNamespace);

        // Ensure that this namespace is no longer there.
        assertNull(namespaceDao.getNamespaceByKey(namespaceKey));
    }

    @Test
    public void testDeleteNamespaceUpperCaseParameters()
    {
        // Create and persist a namespace entity using lower case values.
        createNamespaceEntity(NAMESPACE.toLowerCase());

        // Validate that this namespace exists.
        NamespaceKey namespaceKey = new NamespaceKey(NAMESPACE.toLowerCase());
        assertNotNull(namespaceDao.getNamespaceByKey(namespaceKey));

        // Delete this namespace using upper case input parameters.
        Namespace deletedNamespace = namespaceService.deleteNamespace(new NamespaceKey(NAMESPACE.toUpperCase()));

        // Validate the returned object.
        validateNamespace(NAMESPACE.toLowerCase(), deletedNamespace);

        // Ensure that this namespace is no longer there.
        assertNull(namespaceDao.getNamespaceByKey(namespaceKey));
    }

    @Test
    public void testDeleteNamespaceLowerCaseParameters()
    {
        // Create and persist a namespace entity using upper case values.
        createNamespaceEntity(NAMESPACE.toUpperCase());

        // Validate that this namespace exists.
        NamespaceKey namespaceKey = new NamespaceKey(NAMESPACE.toUpperCase());
        assertNotNull(namespaceDao.getNamespaceByKey(namespaceKey));

        // Delete the namespace using lower case input parameters.
        Namespace deletedNamespace = namespaceService.deleteNamespace(new NamespaceKey(NAMESPACE.toLowerCase()));

        // Validate the returned object.
        validateNamespace(NAMESPACE.toUpperCase(), deletedNamespace);

        // Ensure that this namespace is no longer there.
        assertNull(namespaceDao.getNamespaceByKey(namespaceKey));
    }

    @Test
    public void testDeleteNamespaceNoExists() throws Exception
    {
        // Try to get a non-existing namespace.
        try
        {
            namespaceService.deleteNamespace(new NamespaceKey(NAMESPACE));
            fail("Should throw an ObjectNotFoundException when namespace doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", NAMESPACE), e.getMessage());
        }
    }
}
