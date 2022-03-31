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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceCreateRequest;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespaceKeys;
import org.finra.herd.model.api.xml.NamespaceSearchFilter;
import org.finra.herd.model.api.xml.NamespaceSearchKey;
import org.finra.herd.model.api.xml.NamespaceSearchRequest;
import org.finra.herd.model.api.xml.NamespaceSearchResponse;
import org.finra.herd.model.api.xml.NamespaceUpdateRequest;

/**
 * This class tests various functionality within the namespace REST controller.
 */
public class NamespaceServiceTest extends AbstractServiceTest
{
    // Constant to hold the charge code field option for the search response.
    public final static String CHARGE_CODE_FIELD = "chargeCode".toLowerCase();

    // Constant to hold the s3 key prefix field option for the search response.
    public final static String S3_KEY_PREFIX_FIELD = "s3KeyPrefix".toLowerCase();

    public final static String NO_CHARGE_CODE_FIELD = null;

    public final static String NO_S3_KEY_PREFIX_FIELD = null;

    public static final Set<String> NO_SEARCH_RESPONSE_FIELDS = new HashSet<>();

    @Test
    public void testCreateNamespace()
    {
        // Create a namespace.
        Namespace resultNamespace = namespaceService.createNamespace(new NamespaceCreateRequest(NAMESPACE, CHARGE_CODE));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE, CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), resultNamespace);
    }

    @Test
    public void testCreateNamespaceMissingRequiredParameters()
    {
        // Try to create a namespace instance when namespace code is not specified.
        try
        {
            namespaceService.createNamespace(new NamespaceCreateRequest(BLANK_TEXT, CHARGE_CODE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateNamespaceMissingOptionalParameters()
    {
        // Create a namespace without charge code
        Namespace resultNamespace = namespaceService.createNamespace(new NamespaceCreateRequest(NAMESPACE, NO_CHARGE_CODE));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE, NO_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), resultNamespace);
    }

    @Test
    public void testCreateNamespaceTrimParameters()
    {
        // Create a namespace using input parameters with leading and trailing empty spaces.
        Namespace resultNamespace = namespaceService.createNamespace(new NamespaceCreateRequest(addWhitespace(NAMESPACE), addWhitespace(CHARGE_CODE)));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE, CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), resultNamespace);
    }

    @Test
    public void testCreateNamespaceUpperCaseParameters()
    {
        // Create a namespace using upper case input parameters.
        Namespace resultNamespace = namespaceService.createNamespace(new NamespaceCreateRequest(NAMESPACE.toUpperCase(), CHARGE_CODE.toUpperCase()));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE.toUpperCase(), CHARGE_CODE.toUpperCase(), NAMESPACE_S3_KEY_PREFIX), resultNamespace);
    }

    @Test
    public void testCreateNamespaceLowerCaseParameters()
    {
        // Create a namespace using lower case input parameters.
        Namespace resultNamespace = namespaceService.createNamespace(new NamespaceCreateRequest(NAMESPACE.toLowerCase(), CHARGE_CODE.toLowerCase()));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE.toLowerCase(), CHARGE_CODE.toLowerCase(), NAMESPACE_S3_KEY_PREFIX), resultNamespace);
    }

    @Test
    public void testCreateNamespaceInvalidParameters()
    {
        // Try to create a namespace instance when namespace contains a forward slash character.
        try
        {
            namespaceService.createNamespace(new NamespaceCreateRequest(addSlash(NAMESPACE), CHARGE_CODE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a namespace instance when namespace contains a backward slash character.
        try
        {
            namespaceService.createNamespace(new NamespaceCreateRequest(addBackwardSlash(NAMESPACE), CHARGE_CODE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a backward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateNamespaceAlreadyExists()
    {
        // Create and persist a namespace using upper case.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE.toUpperCase());

        // Try to create a namespace when it already exists. We are passing namespace in lower case, to prove it is case insensitive.
        try
        {
            namespaceService.createNamespace(new NamespaceCreateRequest(NAMESPACE.toLowerCase(), CHARGE_CODE));
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create namespace \"%s\" because it already exists.", NAMESPACE.toLowerCase()), e.getMessage());
        }
    }

    @Test
    public void testGetNamespace()
    {
        // Create and persist a namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE, CHARGE_CODE);

        // Retrieve the namespace.
        Namespace resultNamespace = namespaceService.getNamespace(new NamespaceKey(NAMESPACE));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE, CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), resultNamespace);
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
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE, CHARGE_CODE);

        // Retrieve the namespace using input parameters with leading and trailing empty spaces.
        Namespace resultNamespace = namespaceService.getNamespace(new NamespaceKey(addWhitespace(NAMESPACE)));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE, CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), resultNamespace);
    }

    @Test
    public void testGetNamespaceUpperCaseParameters()
    {
        // Create and persist a namespace entity using lower case values.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE.toLowerCase(), CHARGE_CODE);

        // Retrieve the namespace using upper case input parameters.
        Namespace resultNamespace = namespaceService.getNamespace(new NamespaceKey(NAMESPACE.toUpperCase()));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE.toLowerCase(), CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), resultNamespace);
    }

    @Test
    public void testGetNamespaceLowerCaseParameters()
    {
        // Create and persist a namespace entity using upper case values.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE.toUpperCase(), CHARGE_CODE);

        // Retrieve the namespace using lower case input parameters.
        Namespace resultNamespace = namespaceService.getNamespace(new NamespaceKey(NAMESPACE.toLowerCase()));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE.toUpperCase(), CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), resultNamespace);
    }

    @Test
    public void testGetNamespaceNoExists()
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
    public void testGetNamespaces()
    {
        // Create and persist namespace entities.
        for (NamespaceKey key : namespaceDaoTestHelper.getTestNamespaceKeys())
        {
            namespaceDaoTestHelper.createNamespaceEntity(key.getNamespaceCode());
        }

        // Retrieve a list of namespace keys.
        NamespaceKeys resultNamespaceKeys = namespaceService.getNamespaces();

        // Validate the returned object.
        assertNotNull(resultNamespaceKeys);
        assertNotNull(resultNamespaceKeys.getNamespaceKeys());
        assertTrue(resultNamespaceKeys.getNamespaceKeys().size() >= namespaceDaoTestHelper.getTestNamespaceKeys().size());
        for (NamespaceKey key : namespaceDaoTestHelper.getTestNamespaceKeys())
        {
            assertTrue(resultNamespaceKeys.getNamespaceKeys().contains(key));
        }
    }

    @Test
    public void testDeleteNamespace()
    {
        // Create and persist a namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE, CHARGE_CODE);

        // Validate that this namespace exists.
        NamespaceKey namespaceKey = new NamespaceKey(NAMESPACE);
        assertNotNull(namespaceDao.getNamespaceByKey(namespaceKey));

        // Delete this namespace.
        Namespace deletedNamespace = namespaceService.deleteNamespace(new NamespaceKey(NAMESPACE));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE, CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), deletedNamespace);

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
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE, CHARGE_CODE);

        // Validate that this namespace exists.
        NamespaceKey namespaceKey = new NamespaceKey(NAMESPACE);
        assertNotNull(namespaceDao.getNamespaceByKey(namespaceKey));

        // Delete this namespace using input parameters with leading and trailing empty spaces.
        Namespace deletedNamespace = namespaceService.deleteNamespace(new NamespaceKey(addWhitespace(NAMESPACE)));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE, CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), deletedNamespace);

        // Ensure that this namespace is no longer there.
        assertNull(namespaceDao.getNamespaceByKey(namespaceKey));
    }

    @Test
    public void testDeleteNamespaceUpperCaseParameters()
    {
        // Create and persist a namespace entity using lower case values.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE.toLowerCase(), CHARGE_CODE);

        // Validate that this namespace exists.
        NamespaceKey namespaceKey = new NamespaceKey(NAMESPACE.toLowerCase());
        assertNotNull(namespaceDao.getNamespaceByKey(namespaceKey));

        // Delete this namespace using upper case input parameters.
        Namespace deletedNamespace = namespaceService.deleteNamespace(new NamespaceKey(NAMESPACE.toUpperCase()));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE.toLowerCase(), CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), deletedNamespace);

        // Ensure that this namespace is no longer there.
        assertNull(namespaceDao.getNamespaceByKey(namespaceKey));
    }

    @Test
    public void testDeleteNamespaceLowerCaseParameters()
    {
        // Create and persist a namespace entity using upper case values.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE.toUpperCase(), CHARGE_CODE);

        // Validate that this namespace exists.
        NamespaceKey namespaceKey = new NamespaceKey(NAMESPACE.toUpperCase());
        assertNotNull(namespaceDao.getNamespaceByKey(namespaceKey));

        // Delete the namespace using lower case input parameters.
        Namespace deletedNamespace = namespaceService.deleteNamespace(new NamespaceKey(NAMESPACE.toLowerCase()));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE.toUpperCase(), CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), deletedNamespace);

        // Ensure that this namespace is no longer there.
        assertNull(namespaceDao.getNamespaceByKey(namespaceKey));
    }

    @Test
    public void testDeleteNamespaceNoExists()
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

    @Test
    public void testUpdateNamespace()
    {
        // Create and persist a namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE, CHARGE_CODE);

        // Update this namespace.
        Namespace updatedNamespace = namespaceService.updateNamespaces(new NamespaceKey(NAMESPACE), new NamespaceUpdateRequest(CHARGE_CODE_2));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE, CHARGE_CODE_2, NAMESPACE_S3_KEY_PREFIX), updatedNamespace);
    }

    @Test
    public void testUpdateNamespaceTrimParameters()
    {
        // Create and persist a namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE, CHARGE_CODE);

        // Update this namespace by passing parameters padded with white space.
        Namespace updatedNamespace =
            namespaceService.updateNamespaces(new NamespaceKey(addWhitespace(NAMESPACE)), new NamespaceUpdateRequest(addWhitespace(CHARGE_CODE_2)));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE, CHARGE_CODE_2, NAMESPACE_S3_KEY_PREFIX), updatedNamespace);
    }

    @Test
    public void testUpdateNamespaceNoExists()
    {
        // Try to Update a non-existing namespace.
        try
        {
            namespaceService.updateNamespaces(new NamespaceKey(NAMESPACE), new NamespaceUpdateRequest(CHARGE_CODE));
            fail("Should throw an ObjectNotFoundException when namespace doesn't exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", NAMESPACE), e.getMessage());
        }
    }

    @Test
    public void testUpdateNamespaceUpperCaseParameters()
    {
        // Create and persist a namespace entity using lower case values.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE.toLowerCase(), CHARGE_CODE.toLowerCase());

        // Update this namespace by passing all parameters in uppercase.
        Namespace updatedNamespace =
            namespaceService.updateNamespaces(new NamespaceKey(NAMESPACE.toUpperCase()), new NamespaceUpdateRequest(CHARGE_CODE_2.toUpperCase()));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE.toLowerCase(), CHARGE_CODE_2.toUpperCase(), NAMESPACE_S3_KEY_PREFIX), updatedNamespace);
    }

    @Test
    public void testUpdateNamespaceLowerCaseParameters()
    {
        // Create and persist a namespace entity using upper case values.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE.toUpperCase(), CHARGE_CODE.toUpperCase());

        // Update this namespace by passing all parameters in lowercase.
        Namespace updatedNamespace =
            namespaceService.updateNamespaces(new NamespaceKey(NAMESPACE.toLowerCase()), new NamespaceUpdateRequest(CHARGE_CODE_2.toLowerCase()));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE.toUpperCase(), CHARGE_CODE_2.toLowerCase(), NAMESPACE_S3_KEY_PREFIX), updatedNamespace);
    }

    @Test
    public void testUpdateNamespaceMissingOptionalParameters()
    {
        // Create and persist a namespace entity with charge code.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE, CHARGE_CODE);

        // Update this namespace by passing null charge code value.
        Namespace updatedNamespace =
            namespaceService.updateNamespaces(new NamespaceKey(NAMESPACE), new NamespaceUpdateRequest(NO_CHARGE_CODE));

        // Validate the returned object.
        assertEquals(new Namespace(NAMESPACE, NO_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), updatedNamespace);
    }

    @Test
    public void testSearchNamespaces()
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForNamespaceSearchTesting();

        NamespaceSearchRequest namespaceSearchRequest =
            new NamespaceSearchRequest(Arrays.asList(new NamespaceSearchFilter(Arrays.asList(new NamespaceSearchKey(NAMESPACE_CHARGE_CODE, false)))));

        Set<String> fields = Sets.newHashSet(CHARGE_CODE_FIELD, S3_KEY_PREFIX_FIELD);

        NamespaceSearchResponse expected = new NamespaceSearchResponse(Arrays.asList(new Namespace(NAMESPACE, NAMESPACE_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX),
            new Namespace(NAMESPACE_2, NAMESPACE_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX_2)));

        // Search the namespaces
        NamespaceSearchResponse resultNamespaceSearchResponse = namespaceService.searchNamespaces(namespaceSearchRequest, fields);

        // Validate the returned object.
        assertEquals(expected, resultNamespaceSearchResponse);
    }

    @Test
    public void testSearchNamespacesChargeCodeNoExists()
    {
        // Create and persist database entities required for testing
        createDatabaseEntitiesForNamespaceSearchTesting();

        NamespaceSearchRequest namespaceSearchRequest =
            new NamespaceSearchRequest(Arrays.asList(new NamespaceSearchFilter(Arrays.asList(new NamespaceSearchKey("I_DO_NOT_EXIST", false)))));

        Set<String> fields = Sets.newHashSet(CHARGE_CODE_FIELD, S3_KEY_PREFIX_FIELD);

        NamespaceSearchResponse expected = new NamespaceSearchResponse(new ArrayList<>());

        // Search the namespaces
        NamespaceSearchResponse resultNamespaceSearchResponse = namespaceService.searchNamespaces(namespaceSearchRequest, fields);

        // Validate the returned object
        assertEquals(expected, resultNamespaceSearchResponse);
    }

    @Test
    public void testSearchNamespacesNoChargeCode()
    {
        // Create and persist database entities required for testing
        createDatabaseEntitiesForNamespaceSearchTesting();

        // Case 1: null charge code and false flag
        NamespaceSearchRequest namespaceSearchRequest =
            new NamespaceSearchRequest(Arrays.asList(new NamespaceSearchFilter(Arrays.asList(new NamespaceSearchKey(NO_CHARGE_CODE, false)))));

        Set<String> fields = Sets.newHashSet(CHARGE_CODE_FIELD, S3_KEY_PREFIX_FIELD);

        NamespaceSearchResponse expected = new NamespaceSearchResponse(Arrays.asList(new Namespace(NAMESPACE, NAMESPACE_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX),
            new Namespace(NAMESPACE_2, NAMESPACE_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX_2),
            new Namespace(NAMESPACE_3, NAMESPACE_CHARGE_CODE_2, NAMESPACE_S3_KEY_PREFIX_3),
            new Namespace(NAMESPACE_4, NO_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX_4)));

        // Search the namespaces
        NamespaceSearchResponse resultNamespaceSearchResponse = namespaceService.searchNamespaces(namespaceSearchRequest, fields);

        // Validate the returned object
        assertEquals(expected, resultNamespaceSearchResponse);

        // Case 2: null charge code and true flag
        namespaceSearchRequest = new NamespaceSearchRequest(Arrays.asList(new NamespaceSearchFilter(Arrays.asList(new NamespaceSearchKey(NO_CHARGE_CODE, true)))));

        expected = new NamespaceSearchResponse(Arrays.asList(new Namespace(NAMESPACE, NAMESPACE_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX),
            new Namespace(NAMESPACE_2, NAMESPACE_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX_2),
            new Namespace(NAMESPACE_3, NAMESPACE_CHARGE_CODE_2, NAMESPACE_S3_KEY_PREFIX_3)));

        // Search the namespaces
        resultNamespaceSearchResponse = namespaceService.searchNamespaces(namespaceSearchRequest, fields);

        // Validate the returned object
        assertEquals(expected, resultNamespaceSearchResponse);
    }

    @Test
    public void testSearchNamespacesTrimParameters()
    {
        // Create and persist database entities required for testing
        createDatabaseEntitiesForNamespaceSearchTesting();

        // Create namespace search request by passing parameters padded with white space
        NamespaceSearchRequest namespaceSearchRequest = new NamespaceSearchRequest(
            Arrays.asList(new NamespaceSearchFilter(Arrays.asList(new NamespaceSearchKey(addWhitespace(NAMESPACE_CHARGE_CODE), false)))));

        Set<String> fields = Sets.newHashSet(CHARGE_CODE_FIELD, S3_KEY_PREFIX_FIELD);

        NamespaceSearchResponse expected = new NamespaceSearchResponse(Arrays.asList(new Namespace(NAMESPACE, NAMESPACE_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX),
            new Namespace(NAMESPACE_2, NAMESPACE_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX_2)));

        // Search the namespaces
        NamespaceSearchResponse resultNamespaceSearchResponse = namespaceService.searchNamespaces(namespaceSearchRequest, fields);

        // Validate the returned object
        assertEquals(expected, resultNamespaceSearchResponse);
    }

    @Test
    public void testSearchNamespacesMissingOptionalParametersSearchResponseFields()
    {
        // Create and persist database entities required for testing
        createDatabaseEntitiesForNamespaceSearchTesting();

        NamespaceSearchRequest namespaceSearchRequest =
            new NamespaceSearchRequest(Arrays.asList(new NamespaceSearchFilter(Arrays.asList(new NamespaceSearchKey(NAMESPACE_CHARGE_CODE, false)))));

        NamespaceSearchResponse expected = new NamespaceSearchResponse(Arrays
            .asList(new Namespace(NAMESPACE, NO_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX), new Namespace(NAMESPACE_2, NO_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX_2)));

        // Case1: null charge code field
        Set<String> fields = Sets.newHashSet(NO_CHARGE_CODE_FIELD, S3_KEY_PREFIX_FIELD);

        // Search namespaces without specifying optional parameters in the response fields except for the s3 key prefix
        NamespaceSearchResponse resultNamespaceSearchResponse = namespaceService.searchNamespaces(namespaceSearchRequest, fields);

        // Validate the returned object
        assertEquals(expected, resultNamespaceSearchResponse);

        // Case2: null s3 key prefix field
        fields = Sets.newHashSet(CHARGE_CODE_FIELD, NO_S3_KEY_PREFIX_FIELD);

        // Search namespaces without specifying optional parameters in the response fields except for the charge code
        resultNamespaceSearchResponse = namespaceService.searchNamespaces(namespaceSearchRequest, fields);

        expected = new NamespaceSearchResponse(Arrays.asList(new Namespace(NAMESPACE, NAMESPACE_CHARGE_CODE, NO_NAMESPACE_S3_KEY_PREFIX),
            new Namespace(NAMESPACE_2, NAMESPACE_CHARGE_CODE, NO_NAMESPACE_S3_KEY_PREFIX)));

        // Validate the returned object
        assertEquals(expected, resultNamespaceSearchResponse);
    }

    @Test
    public void testSearchNamespacesMissingOptionalParametersFilters()
    {
        // Create and persist database entities required for testing
        createDatabaseEntitiesForNamespaceSearchTesting();

        Set<String> fields = Sets.newHashSet(CHARGE_CODE_FIELD, S3_KEY_PREFIX_FIELD);

        // Search the namespaces without specifying an optional namespace search filter
        NamespaceSearchResponse resultNamespaceSearchResponse = namespaceService.searchNamespaces(new NamespaceSearchRequest(), fields);

        NamespaceSearchResponse expected = new NamespaceSearchResponse(new ArrayList<>());

        // Validate the returned object
        assertEquals(expected, resultNamespaceSearchResponse);

        // Search namespaces when an optional namespace search filter is set to null
        List<NamespaceSearchFilter> namespaceSearchFilters = new ArrayList<>();
        namespaceSearchFilters.add(null);
        NamespaceSearchRequest namespaceSearchRequest = new NamespaceSearchRequest(namespaceSearchFilters);

        resultNamespaceSearchResponse = namespaceService.searchNamespaces(namespaceSearchRequest, fields);

        // Validate the returned object
        assertEquals(expected, resultNamespaceSearchResponse);
    }

    @Test
    public void testSearchNamespacesInvalidParameters()
    {
        // Try to search namespaces when there are more than one namespace search filter is specified
        try
        {
            namespaceService.searchNamespaces(new NamespaceSearchRequest(Arrays.asList(new NamespaceSearchFilter(), new NamespaceSearchFilter())),
                NO_NAMESPACE_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("At most one namespace search filter must be specified.", e.getMessage());
        }

        // Try to search namespaces when there are more than one namespace search key is specified
        try
        {
            namespaceService.searchNamespaces(
                new NamespaceSearchRequest(Arrays.asList(new NamespaceSearchFilter(Arrays.asList(new NamespaceSearchKey(), new NamespaceSearchKey())))),
                NO_NAMESPACE_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exactly one namespace search key must be specified.", e.getMessage());
        }

        // Try to search namespaces using a un-supported search response field option
        try
        {
            NamespaceSearchRequest namespaceSearchRequest =
                new NamespaceSearchRequest(Arrays.asList(new NamespaceSearchFilter(Arrays.asList(new NamespaceSearchKey(NAMESPACE_CHARGE_CODE, false)))));
            namespaceService.searchNamespaces(namespaceSearchRequest, Sets.newHashSet("INVALID_FIELD_OPTION"));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Search response field \"invalid_field_option\" is not supported.", e.getMessage());
        }
    }

    @Test
    public void testSearchNamespacesMissingRequiredParameters()
    {
        // Try to search namespaces when namespace search request is not specified
        try
        {
            namespaceService.searchNamespaces(null, NO_NAMESPACE_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace search request must be specified.", e.getMessage());
        }

        // Try to search namespaces when namespace search key is not specified
        try
        {
            NamespaceSearchRequest namespaceSearchRequest = new NamespaceSearchRequest(Arrays.asList(new NamespaceSearchFilter()));
            namespaceService.searchNamespaces(namespaceSearchRequest, NO_SEARCH_RESPONSE_FIELDS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exactly one namespace search key must be specified.", e.getMessage());
        }
    }

    /**
     * Creates database entities required for the namespace search service unit tests.
     */
    private void createDatabaseEntitiesForNamespaceSearchTesting()
    {
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE, NAMESPACE_CHARGE_CODE);
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2, NAMESPACE_CHARGE_CODE);
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_3, NAMESPACE_CHARGE_CODE_2);
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_4, NO_CHARGE_CODE);
    }
}
