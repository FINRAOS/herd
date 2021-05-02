package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorization;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationKey;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationKeys;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationUpdateRequest;
import org.finra.herd.service.NamespaceIamRoleAuthorizationService;

/**
 * Tests for NamespaceIamRoleAuthorizationRestController
 */
public class NamespaceIamRoleAuthorizationRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private NamespaceIamRoleAuthorizationRestController namespaceIamRoleAuthorizationRestController;

    @Mock
    private NamespaceIamRoleAuthorizationService namespaceIamRoleAuthorizationService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Asserts that createNamespaceIamRoleAuthorization() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void testCreateNamespaceIamRoleAuthorization()
    {
        // Create the necessary objects for testing.
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME);
        NamespaceIamRoleAuthorization expectedResult = new NamespaceIamRoleAuthorization(ID, namespaceIamRoleAuthorizationKey, IAM_ROLE_DESCRIPTION);
        NamespaceIamRoleAuthorizationCreateRequest namespaceIamRoleAuthorizationCreateRequest =
            new NamespaceIamRoleAuthorizationCreateRequest(namespaceIamRoleAuthorizationKey, IAM_ROLE_DESCRIPTION);

        // Setup the interactions.
        when(namespaceIamRoleAuthorizationService.createNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationCreateRequest)).thenReturn(expectedResult);

        // Call the method being tested.
        NamespaceIamRoleAuthorization actualResult =
            namespaceIamRoleAuthorizationRestController.createNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationCreateRequest);

        // Verify the interactions.
        verify(namespaceIamRoleAuthorizationService).createNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationCreateRequest);
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);

        // Validate the results.
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Asserts that deleteNamespaceIamRoleAuthorization() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void testDeleteNamespaceIamRoleAuthorization()
    {
        // Create the necessary objects for testing.
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME);
        NamespaceIamRoleAuthorization expectedResult = new NamespaceIamRoleAuthorization(ID, namespaceIamRoleAuthorizationKey, IAM_ROLE_DESCRIPTION);

        // Setup the interactions.
        when(namespaceIamRoleAuthorizationService.deleteNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey)).thenReturn(expectedResult);

        // Call the method being tested.
        NamespaceIamRoleAuthorization actualResult = namespaceIamRoleAuthorizationRestController.deleteNamespaceIamRoleAuthorization(NAMESPACE, IAM_ROLE_NAME);

        // Verify the interactions.
        verify(namespaceIamRoleAuthorizationService).deleteNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey);
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);

        // Validate the results.
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Asserts that getNamespaceIamRoleAuthorization() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void testGetNamespaceIamRoleAuthorization()
    {
        // Create the necessary objects for testing.
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME);
        NamespaceIamRoleAuthorization expectedResult = new NamespaceIamRoleAuthorization(ID, namespaceIamRoleAuthorizationKey, IAM_ROLE_DESCRIPTION);

        // Setup the interactions.
        when(namespaceIamRoleAuthorizationService.getNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey)).thenReturn(expectedResult);

        // Call the method being tested.
        NamespaceIamRoleAuthorization actualResult = namespaceIamRoleAuthorizationRestController.getNamespaceIamRoleAuthorization(NAMESPACE, IAM_ROLE_NAME);

        // Verify the interactions.
        verify(namespaceIamRoleAuthorizationService).getNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey);
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);

        // Validate the results.
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Asserts that getNamespaceIamRoleAuthorizations() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void testGetNamespaceIamRoleAuthorizations()
    {
        // Create the necessary objects for testing.
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey1 = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME);
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey2 = new NamespaceIamRoleAuthorizationKey(NAMESPACE_2, IAM_ROLE_NAME_2);

        List<NamespaceIamRoleAuthorizationKey> namespaceIamRoleAuthorizationKeyList = new ArrayList<>();
        namespaceIamRoleAuthorizationKeyList.add(namespaceIamRoleAuthorizationKey1);
        namespaceIamRoleAuthorizationKeyList.add(namespaceIamRoleAuthorizationKey2);

        NamespaceIamRoleAuthorizationKeys expectedResult = new NamespaceIamRoleAuthorizationKeys(namespaceIamRoleAuthorizationKeyList);

        // Setup the interactions.
        when(namespaceIamRoleAuthorizationService.getNamespaceIamRoleAuthorizations()).thenReturn(expectedResult);

        // Call the method being tested.
        NamespaceIamRoleAuthorizationKeys actualResult = namespaceIamRoleAuthorizationRestController.getNamespaceIamRoleAuthorizations();

        // Verify the interactions.
        verify(namespaceIamRoleAuthorizationService).getNamespaceIamRoleAuthorizations();
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);

        // Validate the results.
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Asserts that getNamespaceIamRoleAuthorizationsByIamRoleName() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void testGetNamespaceIamRoleAuthorizationsByIamRoleName()
    {
        // Create the necessary objects for testing.
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey1 = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME);
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey2 = new NamespaceIamRoleAuthorizationKey(NAMESPACE_2, IAM_ROLE_NAME);

        List<NamespaceIamRoleAuthorizationKey> namespaceIamRoleAuthorizationKeyList = new ArrayList<>();
        namespaceIamRoleAuthorizationKeyList.add(namespaceIamRoleAuthorizationKey1);
        namespaceIamRoleAuthorizationKeyList.add(namespaceIamRoleAuthorizationKey2);

        NamespaceIamRoleAuthorizationKeys expectedResult = new NamespaceIamRoleAuthorizationKeys(namespaceIamRoleAuthorizationKeyList);

        // Setup the interactions.
        when(namespaceIamRoleAuthorizationService.getNamespaceIamRoleAuthorizationsByIamRoleName(IAM_ROLE_NAME)).thenReturn(expectedResult);

        // Call the method being tested.
        NamespaceIamRoleAuthorizationKeys actualResult = namespaceIamRoleAuthorizationRestController.getNamespaceIamRoleAuthorizationsByIamRoleName(IAM_ROLE_NAME);

        // Verify the interactions.
        verify(namespaceIamRoleAuthorizationService).getNamespaceIamRoleAuthorizationsByIamRoleName(IAM_ROLE_NAME);
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);

        // Validate the results.
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Asserts that getNamespaceIamRoleAuthorizationsByNamespace() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void testGetNamespaceIamRoleAuthorizationsByNamespace()
    {
        // Create the necessary objects for testing.
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey1 = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME);
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey2 = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME_2);

        List<NamespaceIamRoleAuthorizationKey> namespaceIamRoleAuthorizationKeyList = new ArrayList<>();
        namespaceIamRoleAuthorizationKeyList.add(namespaceIamRoleAuthorizationKey1);
        namespaceIamRoleAuthorizationKeyList.add(namespaceIamRoleAuthorizationKey2);

        NamespaceIamRoleAuthorizationKeys expectedResult = new NamespaceIamRoleAuthorizationKeys(namespaceIamRoleAuthorizationKeyList);

        // Setup the interactions.
        when(namespaceIamRoleAuthorizationService.getNamespaceIamRoleAuthorizationsByNamespace(NAMESPACE)).thenReturn(expectedResult);

        // Call the method being tested.
        NamespaceIamRoleAuthorizationKeys actualResult = namespaceIamRoleAuthorizationRestController.getNamespaceIamRoleAuthorizationsByNamespace(NAMESPACE);

        // Verify the interactions.
        verify(namespaceIamRoleAuthorizationService).getNamespaceIamRoleAuthorizationsByNamespace(NAMESPACE);
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);

        // Validate the results.
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Asserts that updateNamespaceIamRoleAuthorization() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void testUpdateNamespaceIamRoleAuthorization()
    {
        // Create the necessary objects for testing.
        NamespaceIamRoleAuthorizationKey namespaceIamRoleAuthorizationKey = new NamespaceIamRoleAuthorizationKey(NAMESPACE, IAM_ROLE_NAME);
        NamespaceIamRoleAuthorization expectedResult = new NamespaceIamRoleAuthorization(ID, namespaceIamRoleAuthorizationKey, IAM_ROLE_DESCRIPTION);
        NamespaceIamRoleAuthorizationUpdateRequest namespaceIamRoleAuthorizationUpdateRequest =
            new NamespaceIamRoleAuthorizationUpdateRequest(IAM_ROLE_DESCRIPTION);

        // Setup the interactions.
        when(namespaceIamRoleAuthorizationService
            .updateNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey, namespaceIamRoleAuthorizationUpdateRequest)).thenReturn(expectedResult);

        // Call the method being tested.
        NamespaceIamRoleAuthorization actualResult = namespaceIamRoleAuthorizationRestController
            .updateNamespaceIamRoleAuthorization(NAMESPACE, IAM_ROLE_NAME, namespaceIamRoleAuthorizationUpdateRequest);

        // Verify the interactions.
        verify(namespaceIamRoleAuthorizationService)
            .updateNamespaceIamRoleAuthorization(namespaceIamRoleAuthorizationKey, namespaceIamRoleAuthorizationUpdateRequest);
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);

        // Validate the results.
        assertEquals(expectedResult, actualResult);
    }
}
