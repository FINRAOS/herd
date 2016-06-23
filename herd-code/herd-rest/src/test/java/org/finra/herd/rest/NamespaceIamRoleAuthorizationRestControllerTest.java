package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorization;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationCreateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizationUpdateRequest;
import org.finra.herd.model.api.xml.NamespaceIamRoleAuthorizations;
import org.finra.herd.service.NamespaceIamRoleAuthorizationService;

/**
 * Tests for NamespaceIamRoleAuthorizationRestController
 */
public class NamespaceIamRoleAuthorizationRestControllerTest
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
    public void createNamespaceIamRoleAuthorizationAssertCallsService()
    {
        NamespaceIamRoleAuthorizationCreateRequest expectedRequest = new NamespaceIamRoleAuthorizationCreateRequest();
        NamespaceIamRoleAuthorization expectedResult = new NamespaceIamRoleAuthorization();
        when(namespaceIamRoleAuthorizationService.createNamespaceIamRoleAuthorization(any())).thenReturn(expectedResult);
        NamespaceIamRoleAuthorization actualResult = namespaceIamRoleAuthorizationRestController.createNamespaceIamRoleAuthorization(expectedRequest);
        verify(namespaceIamRoleAuthorizationService).createNamespaceIamRoleAuthorization(expectedRequest);
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Asserts that getNamespaceIamRoleAuthorization() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void getNamespaceIamRoleAuthorizationAssertCallsService()
    {
        String expectedNamespace = "namespace";
        NamespaceIamRoleAuthorization expectedResult = new NamespaceIamRoleAuthorization();
        when(namespaceIamRoleAuthorizationService.getNamespaceIamRoleAuthorization(any())).thenReturn(expectedResult);
        NamespaceIamRoleAuthorization actualResult = namespaceIamRoleAuthorizationRestController.getNamespaceIamRoleAuthorization(expectedNamespace);
        verify(namespaceIamRoleAuthorizationService).getNamespaceIamRoleAuthorization(expectedNamespace);
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Asserts that getNamespaceIamRoleAuthorizations() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void getNamespaceIamRoleAuthorizationsAssertCallsService()
    {
        NamespaceIamRoleAuthorizations expectedResult = new NamespaceIamRoleAuthorizations();
        when(namespaceIamRoleAuthorizationService.getNamespaceIamRoleAuthorizations()).thenReturn(expectedResult);
        NamespaceIamRoleAuthorizations actualResult = namespaceIamRoleAuthorizationRestController.getNamespaceIamRoleAuthorizations();
        verify(namespaceIamRoleAuthorizationService).getNamespaceIamRoleAuthorizations();
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Asserts that updateNamespaceIamRoleAuthorization() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void updateNamespaceIamRoleAuthorizationAssertCallsService()
    {
        String expectedNamespace = "namespace";
        NamespaceIamRoleAuthorizationUpdateRequest expectedRequest = new NamespaceIamRoleAuthorizationUpdateRequest();
        NamespaceIamRoleAuthorization expectedResult = new NamespaceIamRoleAuthorization();
        when(namespaceIamRoleAuthorizationService.updateNamespaceIamRoleAuthorization(any(), any())).thenReturn(expectedResult);
        NamespaceIamRoleAuthorization actualResult = namespaceIamRoleAuthorizationRestController.updateNamespaceIamRoleAuthorization(expectedNamespace,
            expectedRequest);
        verify(namespaceIamRoleAuthorizationService).updateNamespaceIamRoleAuthorization(expectedNamespace, expectedRequest);
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);
        assertEquals(expectedResult, actualResult);
    }

    /**
     * Asserts that deleteNamespaceIamRoleAuthorization() calls service with correct parameters, and returns whatever the service returns.
     */
    @Test
    public void deleteNamespaceIamRoleAuthorizationAssertCallsService()
    {
        String expectedNamespace = "namespace";
        NamespaceIamRoleAuthorization expectedResult = new NamespaceIamRoleAuthorization();
        when(namespaceIamRoleAuthorizationService.deleteNamespaceIamRoleAuthorization(any())).thenReturn(expectedResult);
        NamespaceIamRoleAuthorization actualResult = namespaceIamRoleAuthorizationRestController.deleteNamespaceIamRoleAuthorization(expectedNamespace);
        verify(namespaceIamRoleAuthorizationService).deleteNamespaceIamRoleAuthorization(expectedNamespace);
        verifyNoMoreInteractions(namespaceIamRoleAuthorizationService);
        assertEquals(expectedResult, actualResult);
    }
}
