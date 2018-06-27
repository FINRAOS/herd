
import static org.finra.herd.dao.AbstractDaoTest.DESCRIPTION;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_ROLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.SecurityRole;
import org.finra.herd.model.api.xml.SecurityRoleCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.api.xml.SecurityRoleKeys;
import org.finra.herd.model.api.xml.SecurityRoleUpdateRequest;
import org.finra.herd.rest.SecurityRoleRestController;
import org.finra.herd.service.SecurityRoleService;

public class SecurityRoleRestControllerTest
{
    @InjectMocks
    private SecurityRoleRestController securityRoleRestController;

    @Mock
    private SecurityRoleService securityRoleService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSecurityRole()
    {
        // Create a security role create request.
        SecurityRoleCreateRequest securityRoleCreateRequest = new SecurityRoleCreateRequest(SECURITY_ROLE, DESCRIPTION);

        // Create a security role.
        SecurityRole securityRole = new SecurityRole(SECURITY_ROLE, DESCRIPTION);

        // Mock the external calls.
        when(securityRoleService.createSecurityRole(securityRoleCreateRequest)).thenReturn(securityRole);

        // Call the method under test.
        SecurityRole result = securityRoleRestController.createSecurityRole(securityRoleCreateRequest);

        // Verify the external calls.
        verify(securityRoleService).createSecurityRole(securityRoleCreateRequest);
        verifyNoMoreInteractions(securityRoleService);

        // Validate the result.
        assertEquals(securityRole, result);
    }

    @Test
    public void testDeleteSecurityRole()
    {
        // Create a security role.
        SecurityRole securityRole = new SecurityRole(SECURITY_ROLE, DESCRIPTION);

        // Mock the external calls.
        when(securityRoleService.deleteSecurityRole(new SecurityRoleKey(SECURITY_ROLE))).thenReturn(securityRole);

        // Call the method under test.
        SecurityRole result = securityRoleRestController.deleteSecurityRole(SECURITY_ROLE);

        // Verify the external calls.
        verify(securityRoleService).deleteSecurityRole(new SecurityRoleKey(SECURITY_ROLE));
        verifyNoMoreInteractions(securityRoleService);

        // Validate the result.
        assertEquals(securityRole, result);
    }

    @Test
    public void testGetSecurityRole()
    {
        // Create a security role key.
        SecurityRoleKey securityRoleKey = new SecurityRoleKey(SECURITY_ROLE);

        // Create a security role.
        SecurityRole securityRole = new SecurityRole(SECURITY_ROLE, DESCRIPTION);

        // Mock the external calls.
        when(securityRoleService.getSecurityRole(securityRoleKey)).thenReturn(securityRole);

        // Call the method under test.
        SecurityRole result = securityRoleRestController.getSecurityRole(SECURITY_ROLE);

        // Verify the external calls.
        verify(securityRoleService).getSecurityRole(new SecurityRoleKey(SECURITY_ROLE));
        verifyNoMoreInteractions(securityRoleService);

        // Validate the result.
        assertEquals(securityRole, result);
    }

    @Test
    public void testGetSecurityRoles()
    {
        // Create security role keys.
        SecurityRoleKeys securityRoleKeys = new SecurityRoleKeys(Lists.newArrayList(new SecurityRoleKey(SECURITY_ROLE)));

        // Mock the external calls.
        when(securityRoleService.getSecurityRoles()).thenReturn(securityRoleKeys);

        // Retrieve a list of security role keys.
        SecurityRoleKeys result = securityRoleRestController.getSecurityRoles();

        // Verify the external calls.
        verify(securityRoleService).getSecurityRoles();
        verifyNoMoreInteractions(securityRoleService);

        // Validate the result.
        assertEquals(securityRoleKeys, result);
    }

    @Test
    public void testUpdateSecurityRole()
    {
        // Create a security role update request.
        SecurityRoleUpdateRequest securityRoleUpdateRequest = new SecurityRoleUpdateRequest(DESCRIPTION);

        // Create a security role key.
        SecurityRoleKey securityRoleKey = new SecurityRoleKey(SECURITY_ROLE);

        // Create a security role.
        SecurityRole securityRole = new SecurityRole(SECURITY_ROLE, DESCRIPTION);

        // Mock the external calls.
        when(securityRoleService.updateSecurityRole(securityRoleKey, securityRoleUpdateRequest)).thenReturn(securityRole);

        // Call the method under test.
        SecurityRole result = securityRoleRestController.updateSecurityRole(SECURITY_ROLE,securityRoleUpdateRequest);

        // Verify the external calls.
        verify(securityRoleService).updateSecurityRole(securityRoleKey, securityRoleUpdateRequest);
        verifyNoMoreInteractions(securityRoleService);

        // Validate the result.
        assertEquals(securityRole, result);
    }
}
