package org.finra.herd.service.helper;

import static org.finra.herd.dao.AbstractDaoTest.SECURITY_ROLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.SecurityRoleDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.SecurityRoleEntity;

public class SecurityRoleDaoHelperTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private SecurityRoleDao securityRoleDao;

    @InjectMocks
    private SecurityRoleDaoHelper securityRoleDaoHelper = new SecurityRoleDaoHelper();

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetSecurityRoleEntity()
    {
        // Create a security role entity.
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();

        // Mock the external calls.
        when(securityRoleDao.getSecurityRoleByName(SECURITY_ROLE)).thenReturn(securityRoleEntity);

        // Call the method under test.
        SecurityRoleEntity result = securityRoleDaoHelper.getSecurityRoleEntity(SECURITY_ROLE);

        // Validate the results.
        assertEquals(securityRoleEntity, result);

        // Verify the external calls.
        verify(securityRoleDao).getSecurityRoleByName(SECURITY_ROLE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityRoleEntitySecurityRoleNoExists()
    {
        // Specify the expected exception.
        expectedException.expect(ObjectNotFoundException.class);
        expectedException.expectMessage(String.format("Security role with name \"%s\" doesn't exist.", SECURITY_ROLE));

        // Mock the external calls.
        when(securityRoleDao.getSecurityRoleByName(SECURITY_ROLE)).thenReturn(null);

        // Call the method under test.
        securityRoleDaoHelper.getSecurityRoleEntity(SECURITY_ROLE);

        // Verify the external calls.
        verify(securityRoleDao).getSecurityRoleByName(SECURITY_ROLE);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(securityRoleDao);
    }
}
