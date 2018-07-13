package org.finra.herd.service.helper;

import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION;
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

import org.finra.herd.dao.SecurityRoleFunctionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;

public class SecurityRoleFunctionDaoHelperTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private SecurityRoleFunctionDao securityRoleFunctionDao;

    @InjectMocks
    private SecurityRoleFunctionDaoHelper securityRoleFunctionDaoHelper = new SecurityRoleFunctionDaoHelper();

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetSecurityRoleFunctionEntity()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Create a security role to function mapping entity.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();

        // Mock the external calls.
        when(securityRoleFunctionDao.getSecurityRoleFunctionByKey(securityRoleFunctionKey)).thenReturn(securityRoleFunctionEntity);

        // Call the method under test.
        SecurityRoleFunctionEntity result = securityRoleFunctionDaoHelper.getSecurityRoleFunctionEntity(securityRoleFunctionKey);

        // Validate the results.
        assertEquals(securityRoleFunctionEntity, result);

        // Verify the external calls.
        verify(securityRoleFunctionDao).getSecurityRoleFunctionByKey(securityRoleFunctionKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityRoleFunctionEntitySecurityRoleFunctionNoExists()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Mock the external calls.
        when(securityRoleFunctionDao.getSecurityRoleFunctionByKey(securityRoleFunctionKey)).thenReturn(null);

        // Specify the expected exception.
        expectedException.expect(ObjectNotFoundException.class);
        expectedException.expectMessage(String
            .format("Security role to function mapping with \"%s\" security role name and \"%s\" security function name doesn't exist.", SECURITY_ROLE,
                SECURITY_FUNCTION));

        // Call the method under test.
        securityRoleFunctionDaoHelper.getSecurityRoleFunctionEntity(securityRoleFunctionKey);

        // Verify the external calls.
        verify(securityRoleFunctionDao).getSecurityRoleFunctionByKey(securityRoleFunctionKey);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(securityRoleFunctionDao);
    }
}
