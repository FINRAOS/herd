package org.finra.herd.service.impl;

import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_ROLE;
import static org.finra.herd.service.AbstractServiceTest.ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.finra.herd.dao.SecurityRoleFunctionDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.SecurityFunctionKey;
import org.finra.herd.model.api.xml.SecurityRoleFunction;
import org.finra.herd.model.api.xml.SecurityRoleFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKeys;
import org.finra.herd.model.api.xml.SecurityRoleKey;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;
import org.finra.herd.service.helper.SecurityFunctionDaoHelper;
import org.finra.herd.service.helper.SecurityFunctionHelper;
import org.finra.herd.service.helper.SecurityRoleDaoHelper;
import org.finra.herd.service.helper.SecurityRoleFunctionDaoHelper;
import org.finra.herd.service.helper.SecurityRoleFunctionHelper;
import org.finra.herd.service.helper.SecurityRoleHelper;

public class SecurityRoleFunctionServiceImplTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private SecurityFunctionDaoHelper securityFunctionDaoHelper;

    @Mock
    private SecurityFunctionHelper securityFunctionHelper;

    @Mock
    private SecurityRoleDaoHelper securityRoleDaoHelper;

    @Mock
    private SecurityRoleFunctionDao securityRoleFunctionDao;

    @Mock
    private SecurityRoleFunctionDaoHelper securityRoleFunctionDaoHelper;

    @Mock
    private SecurityRoleFunctionHelper securityRoleFunctionHelper;

    @InjectMocks
    private SecurityRoleFunctionServiceImpl securityRoleFunctionService;

    @Mock
    private SecurityRoleHelper securityRoleHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSecurityRoleFunction()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Create a security role to function mapping create request.
        SecurityRoleFunctionCreateRequest securityRoleFunctionCreateRequest = new SecurityRoleFunctionCreateRequest(securityRoleFunctionKey);

        // Create a security role entity.
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();
        securityRoleEntity.setCode(SECURITY_ROLE);

        // Create a security function entity.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode(SECURITY_FUNCTION);

        // Mock the external calls.
        when(securityRoleFunctionDao.getSecurityRoleFunctionByKey(securityRoleFunctionKey)).thenReturn(null);
        when(securityRoleDaoHelper.getSecurityRoleEntity(SECURITY_ROLE)).thenReturn(securityRoleEntity);
        when(securityFunctionDaoHelper.getSecurityFunctionEntity(SECURITY_FUNCTION)).thenReturn(securityFunctionEntity);
        doAnswer(new Answer<Void>()
        {
            public Void answer(InvocationOnMock invocation)
            {
                // Get the security role to function mapping entity and set its primary key.
                SecurityRoleFunctionEntity securityRoleFunctionEntity = (SecurityRoleFunctionEntity) invocation.getArguments()[0];
                securityRoleFunctionEntity.setId(ID);
                return null;
            }
        }).when(securityRoleFunctionDao).saveAndRefresh(any(SecurityRoleFunctionEntity.class));

        // Call the method under test.
        SecurityRoleFunction result = securityRoleFunctionService.createSecurityRoleFunction(securityRoleFunctionCreateRequest);

        // Validate the results.
        assertEquals(new SecurityRoleFunction(result.getId(), securityRoleFunctionKey), result);

        // Verify the external calls.
        verify(securityRoleFunctionHelper).validateAndTrimSecurityRoleFunctionCreateRequest(securityRoleFunctionCreateRequest);
        verify(securityRoleFunctionDao).getSecurityRoleFunctionByKey(securityRoleFunctionKey);
        verify(securityRoleDaoHelper).getSecurityRoleEntity(SECURITY_ROLE);
        verify(securityFunctionDaoHelper).getSecurityFunctionEntity(SECURITY_FUNCTION);
        verify(securityRoleFunctionDao).saveAndRefresh(any(SecurityRoleFunctionEntity.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateSecurityRoleFunctionSecurityRoleFunctionAlreadyExists()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Create a security role to function mapping create request.
        SecurityRoleFunctionCreateRequest securityRoleFunctionCreateRequest = new SecurityRoleFunctionCreateRequest(securityRoleFunctionKey);

        // Create a security role to function mapping entity.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();

        // Mock the external calls.
        when(securityRoleFunctionDao.getSecurityRoleFunctionByKey(securityRoleFunctionKey)).thenReturn(securityRoleFunctionEntity);

        // Specify the expected exception.
        expectedException.expect(AlreadyExistsException.class);
        expectedException.expectMessage(String.format(
            "Unable to create security role to function mapping for \"%s\" security role name and \"%s\" security function name because it already exists.",
            SECURITY_ROLE, SECURITY_FUNCTION));

        // Call the method under test.
        securityRoleFunctionService.createSecurityRoleFunction(securityRoleFunctionCreateRequest);

        // Verify the external calls.
        verify(securityRoleFunctionHelper).validateAndTrimSecurityRoleFunctionCreateRequest(securityRoleFunctionCreateRequest);
        verify(securityRoleFunctionDao).getSecurityRoleFunctionByKey(securityRoleFunctionKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteSecurityRoleFunction()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Create a security role entity.
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();
        securityRoleEntity.setCode(SECURITY_ROLE);

        // Create a security function entity.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode(SECURITY_FUNCTION);

        // Create a security role to function mapping entity.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();
        securityRoleFunctionEntity.setId(ID);
        securityRoleFunctionEntity.setSecurityRole(securityRoleEntity);
        securityRoleFunctionEntity.setSecurityFunction(securityFunctionEntity);

        // Mock the external calls.
        when(securityRoleFunctionDaoHelper.getSecurityRoleFunctionEntity(securityRoleFunctionKey)).thenReturn(securityRoleFunctionEntity);

        // Call the method under test.
        SecurityRoleFunction result = securityRoleFunctionService.deleteSecurityRoleFunction(securityRoleFunctionKey);

        // Validate the results.
        assertEquals(new SecurityRoleFunction(ID, securityRoleFunctionKey), result);

        // Verify the external calls.
        verify(securityRoleFunctionHelper).validateAndTrimSecurityRoleFunctionKey(securityRoleFunctionKey);
        verify(securityRoleFunctionDaoHelper).getSecurityRoleFunctionEntity(securityRoleFunctionKey);
        verify(securityRoleFunctionDao).delete(securityRoleFunctionEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityRoleFunction()
    {
        // Create a security role to function mapping key.
        SecurityRoleFunctionKey securityRoleFunctionKey = new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION);

        // Create a security role entity.
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();
        securityRoleEntity.setCode(SECURITY_ROLE);

        // Create a security function entity.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode(SECURITY_FUNCTION);

        // Create a security role to function mapping entity.
        SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();
        securityRoleFunctionEntity.setId(ID);
        securityRoleFunctionEntity.setSecurityRole(securityRoleEntity);
        securityRoleFunctionEntity.setSecurityFunction(securityFunctionEntity);

        // Mock the external calls.
        when(securityRoleFunctionDaoHelper.getSecurityRoleFunctionEntity(securityRoleFunctionKey)).thenReturn(securityRoleFunctionEntity);

        // Call the method under test.
        SecurityRoleFunction result = securityRoleFunctionService.getSecurityRoleFunction(securityRoleFunctionKey);

        // Validate the results.
        assertEquals(new SecurityRoleFunction(ID, securityRoleFunctionKey), result);

        // Verify the external calls.
        verify(securityRoleFunctionHelper).validateAndTrimSecurityRoleFunctionKey(securityRoleFunctionKey);
        verify(securityRoleFunctionDaoHelper).getSecurityRoleFunctionEntity(securityRoleFunctionKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityRoleFunctions()
    {
        // Create a list of security role to function mapping keys.
        List<SecurityRoleFunctionKey> securityRoleFunctionKeys = Collections.singletonList(new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION));

        // Mock the external calls.
        when(securityRoleFunctionDao.getSecurityRoleFunctionKeys()).thenReturn(securityRoleFunctionKeys);

        // Call the method under test.
        SecurityRoleFunctionKeys result = securityRoleFunctionService.getSecurityRoleFunctions();

        // Validate the results.
        assertEquals(new SecurityRoleFunctionKeys(securityRoleFunctionKeys), result);

        // Verify the external calls.
        verify(securityRoleFunctionDao).getSecurityRoleFunctionKeys();
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityRoleFunctionsBySecurityFunction()
    {
        // Create a security function key.
        SecurityFunctionKey securityFunctionKey = new SecurityFunctionKey(SECURITY_FUNCTION);

        // Create a list of security role to function mapping keys.
        List<SecurityRoleFunctionKey> securityRoleFunctionKeys = Collections.singletonList(new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION));

        // Mock the external calls.
        when(securityRoleFunctionDao.getSecurityRoleFunctionKeysBySecurityFunction(SECURITY_FUNCTION)).thenReturn(securityRoleFunctionKeys);

        // Call the method under test.
        SecurityRoleFunctionKeys result = securityRoleFunctionService.getSecurityRoleFunctionsBySecurityFunction(securityFunctionKey);

        // Validate the results.
        assertEquals(new SecurityRoleFunctionKeys(securityRoleFunctionKeys), result);

        // Verify the external calls.
        verify(securityFunctionHelper).validateAndTrimSecurityFunctionKey(securityFunctionKey);
        verify(securityRoleFunctionDao).getSecurityRoleFunctionKeysBySecurityFunction(SECURITY_FUNCTION);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityRoleFunctionsBySecurityRole()
    {
        // Create a security role key.
        SecurityRoleKey securityRoleKey = new SecurityRoleKey(SECURITY_ROLE);

        // Create a list of security role to function mapping keys.
        List<SecurityRoleFunctionKey> securityRoleFunctionKeys = Collections.singletonList(new SecurityRoleFunctionKey(SECURITY_ROLE, SECURITY_FUNCTION));

        // Mock the external calls.
        when(securityRoleFunctionDao.getSecurityRoleFunctionKeysBySecurityRole(SECURITY_ROLE)).thenReturn(securityRoleFunctionKeys);

        // Call the method under test.
        SecurityRoleFunctionKeys result = securityRoleFunctionService.getSecurityRoleFunctionsBySecurityRole(securityRoleKey);

        // Validate the results.
        assertEquals(new SecurityRoleFunctionKeys(securityRoleFunctionKeys), result);

        // Verify the external calls.
        verify(securityRoleHelper).validateAndTrimSecurityRoleKey(securityRoleKey);
        verify(securityRoleFunctionDao).getSecurityRoleFunctionKeysBySecurityRole(SECURITY_ROLE);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(securityFunctionDaoHelper, securityFunctionHelper, securityRoleDaoHelper, securityRoleFunctionDao,
            securityRoleFunctionDaoHelper, securityRoleFunctionHelper, securityRoleHelper);
    }
}
