package org.finra.herd.service.impl;

import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION_2;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION_3;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.SecurityFunctionDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.SecurityFunction;
import org.finra.herd.model.api.xml.SecurityFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityFunctionKey;
import org.finra.herd.model.api.xml.SecurityFunctionKeys;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.SecurityFunctionDaoHelper;

/**
 * This class tests functionality within the security function service implementation.
 */
public class SecurityFunctionServiceImplTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private SecurityFunctionDao securityFunctionDao;

    @Mock
    private SecurityFunctionDaoHelper securityFunctionDaoHelper;

    @InjectMocks
    private SecurityFunctionServiceImpl securityFunctionService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSecurityFunction()
    {
        // Create security function create request.
        SecurityFunctionCreateRequest securityFunctionCreateRequest = new SecurityFunctionCreateRequest(SECURITY_FUNCTION);

        // Create security function entity.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode(SECURITY_FUNCTION_2);

        // Create security function.
        SecurityFunction securityFunction = new SecurityFunction(SECURITY_FUNCTION_2);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("security function name", SECURITY_FUNCTION)).thenReturn(SECURITY_FUNCTION_2);
        when(securityFunctionDao.getSecurityFunctionByName(SECURITY_FUNCTION_2)).thenReturn(null);
        when(securityFunctionDao.saveAndRefresh(any(SecurityFunctionEntity.class))).thenReturn(securityFunctionEntity);

        // Call the method under test.
        SecurityFunction result = securityFunctionService.createSecurityFunction(securityFunctionCreateRequest);

        // Validate the result.
        assertEquals(securityFunction, result);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("security function name", SECURITY_FUNCTION);
        verify(securityFunctionDao).getSecurityFunctionByName(SECURITY_FUNCTION_2);
        verify(securityFunctionDao).saveAndRefresh(any(SecurityFunctionEntity.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateSecurityFunctionAlreadyExists()
    {
        // Create security function create request.
        SecurityFunctionCreateRequest securityFunctionCreateRequest = new SecurityFunctionCreateRequest(SECURITY_FUNCTION);

        // Create security function entity.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode(SECURITY_FUNCTION_3);

        // Specify the expected exception.
        expectedException.expect(AlreadyExistsException.class);
        expectedException.expectMessage(String.format("Unable to create security function \"%s\" because it already exists.", SECURITY_FUNCTION_2));

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("security function name", SECURITY_FUNCTION)).thenReturn(SECURITY_FUNCTION_2);
        when(securityFunctionDao.getSecurityFunctionByName(SECURITY_FUNCTION_2)).thenReturn(securityFunctionEntity);

        // Call the method under test.
        securityFunctionService.createSecurityFunction(securityFunctionCreateRequest);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("security function name", SECURITY_FUNCTION);
        verify(securityFunctionDao).getSecurityFunctionByName(SECURITY_FUNCTION_2);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateSecurityFunctionNameContainsUnprintableCharacters()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A security function name must contain only ASCII printable characters.");

        // Call the method under test.
        securityFunctionService.createSecurityFunction(new SecurityFunctionCreateRequest(SECURITY_FUNCTION + "\u0000"));

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateSecurityFunctionNoRequest()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A security function create request must be specified.");

        // Call the method under test.
        securityFunctionService.createSecurityFunction(null);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteSecurityFunction()
    {
        // Create security function key.
        SecurityFunctionKey securityFunctionKey = new SecurityFunctionKey(SECURITY_FUNCTION);

        // Create security function entity.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode(SECURITY_FUNCTION_3);

        // Create security function.
        SecurityFunction securityFunction = new SecurityFunction(SECURITY_FUNCTION_3);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("security function name", SECURITY_FUNCTION)).thenReturn(SECURITY_FUNCTION_2);
        when(securityFunctionDaoHelper.getSecurityFunctionEntity(SECURITY_FUNCTION_2)).thenReturn(securityFunctionEntity);

        // Call the method under test.
        SecurityFunction result = securityFunctionService.deleteSecurityFunction(securityFunctionKey);

        // Validate the result.
        assertEquals(securityFunction, result);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("security function name", SECURITY_FUNCTION);
        verify(securityFunctionDaoHelper).getSecurityFunctionEntity(SECURITY_FUNCTION_2);
        verify(securityFunctionDao).delete(securityFunctionEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteSecurityFunctionNoKey()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A security function key must be specified");

        // Call the method under test.
        securityFunctionService.deleteSecurityFunction(null);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityFunction()
    {
        // Create security function key.
        SecurityFunctionKey securityFunctionKey = new SecurityFunctionKey(SECURITY_FUNCTION);

        // Create security function entity.
        SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
        securityFunctionEntity.setCode(SECURITY_FUNCTION_3);

        // Create security function.
        SecurityFunction securityFunction = new SecurityFunction(SECURITY_FUNCTION_3);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("security function name", SECURITY_FUNCTION)).thenReturn(SECURITY_FUNCTION_2);
        when(securityFunctionDaoHelper.getSecurityFunctionEntity(SECURITY_FUNCTION_2)).thenReturn(securityFunctionEntity);

        // Call the method under test.
        SecurityFunction result = securityFunctionService.getSecurityFunction(securityFunctionKey);

        // Validate the result.
        assertEquals(securityFunction, result);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("security function name", SECURITY_FUNCTION);
        verify(securityFunctionDaoHelper).getSecurityFunctionEntity(SECURITY_FUNCTION_2);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityFunctionNoKey()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A security function key must be specified");

        // Call the method under test.
        securityFunctionService.getSecurityFunction(null);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityFunctions()
    {
        // Create a list of security function names in reverse order.
        List<String> securityFunctionNames = Arrays.asList(SECURITY_FUNCTION_3, SECURITY_FUNCTION_2, SECURITY_FUNCTION);

        // Create a list of security function keys in the same order and the list of names.
        SecurityFunctionKeys securityFunctionKeys = new SecurityFunctionKeys(Arrays
            .asList(new SecurityFunctionKey(SECURITY_FUNCTION_3), new SecurityFunctionKey(SECURITY_FUNCTION_2), new SecurityFunctionKey(SECURITY_FUNCTION)));

        // Mock the external calls.
        when(securityFunctionDao.getSecurityFunctions()).thenReturn(securityFunctionNames);

        // Call the method under test.
        SecurityFunctionKeys results = securityFunctionService.getSecurityFunctions();

        // Validate the result.
        assertEquals(securityFunctionKeys, results);

        // Verify the external calls.
        verify(securityFunctionDao).getSecurityFunctions();
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityFunctionsEmptyList()
    {
        // Mock the external calls.
        when(securityFunctionDao.getSecurityFunctions()).thenReturn(Collections.emptyList());

        // Call the method under test.
        SecurityFunctionKeys results = securityFunctionService.getSecurityFunctions();

        // Validate the result.
        assertEquals(new SecurityFunctionKeys(), results);

        // Verify the external calls.
        verify(securityFunctionDao).getSecurityFunctions();
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(securityFunctionDaoHelper, alternateKeyHelper, securityFunctionDao);
    }
}
