package org.finra.herd.service.impl;

import static org.finra.herd.dao.AbstractDaoTest.CREATED_BY;
import static org.finra.herd.dao.AbstractDaoTest.CREATED_ON;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION_2;
import static org.finra.herd.dao.AbstractDaoTest.SECURITY_FUNCTION_3;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
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
    private static final String SECURITY_FUNCTION_NAME_WITH_EXTRA_SPACES = SECURITY_FUNCTION + "    ";

    private static final SecurityFunctionCreateRequest SECURITY_FUNCTION_CREATE_REQUEST = new SecurityFunctionCreateRequest()
    {{
        setSecurityFunctionName(SECURITY_FUNCTION);
    }};

    private static final SecurityFunctionCreateRequest SECURITY_FUNCTION_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME = new SecurityFunctionCreateRequest()
    {{
        setSecurityFunctionName(SECURITY_FUNCTION_NAME_WITH_EXTRA_SPACES);
    }};

    private static final SecurityFunctionKey SECURITY_FUNCTION_KEY = new SecurityFunctionKey()
    {{
        setSecurityFunctionName(SECURITY_FUNCTION);
    }};

    private static final SecurityFunctionKey SECURITY_FUNCTION_KEY_WITH_EXTRA_SPACES_IN_NAME = new SecurityFunctionKey()
    {{
        setSecurityFunctionName(SECURITY_FUNCTION_NAME_WITH_EXTRA_SPACES);
    }};

    private static final SecurityFunctionEntity SECURITY_FUNCTION_ENTITY = new SecurityFunctionEntity()
    {{
        setCode(SECURITY_FUNCTION);
        setCreatedBy(CREATED_BY);
        setUpdatedBy(CREATED_BY);
        setCreatedOn(new Timestamp(CREATED_ON.getMillisecond()));
    }};

    private static final List<String> ALL_SECURITY_FUNCTION_NAMES = Arrays.asList(SECURITY_FUNCTION, SECURITY_FUNCTION_2, SECURITY_FUNCTION_3);

    @InjectMocks
    private SecurityFunctionServiceImpl securityFunctionService;

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private SecurityFunctionDao securityFunctionDao;

    @Mock
    private SecurityFunctionDaoHelper securityFunctionDaoHelper;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateSecurityFunction()
    {

        when(securityFunctionDao.getSecurityFunctionByName(SECURITY_FUNCTION)).thenReturn(null);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(SECURITY_FUNCTION);
        when(securityFunctionDao.saveAndRefresh(any(SecurityFunctionEntity.class))).thenReturn(SECURITY_FUNCTION_ENTITY);

        SecurityFunction securityFunction = securityFunctionService.createSecurityFunction(SECURITY_FUNCTION_CREATE_REQUEST);
        assertEquals(SECURITY_FUNCTION, securityFunction.getSecurityFunctionName());

        verify(alternateKeyHelper).validateStringParameter("security function name", SECURITY_FUNCTION);
        verify(securityFunctionDao).getSecurityFunctionByName(SECURITY_FUNCTION);
        verify(securityFunctionDao).saveAndRefresh(any(SecurityFunctionEntity.class));

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateSecurityFunctionAlreadyExists()
    {
        expectedException.expect(AlreadyExistsException.class);
        expectedException.expectMessage(String.format("Unable to create security function \"%s\" because it already exists.", SECURITY_FUNCTION));

        when(securityFunctionDao.getSecurityFunctionByName(SECURITY_FUNCTION)).thenReturn(SECURITY_FUNCTION_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(SECURITY_FUNCTION);
        securityFunctionService.createSecurityFunction(SECURITY_FUNCTION_CREATE_REQUEST);
    }

    @Test
    public void testGetSecurityFunction()
    {
        when(securityFunctionDaoHelper.getSecurityFunctionEntity(SECURITY_FUNCTION)).thenReturn(SECURITY_FUNCTION_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(SECURITY_FUNCTION);

        SecurityFunction securityFunction = securityFunctionService.getSecurityFunction(SECURITY_FUNCTION_KEY);
        assertEquals(SECURITY_FUNCTION, securityFunction.getSecurityFunctionName());
        verify(alternateKeyHelper).validateStringParameter("security function name", SECURITY_FUNCTION_KEY.getSecurityFunctionName());
        verify(securityFunctionDaoHelper).getSecurityFunctionEntity(SECURITY_FUNCTION);

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityFunctionNullKey()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A security function key must be specified");

        securityFunctionService.getSecurityFunction(null);
    }


    @Test
    public void testDeleteSecurityFunction()
    {
        when(securityFunctionDaoHelper.getSecurityFunctionEntity(SECURITY_FUNCTION)).thenReturn(SECURITY_FUNCTION_ENTITY);
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(SECURITY_FUNCTION);

        SecurityFunction securityFunction = securityFunctionService.deleteSecurityFunction(SECURITY_FUNCTION_KEY);
        assertEquals(SECURITY_FUNCTION, securityFunction.getSecurityFunctionName());
        verify(alternateKeyHelper).validateStringParameter("security function name", SECURITY_FUNCTION);
        verify(securityFunctionDaoHelper).getSecurityFunctionEntity(SECURITY_FUNCTION);
        verify(securityFunctionDao).delete(SECURITY_FUNCTION_ENTITY);

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteSecurityFunctionNullKey()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A security function key must be specified");

        securityFunctionService.deleteSecurityFunction(null);
    }

    @Test
    public void testGetSecurityFunctions()
    {
        when(securityFunctionDao.getSecurityFunctions()).thenReturn(ALL_SECURITY_FUNCTION_NAMES);

        SecurityFunctionKeys securityFunctionKeys = securityFunctionService.getSecurityFunctions();

        assertNotNull(securityFunctionKeys);
        List<SecurityFunctionKey> securityFunctionKeyList = securityFunctionKeys.getSecurityFunctionKeys();
        assertEquals(ALL_SECURITY_FUNCTION_NAMES.size(), securityFunctionKeyList.size());

        // verify the order is reserved
        assertEquals(SECURITY_FUNCTION, securityFunctionKeyList.get(0).getSecurityFunctionName());
        assertEquals(SECURITY_FUNCTION_2, securityFunctionKeyList.get(1).getSecurityFunctionName());
        assertEquals(SECURITY_FUNCTION_3, securityFunctionKeyList.get(2).getSecurityFunctionName());

        verify(securityFunctionDao).getSecurityFunctions();

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetSecurityFunctionsEmptyList()
    {
        when(securityFunctionDao.getSecurityFunctions()).thenReturn(Collections.emptyList());
        SecurityFunctionKeys securityFunctionKeys = securityFunctionService.getSecurityFunctions();

        assertNotNull(securityFunctionKeys);
        assertEquals(0, securityFunctionKeys.getSecurityFunctionKeys().size());

        verify(securityFunctionDao).getSecurityFunctions();

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateSecurityFunctionCreateRequestExtraSpaces()
    {
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(SECURITY_FUNCTION);

        assertEquals(SECURITY_FUNCTION_NAME_WITH_EXTRA_SPACES, SECURITY_FUNCTION_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME.getSecurityFunctionName());
        securityFunctionService.validateSecurityFunctionCreateRequest(SECURITY_FUNCTION_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME);
        // White space should be trimmed now
        assertEquals(SECURITY_FUNCTION, SECURITY_FUNCTION_CREATE_REQUEST_WITH_EXTRA_SPACES_IN_NAME.getSecurityFunctionName());
    }

    @Test
    public void testValidateAndTrimSecurityFunctionKeyExtraSpaces()
    {
        when(alternateKeyHelper.validateStringParameter(anyString(), anyString())).thenReturn(SECURITY_FUNCTION);

        assertEquals(SECURITY_FUNCTION_NAME_WITH_EXTRA_SPACES, SECURITY_FUNCTION_KEY_WITH_EXTRA_SPACES_IN_NAME.getSecurityFunctionName());
        securityFunctionService.validateAndTrimSecurityFunctionKey(SECURITY_FUNCTION_KEY_WITH_EXTRA_SPACES_IN_NAME);
        // White space should be trimmed now
        assertEquals(SECURITY_FUNCTION, SECURITY_FUNCTION_KEY_WITH_EXTRA_SPACES_IN_NAME.getSecurityFunctionName());
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(securityFunctionDaoHelper, alternateKeyHelper, securityFunctionDao);
    }
}
