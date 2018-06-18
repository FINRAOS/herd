package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.SecurityFunctionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * unit test for class {@link SecurityFunctionDaoHelper}
 */
public class SecurityFunctionDaoHelperTest extends AbstractServiceTest
{
    private static final SecurityFunctionEntity SECURITY_FUNCTION_ENTITY = new SecurityFunctionEntity()
    {{
        setCode(SECURITY_FUNCTION);
    }};

    @InjectMocks
    private SecurityFunctionDaoHelper securityFunctionDaoHelper = new SecurityFunctionDaoHelper();

    @Mock
    private SecurityFunctionDao securityFunctionDao;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetSecurityFunctionEntity()
    {
        when(securityFunctionDao.getSecurityFunctionByName(SECURITY_FUNCTION)).thenReturn(SECURITY_FUNCTION_ENTITY);
        SecurityFunctionEntity securityFunctionEntity = securityFunctionDaoHelper.getSecurityFunctionEntityByName(SECURITY_FUNCTION);
        assertEquals(SECURITY_FUNCTION, securityFunctionEntity.getCode());
        verify(securityFunctionDao, times(1)).getSecurityFunctionByName(SECURITY_FUNCTION);
    }

    @Test
    public void testGetSecurityFunctionEntityNonExistent()
    {
        expectedException.expect(ObjectNotFoundException.class);
        expectedException.expectMessage("Security function with name \"" + SECURITY_FUNCTION + "\" doesn't exist.");

        when(securityFunctionDao.getSecurityFunctionByName(SECURITY_FUNCTION)).thenReturn(null);
        securityFunctionDaoHelper.getSecurityFunctionEntityByName(SECURITY_FUNCTION);
    }
}
