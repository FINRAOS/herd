package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.GlobalAttributeDefinitionDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests the functionality of global attribute definition dao helper.
 */
public class GlobalAttributeDefinitionDaoHelperTest extends AbstractServiceTest
{
    @InjectMocks
    private GlobalAttributeDefinitionDaoHelper globalAttributeDefinitionDaoHelper;

    @Mock
    private GlobalAttributeDefinitionDao globalAttributeDefinitionDao;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetGlobalAttributeDefinitionEntity()
    {
        // Create a global attribute definition key.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Create a global attribute definition entity.
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Mock calls to external methods.
        when(globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinitionEntity);

        // Call the method under test.
        GlobalAttributeDefinitionEntity response = globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey);

        // Verify the external calls.
        verify(globalAttributeDefinitionDao).getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey);
        verifyNoMoreInteractions(globalAttributeDefinitionDao);

        // Validate the response.
        assertEquals(globalAttributeDefinitionEntity, response);
    }

    @Test
    public void testGetGlobalAttributeDefinitionEntityEntityNoExists()
    {
        // Create a global attribute definition key.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Mock calls to external methods.
        when(globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey)).thenReturn(null);

        // Try to call the method under test.
        try
        {
            globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Global attribute definition with level \"%s\" doesn't exist for global attribute definition name \"%s\".",
                globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel(), globalAttributeDefinitionKey.getGlobalAttributeDefinitionName()),
                e.getMessage());
        }

        // Verify the external calls.
        verify(globalAttributeDefinitionDao).getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey);
        verifyNoMoreInteractions(globalAttributeDefinitionDao);
    }

    @Test
    public void testValidateGlobalAttributeDefinitionNoExistsEntityExists()
    {
        // Create a global attribute definition key.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Create a global attribute definition entity.
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity = new GlobalAttributeDefinitionEntity();

        // Mock calls to external methods.
        when(globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinitionEntity);

        // Try to call the method under test.
        try
        {
            globalAttributeDefinitionDaoHelper.validateGlobalAttributeDefinitionNoExists(globalAttributeDefinitionKey);
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create global attribute definition with global attribute definition level \"%s\" " +
                "and global attribute definition name \"%s\" because it already exists.", globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel(),
                globalAttributeDefinitionKey.getGlobalAttributeDefinitionName()), e.getMessage());
        }

        // Verify the external calls.
        verify(globalAttributeDefinitionDao).getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey);
        verifyNoMoreInteractions(globalAttributeDefinitionDao);
    }

    @Test
    public void testValidateGlobalAttributeDefinitionNoExists()
    {
        // Create a global attribute definition key.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Mock calls to external methods.
        when(globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey)).thenReturn(null);

        // Call the method under test.
        globalAttributeDefinitionDaoHelper.validateGlobalAttributeDefinitionNoExists(globalAttributeDefinitionKey);

        // Verify the external calls.
        verify(globalAttributeDefinitionDao).getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey);
        verifyNoMoreInteractions(globalAttributeDefinitionDao);
    }
}
