package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
 * This class tests the functionality of global attribute definition dao helper
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

        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);

        //Mock calls to external methods
        when(globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinitionEntity);

        //Call the method to test
        globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey);

        //verify the interactions
        verify(globalAttributeDefinitionDao).getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey);
        verifyNoMoreInteractions(globalAttributeDefinitionDao);

        //validate
        assertNotNull(globalAttributeDefinitionEntity != null);
    }

    @Test
    public void testGetGlobalAttributeDefinitionEntityIsNull()
    {

        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        try
        {
            //Mock calls to external methods
            when(globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey)).thenReturn(null);
            //call method under test
            globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Global Attribute Definition with level \"%s\" doesn't exist for global attribute definition name \"%s\".",
                globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel(), globalAttributeDefinitionKey.getGlobalAttributeDefinitionName()),
                e.getMessage());
        }
    }

    @Test
    public void testCheckGlobalAttributeDefinitionEntityNotExists()
    {

        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);

        try
        {
            //Mock calls to external methods
            when(globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinitionEntity);
            //call method under test
            globalAttributeDefinitionDaoHelper.checkGlobalAttributeDefinitionExists(globalAttributeDefinitionKey);
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format(
                "Unable to create global attribute definition with global attribute definition level \"%s\" and global attribute definition name \"%s\" because it already exists.",
                globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel(), globalAttributeDefinitionKey.getGlobalAttributeDefinitionName()),
                e.getMessage());
        }
    }

    @Test
    public void testCheckGlobalAttributeDefinitionEntityExists()
    {

        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        //Mock calls to external methods
        when(globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey)).thenReturn(null);
        //call method under test
        globalAttributeDefinitionDaoHelper.checkGlobalAttributeDefinitionExists(globalAttributeDefinitionKey);
        //verify the interactions
        verify(globalAttributeDefinitionDao).getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey);
    }

}
