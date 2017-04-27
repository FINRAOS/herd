package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.GlobalAttributeDefinitionDao;
import org.finra.herd.dao.GlobalAttributeDefinitionLevelDao;
import org.finra.herd.dao.GlobalAttributeDefinitionLevelDaoTestHelper;
import org.finra.herd.model.api.xml.GlobalAttributeDefinition;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionCreateRequest;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKeys;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionLevelEntity;
import org.finra.herd.service.GlobalAttributeDefinitionService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.GlobalAttributeDefinitionDaoHelper;
import org.finra.herd.service.helper.GlobalAttributeDefinitionHelper;

/**
 * Created by k26425 on 4/26/2017.
 */
public class GlobalAttributeDefinitionRestControllerTest extends AbstractRestTest
{
    @Mock
    private GlobalAttributeDefinitionService globalAttributeDefinitionService;

    @InjectMocks
    private GlobalAttributeDefinitionRestController globalAttributeDefinitionRestController;

    @Mock
    private GlobalAttributeDefinitionDao globalAttributeDefinitionDao;

    @Mock
    private GlobalAttributeDefinitionLevelDao globalAttributeDefinitionLevelDao;

    @Mock
    private GlobalAttributeDefinitionHelper globalAttributeDefinitionHelper;

    @Mock
    private GlobalAttributeDefinitionDaoHelper globalAttributeDefinitionDaoHelper;

    @Mock
    private GlobalAttributeDefinitionLevelDaoTestHelper globalAttributeDefinitionLevelDaoTestHelper;

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateGlobalAttributeDefinition()
    {

        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        GlobalAttributeDefinitionCreateRequest request = new GlobalAttributeDefinitionCreateRequest(globalAttributeDefinitionKey);

        //create a test global attribute definition entity
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);

        GlobalAttributeDefinitionLevelEntity globalAttributeDefinitionLevelEntity =
            globalAttributeDefinitionLevelDaoTestHelper.createGlobalAttributeDefinitionLevelEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL);

        //create a global attribute definition
        GlobalAttributeDefinition globalAttributeDefinition = new GlobalAttributeDefinition();
        globalAttributeDefinition.setId(1);
        globalAttributeDefinition.setGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);

        when(alternateKeyHelper.validateStringParameter("global attribute definition level", GLOBAL_ATTRIBUTE_DEFINITON_LEVEL))
            .thenReturn(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL);
        when(alternateKeyHelper.validateStringParameter("global attribute definition name", GLOBAL_ATTRIBUTE_DEFINITON_NAME_1))
            .thenReturn(GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        when(globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey)).thenReturn(null);
        when(globalAttributeDefinitionLevelDao.getGlobalAttributeDefinitionLevel(globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel()))
            .thenReturn(globalAttributeDefinitionLevelEntity);
        when(globalAttributeDefinitionDao.saveAndRefresh(any(GlobalAttributeDefinitionEntity.class))).thenReturn(globalAttributeDefinitionEntity);
        when(globalAttributeDefinitionService.createGlobalAttributeDefinition(request)).thenReturn(globalAttributeDefinition);

        //call method under  test
        GlobalAttributeDefinition response = globalAttributeDefinitionRestController.createGlobalAttributeDefinition(request);

        assertEquals(new GlobalAttributeDefinition(response.getId(), globalAttributeDefinitionKey), response);
    }

    @Test
    public void testDeleteGlobalAttributeDefinition()
    {
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);

        //create a test global attribute definition entity
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);

        //create a global attribute definition
        GlobalAttributeDefinition globalAttributeDefinition = new GlobalAttributeDefinition();
        globalAttributeDefinition.setId(1);
        globalAttributeDefinition.setGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);

        when(globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinitionEntity);
        when(globalAttributeDefinitionService.deleteGlobalAttributeDefinition(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinition);

        GlobalAttributeDefinition response = globalAttributeDefinitionRestController
            .deleteGlobalAttributeDefinition(globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel(),
                globalAttributeDefinitionKey.getGlobalAttributeDefinitionName());
        assertEquals(new GlobalAttributeDefinition(response.getId(), globalAttributeDefinitionKey), response);
    }

    @Test
    public void testGetGlobalAttributeDefinition()
    {
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey1 =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_2);

        GlobalAttributeDefinitionKeys globalAttributeDefinitionKeys =
            new GlobalAttributeDefinitionKeys(Arrays.asList(globalAttributeDefinitionKey, globalAttributeDefinitionKey1));

        when(globalAttributeDefinitionDao.getAllGlobalAttributeDefinitionKeys())
            .thenReturn(Arrays.asList(globalAttributeDefinitionKey, globalAttributeDefinitionKey1));
        when(globalAttributeDefinitionService.getGlobalAttributeDefinitionKeys()).thenReturn(globalAttributeDefinitionKeys);

        GlobalAttributeDefinitionKeys response = globalAttributeDefinitionRestController.getGlobalAttributeDefinitions();

        assertNotNull(response);
        assertEquals(response.getGlobalAttributeDefinitionKeies(), Arrays.asList(globalAttributeDefinitionKey, globalAttributeDefinitionKey1));
    }

}
