package org.finra.herd.service.helper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionDao;
import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionStatusDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * The test for the business object definition description suggestion dao helper.
 */
public class BusinessObjectDefinitionDescriptionSuggestionDaoHelperTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Mock
    private BusinessObjectDefinitionDescriptionSuggestionDao businessObjectDefinitionDescriptionSuggestionDao;

    @Mock
    private BusinessObjectDefinitionDescriptionSuggestionStatusDao businessObjectDefinitionDescriptionSuggestionStatusDao;

    @InjectMocks
    private BusinessObjectDefinitionDescriptionSuggestionDaoHelper businessObjectDefinitionDescriptionSuggestionDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionEntity()
    {
        // Setup
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = mock(BusinessObjectDefinitionEntity.class);

        // Create a business object definition description suggestion entity.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(USER_ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);

        // Setup mock interactions.
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity, USER_ID))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionEntity);

        // Call method under test.
        BusinessObjectDefinitionDescriptionSuggestionEntity result = businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity, USER_ID);

        // Validate result.
        assertThat("Result does not equal businessObjectDefinitionDescriptionSuggestionEntity.", result,
            is(equalTo(businessObjectDefinitionDescriptionSuggestionEntity)));

        // Verify mocks interactions.
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity, USER_ID);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionDao);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionEntityWithException()
    {
        // Setup
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setName(BDEF_NAME);
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);

        // Setup mock interactions.
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity, USER_ID)).thenReturn(null);

        try
        {
            // Call method under test.
            businessObjectDefinitionDescriptionSuggestionDaoHelper
                .getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity, USER_ID);
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            // Validate exception message.
            assertThat("Exception message is not correct.", objectNotFoundException.getMessage(), is(equalTo(String.format(
                "Business object definition description suggestion with the parameters " +
                    " {namespace=\"%s\", businessObjectDefinitionName=\"%s\", userId=\"%s\"} does not exist.",
                businessObjectDefinitionEntity.getNamespace().getCode(), businessObjectDefinitionEntity.getName(), USER_ID))));
        }

        // Verify mocks interactions.
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity, USER_ID);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionDao);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus()
    {
        // Create the relative entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity.getCode(), BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            businessObjectDefinitionDescriptionSuggestionStatusDaoTestHelper
                .createBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Create new business object definition description suggestion entities.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(USER_ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity2 =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity2.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity2.setDescriptionSuggestion(DESCRIPTION_SUGGESTION_2);
        businessObjectDefinitionDescriptionSuggestionEntity2.setUserId(USER_ID_2);
        businessObjectDefinitionDescriptionSuggestionEntity2.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        List<BusinessObjectDefinitionDescriptionSuggestionEntity> businessObjectDefinitionDescriptionSuggestionEntities =
            Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionEntity, businessObjectDefinitionDescriptionSuggestionEntity2);

        // Setup mock interactions.
        when(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionStatusDao
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionStatusEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionStatusEntity)).thenReturn(businessObjectDefinitionDescriptionSuggestionEntities);

        // Call method under test.
        List<BusinessObjectDefinitionDescriptionSuggestionEntity> result = businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionKey,
                BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Validate results.
        assertThat("Result size does not equal businessObjectDefinitionDescriptionSuggestionEntities.", result.size(),
            is(equalTo(businessObjectDefinitionDescriptionSuggestionEntities.size())));

        for (int i = 0; i < result.size(); i++)
        {
            assertThat("Result is not equal businessObjectDefinitionDescriptionSuggestionEntity.", result.get(i),
                is(equalTo(businessObjectDefinitionDescriptionSuggestionEntities.get(i))));
        }

        // Verify mocks interactions.
        verify(businessObjectDefinitionDao).getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionStatusDao)
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS);
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionStatusEntity);
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionDescriptionSuggestionDao,
            businessObjectDefinitionDescriptionSuggestionStatusDao);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatusWithEmptyStatus()
    {
        // Create the relative entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity.getCode(), BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            businessObjectDefinitionDescriptionSuggestionStatusDaoTestHelper
                .createBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Create new business object definition description suggestion entities.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(USER_ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity2 =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity2.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity2.setDescriptionSuggestion(DESCRIPTION_SUGGESTION_2);
        businessObjectDefinitionDescriptionSuggestionEntity2.setUserId(USER_ID_2);
        businessObjectDefinitionDescriptionSuggestionEntity2.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        List<BusinessObjectDefinitionDescriptionSuggestionEntity> businessObjectDefinitionDescriptionSuggestionEntities =
            Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionEntity, businessObjectDefinitionDescriptionSuggestionEntity2);

        // Setup mock interactions.
        when(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity, null))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionEntities);

        // Call method under test.
        List<BusinessObjectDefinitionDescriptionSuggestionEntity> result = businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionKey, "");

        // Validate results.
        assertThat("Result size does not equal businessObjectDefinitionDescriptionSuggestionEntities.", result.size(),
            is(equalTo(businessObjectDefinitionDescriptionSuggestionEntities.size())));

        for (int i = 0; i < result.size(); i++)
        {
            assertThat("Result is not equal businessObjectDefinitionDescriptionSuggestionEntity.", result.get(i),
                is(equalTo(businessObjectDefinitionDescriptionSuggestionEntities.get(i))));
        }

        // Verify mocks interactions.
        verify(businessObjectDefinitionDao).getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity, null);
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionDescriptionSuggestionDao,
            businessObjectDefinitionDescriptionSuggestionStatusDao);

        // Reset the mocks
        reset(businessObjectDefinitionDao);
        reset(businessObjectDefinitionDescriptionSuggestionDao);

        // Setup mock interactions.
        when(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity, null))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionEntities);

        // Call method under test.
        result = businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionKey, "  ");

        // Validate results.
        assertThat("Result size does not equal businessObjectDefinitionDescriptionSuggestionEntities.", result.size(),
            is(equalTo(businessObjectDefinitionDescriptionSuggestionEntities.size())));

        for (int i = 0; i < result.size(); i++)
        {
            assertThat("Result is not equal businessObjectDefinitionDescriptionSuggestionEntity.", result.get(i),
                is(equalTo(businessObjectDefinitionDescriptionSuggestionEntities.get(i))));
        }

        // Verify mocks interactions.
        verify(businessObjectDefinitionDao).getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity, null);
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionDescriptionSuggestionDao,
            businessObjectDefinitionDescriptionSuggestionStatusDao);
    }


    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatusWithNullStatus()
    {
        // Create the relative entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity.getCode(), BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            businessObjectDefinitionDescriptionSuggestionStatusDaoTestHelper
                .createBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Create new business object definition description suggestion entities.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(USER_ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity2 =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity2.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity2.setDescriptionSuggestion(DESCRIPTION_SUGGESTION_2);
        businessObjectDefinitionDescriptionSuggestionEntity2.setUserId(USER_ID_2);
        businessObjectDefinitionDescriptionSuggestionEntity2.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        List<BusinessObjectDefinitionDescriptionSuggestionEntity> businessObjectDefinitionDescriptionSuggestionEntities =
            Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionEntity, businessObjectDefinitionDescriptionSuggestionEntity2);

        // Setup mock interactions.
        when(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity, null))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionEntities);

        // Call method under test.
        List<BusinessObjectDefinitionDescriptionSuggestionEntity> result = businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionKey, null);

        // Validate results.
        assertThat("Result size does not equal businessObjectDefinitionDescriptionSuggestionEntities.", result.size(),
            is(equalTo(businessObjectDefinitionDescriptionSuggestionEntities.size())));

        for (int i = 0; i < result.size(); i++)
        {
            assertThat("Result is not equal businessObjectDefinitionDescriptionSuggestionEntity.", result.get(i),
                is(equalTo(businessObjectDefinitionDescriptionSuggestionEntities.get(i))));
        }

        // Verify mocks interactions.
        verify(businessObjectDefinitionDao).getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity, null);
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionDescriptionSuggestionDao,
            businessObjectDefinitionDescriptionSuggestionStatusDao);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatusWithEmptyResultList()
    {
        // Create the relative entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity.getCode(), BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            businessObjectDefinitionDescriptionSuggestionStatusDaoTestHelper
                .createBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Setup mock interactions.
        when(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionStatusDao
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionStatusEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionStatusEntity)).thenReturn(null);

        // Call method under test.
        List<BusinessObjectDefinitionDescriptionSuggestionEntity> result = businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionKey,
                BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Validate results.
        assertThat("Result size does not equal zero.", result.size(), is(equalTo(0)));

        // Verify mocks interactions.
        verify(businessObjectDefinitionDao).getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionStatusDao)
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS);
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionStatusEntity);
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionDescriptionSuggestionDao,
            businessObjectDefinitionDescriptionSuggestionStatusDao);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatusWithNullBusinessObjectDefinition()
    {
        // Create the relative entities.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        // Setup mock interactions.
        when(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey)).thenReturn(null);

        // Call method under test.
        List<BusinessObjectDefinitionDescriptionSuggestionEntity> result = businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionKey,
                BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Validate results.
        assertThat("Result size does not equal zero.", result.size(), is(equalTo(0)));

        // Verify mocks interactions.
        verify(businessObjectDefinitionDao).getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionDescriptionSuggestionDao,
            businessObjectDefinitionDescriptionSuggestionStatusDao);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatusWithNoStatusEntityFound()
    {
        // Create the relative entities.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity.getCode(), BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        // Setup mock interactions.
        when(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionStatusDao
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS)).thenReturn(null);

        // Call method under test.
        List<BusinessObjectDefinitionDescriptionSuggestionEntity> result = businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionKey,
                BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Validate results.
        assertThat("Result size does not equal zero.", result.size(), is(equalTo(0)));

        // Verify mocks interactions.
        verify(businessObjectDefinitionDao).getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionStatusDao)
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS);
        verifyNoMoreInteractions(businessObjectDefinitionDao, businessObjectDefinitionDescriptionSuggestionDao,
            businessObjectDefinitionDescriptionSuggestionStatusDao);
    }
}
