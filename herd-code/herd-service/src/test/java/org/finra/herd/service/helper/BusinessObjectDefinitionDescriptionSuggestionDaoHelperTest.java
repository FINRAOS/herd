package org.finra.herd.service.helper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

/**
 * The test for the business object definition description suggestion dao helper.
 */
public class BusinessObjectDefinitionDescriptionSuggestionDaoHelperTest
{
    @Mock
    private BusinessObjectDefinitionDescriptionSuggestionDao businessObjectDefinitionDescriptionSuggestionDao;

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
        String userId = "userId";

        // Create a business object definition description suggestion entity.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(userId);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion("DESCRIPTION SUGGESTION");

        // Setup mock interactions.
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(businessObjectDefinitionEntity, userId))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionEntity);

        // Call method under test.
        BusinessObjectDefinitionDescriptionSuggestionEntity result = businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity, userId);

        // Validate result.
        assertThat("Result does not equal businessObjectDefinitionDescriptionSuggestionEntity.", result,
            is(equalTo(businessObjectDefinitionDescriptionSuggestionEntity)));

        // Verify mocks interactions.
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(businessObjectDefinitionEntity, userId);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionDao);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionEntityWithException()
    {
        // Setup
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode("NAMESPACE_CODE");
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setName("BDEF_NAME");
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        String userId = "userId";

        // Setup mock interactions.
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(businessObjectDefinitionEntity, userId))
            .thenReturn(null);

        try
        {
            // Call method under test.
            businessObjectDefinitionDescriptionSuggestionDaoHelper
                .getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity, userId);
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            // Validate exception message.
            assertThat("Exception message is not correct.", objectNotFoundException.getMessage(), is(equalTo(String.format(
                "Business object definition description suggestion with the parameters " +
                    " {namespace=\"%s\", businessObjectDefinitionName=\"%s\", userId=\"%s\"} does not exist.",
                businessObjectDefinitionEntity.getNamespace().getCode(), businessObjectDefinitionEntity.getName(), userId))));
        }

        // Verify mocks interactions.
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionEntityAndUserId(businessObjectDefinitionEntity, userId);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionDao);
    }
}
