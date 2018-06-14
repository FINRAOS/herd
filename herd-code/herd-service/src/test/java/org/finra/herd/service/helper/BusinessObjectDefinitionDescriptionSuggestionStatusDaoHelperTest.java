package org.finra.herd.service.helper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionStatusDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * The test for the business object definition description suggestion dao helper.
 */
public class BusinessObjectDefinitionDescriptionSuggestionStatusDaoHelperTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDefinitionDescriptionSuggestionStatusDao businessObjectDefinitionDescriptionSuggestionStatusDao;

    @InjectMocks
    private BusinessObjectDefinitionDescriptionSuggestionStatusDaoHelper businessObjectDefinitionDescriptionSuggestionStatusDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionStatusEntity()
    {
        // Create a business object definition description suggestion entity.
        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        businessObjectDefinitionDescriptionSuggestionStatusEntity.setCode(BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Setup mock interactions.
        when(businessObjectDefinitionDescriptionSuggestionStatusDao
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        // Call method under test.
        BusinessObjectDefinitionDescriptionSuggestionStatusEntity result = businessObjectDefinitionDescriptionSuggestionStatusDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Validate result.
        assertThat("Result does not equal businessObjectDefinitionDescriptionSuggestionStatusEntity.", result,
            is(equalTo(businessObjectDefinitionDescriptionSuggestionStatusEntity)));

        // Verify mocks interactions.
        verify(businessObjectDefinitionDescriptionSuggestionStatusDao)
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionStatusDao);
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionStatusEntityWithException()
    {
        // Setup mock interactions.
        when(businessObjectDefinitionDescriptionSuggestionStatusDao
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS)).thenReturn(null);

        try
        {
            // Call method under test.
            businessObjectDefinitionDescriptionSuggestionStatusDaoHelper
                .getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS);
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            // Validate exception message.
            assertThat("Exception message is not correct.", objectNotFoundException.getMessage(), is(equalTo(String
                .format("Business object definition description suggestion status with code \"%s\" doesn't exist.", BDEF_DESCRIPTION_SUGGESTION_STATUS))));
        }

        // Verify mocks interactions.
        verify(businessObjectDefinitionDescriptionSuggestionStatusDao)
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS);
        verifyNoMoreInteractions(businessObjectDefinitionDescriptionSuggestionStatusDao);
    }
}
