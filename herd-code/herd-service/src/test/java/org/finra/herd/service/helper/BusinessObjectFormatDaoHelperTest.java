package org.finra.herd.service.helper;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the BusinessObjectFormatDaoHelper class.
 */
public class BusinessObjectFormatDaoHelperTest extends AbstractServiceTest
{
    @Test
    public void testCheckBusinessObjectFormatContainsPublishableAttributeDefinitions()
    {
        // Create the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create attribute definition entity with publish flag not set (null).
        BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity1 = new BusinessObjectDataAttributeDefinitionEntity();
        businessObjectDataAttributeDefinitionEntity1.setPublish(null);

        // Create attribute definition entity with publish flag to false.
        BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity2 = new BusinessObjectDataAttributeDefinitionEntity();
        businessObjectDataAttributeDefinitionEntity2.setPublish(false);

        // Create attribute definition entity with publish flag to true.
        BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity3 = new BusinessObjectDataAttributeDefinitionEntity();
        businessObjectDataAttributeDefinitionEntity3.setPublish(true);

        // Add attribute definition entities to the business object format entity with the publishable attribute definition entity going last.
        businessObjectFormatEntity.setAttributeDefinitions(Arrays
            .asList(businessObjectDataAttributeDefinitionEntity1, businessObjectDataAttributeDefinitionEntity2, businessObjectDataAttributeDefinitionEntity3));

        // Validate the result.
        assertTrue(businessObjectFormatDaoHelper.checkBusinessObjectFormatContainsPublishableAttributeDefinitions(businessObjectFormatEntity));

        // Create a business object format entity without any attribute definitions.
        businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Validate the result.
        assertFalse(businessObjectFormatDaoHelper.checkBusinessObjectFormatContainsPublishableAttributeDefinitions(businessObjectFormatEntity));

    }

    @Test
    public void testIsBusinessObjectDataPublishedAttributesChangeEventNotificationRequired()
    {
        // Create the business object format entity with the feature flag not set (null).
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setEnableBusinessObjectDataPublishedAttributesChangeEventNotification(null);

        // Call the method under test with this format.
        assertFalse(businessObjectFormatDaoHelper.isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity));

        // Create the business object format entity with the feature flag set to false.
        businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setEnableBusinessObjectDataPublishedAttributesChangeEventNotification(false);

        // Call the method under test with this format.
        assertFalse(businessObjectFormatDaoHelper.isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity));

        // Create the business object format entity with the feature flag set to true and without any attribute definitions.
        businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setEnableBusinessObjectDataPublishedAttributesChangeEventNotification(true);

        // Call the method under test with this format.
        assertFalse(businessObjectFormatDaoHelper.isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity));

        // Create the business object format entity with the feature flag set to true and with publishable attribute definition.
        businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setEnableBusinessObjectDataPublishedAttributesChangeEventNotification(true);
        BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
        businessObjectDataAttributeDefinitionEntity.setPublish(true);
        businessObjectFormatEntity.setAttributeDefinitions(Collections.singletonList(businessObjectDataAttributeDefinitionEntity));

        // Call the method under test with this format.
        assertTrue(businessObjectFormatDaoHelper.isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(businessObjectFormatEntity));
    }
}
