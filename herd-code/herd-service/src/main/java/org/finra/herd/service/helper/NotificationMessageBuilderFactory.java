package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The factory class for various notification message builders
 */
@Component
public class NotificationMessageBuilderFactory
{
    @Autowired
    private BusinessObjectDataStatusChangeMessageBuilder businessObjectDataStatusChangeMessageBuilder;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilder businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder;

    @Autowired
    private BusinessObjectFormatVersionChangeMessageBuilder businessObjectFormatVersionChangeMessageBuilder;

    @Autowired
    private StorageUnitStatusChangeMessageBuilder storageUnitStatusChangeMessageBuilder;

    @Autowired
    private UserNamespaceAuthorizationChangeMessageBuilder userNamespaceAuthorizationChangeMessageBuilder;

    @Autowired
    private SystemMonitorResponseMessageBuilder systemMonitorResponseMessageBuilder;

    /**
     * Get the builder for Business ObjectData Status Change Message
     *
     * @return the builder for Business ObjectData Status Change Message
     */
    public BusinessObjectDataStatusChangeMessageBuilder getBusinessObjectDataStatusChangeMessageBuilder()
    {
        return businessObjectDataStatusChangeMessageBuilder;
    }

    /**
     * Get the builder for Business Object Definition Description Suggestion Change Message
     *
     * @return the builder for Business Object Definition Description Suggestion Change Message
     */
    public BusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilder getBusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilder()
    {
        return businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder;
    }

    /**
     * Get the builder for Business Object Format Version Change Message
     *
     * @return the builder for Business Object Format Version Change Message
     */
    public BusinessObjectFormatVersionChangeMessageBuilder getBusinessObjectFormatVersionChangeMessageBuilder()
    {
        return businessObjectFormatVersionChangeMessageBuilder;
    }

    /**
     * Get the builder for Storage Unit Status Change Message
     *
     * @return the builder for Storage Unit Status Change Message
     */
    public StorageUnitStatusChangeMessageBuilder getStorageUnitStatusChangeMessageBuilder()
    {
        return storageUnitStatusChangeMessageBuilder;
    }

    /**
     * Get the builder for User Namespace Authorization Change Message
     *
     * @return the builder for User Namespace Authorization Change Message
     */
    public UserNamespaceAuthorizationChangeMessageBuilder getUserNamespaceAuthorizationChangeMessageBuilder()
    {
        return userNamespaceAuthorizationChangeMessageBuilder;
    }

    /**
     * Get the builder for System Monitor Response Message
     *
     * @return the builder for System Monitor Response Message
     */
    public SystemMonitorResponseMessageBuilder getSystemMonitorResponseMessageBuilder()
    {
        return systemMonitorResponseMessageBuilder;
    }
}
