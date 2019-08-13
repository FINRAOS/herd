package org.finra.herd.tools.access.validator;

import java.util.Map;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;

public class JsonMessage
{
    private Map<String, String> attributes;
    private BusinessObjectDataKey businessObjectDataKey;
    private String eventDate;

    public Map<String, String> getAttributes()
    {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes)
    {
        this.attributes = attributes;
    }

    public BusinessObjectDataKey getBusinessObjectDataKey()
    {
        return businessObjectDataKey;
    }

    public void setBusinessObjectDataKey(BusinessObjectDataKey businessObjectDataKey)
    {
        this.businessObjectDataKey = businessObjectDataKey;
    }

    public String getEventDate()
    {
        return eventDate;
    }

    public void setEventDate(String eventDate)
    {
        this.eventDate = eventDate;
    }

    @Override
    public String toString()
    {
        return "JsonMessage{" + "attributes=" + attributes + ", businessObjectDataKey=" + businessObjectDataKey + ", eventDate='" + eventDate + '\'' + '}';
    }
}
