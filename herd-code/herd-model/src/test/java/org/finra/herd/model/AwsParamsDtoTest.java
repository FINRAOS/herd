package org.finra.herd.model;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.finra.herd.model.dto.AwsParamsDto;

public class AwsParamsDtoTest
{
    @Test
    public void testEquals() throws Exception
    {
        AwsParamsDto dto1 = new AwsParamsDto();
        dto1.setHttpProxyHost("localhost1");
        dto1.setHttpProxyPort(8080);
        
        AwsParamsDto dto2 = new AwsParamsDto();
        dto2.setHttpProxyHost("localhost2");
        dto2.setHttpProxyPort(8080);
        
        AwsParamsDto dto3 = new AwsParamsDto();
        dto3.setHttpProxyHost("localhost1");
        dto3.setHttpProxyPort(8080);
        
        assertTrue(!dto1.equals(dto2));
        assertTrue(dto1.equals(dto3));
    }
    
    @Test
    public void testHashCode() throws Exception
    {
        AwsParamsDto dto1 = new AwsParamsDto();
        dto1.setHttpProxyHost("localhost1");
        dto1.setHttpProxyPort(8080);
        
        AwsParamsDto dto2 = new AwsParamsDto();
        dto2.setHttpProxyHost("localhost2");
        dto2.setHttpProxyPort(8080);
        
        AwsParamsDto dto3 = new AwsParamsDto();
        dto3.setHttpProxyHost("localhost1");
        dto3.setHttpProxyPort(8080);
        
        assertTrue(dto1.hashCode() != dto2.hashCode());
        assertTrue(dto1.hashCode() == dto3.hashCode());
    }
}
