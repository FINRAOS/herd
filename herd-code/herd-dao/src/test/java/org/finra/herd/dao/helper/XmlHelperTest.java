/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.finra.herd.dao.helper;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.model.api.xml.BuildInformation;

/**
 * This class tests functionality within the XmlHelper class.
 */
public class XmlHelperTest extends AbstractDaoTest
{
    @Autowired
    private XmlHelper xmlHelper;

    @Test
    public void testObjectToXml() throws Exception
    {
        assertEquals(getTestXml(), xmlHelper.objectToXml(getTestBuildInformation()));
    }

    @Test
    public void testObjectToXmlFormatted() throws Exception
    {
        assertEquals(getTestXml().replaceAll("\\s+",""), xmlHelper.objectToXml(getTestBuildInformation(), true).replaceAll("\\s+",""));
    }

    @Test
    public void testUnmarshallXmlToObject() throws Exception
    {
        assertEquals(getTestBuildInformation(), xmlHelper.unmarshallXmlToObject(BuildInformation.class, getTestXml()));
    }

    private BuildInformation getTestBuildInformation()
    {
        return new BuildInformation(STRING_VALUE, STRING_VALUE, STRING_VALUE, STRING_VALUE);
    }

    private String getTestXml()
    {
        return String.format("<?xml version=\"1.0\" encoding=\"UTF-8\"?><buildInformation><buildOs>%s</buildOs><buildUser>%s</buildUser>" +
            "<buildDate>%s</buildDate><buildNumber>%s</buildNumber></buildInformation>", STRING_VALUE, STRING_VALUE, STRING_VALUE, STRING_VALUE);
    }
}
