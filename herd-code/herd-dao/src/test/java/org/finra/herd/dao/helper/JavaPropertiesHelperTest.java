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

import java.util.Map;
import java.util.Properties;

import org.finra.herd.dao.AbstractDaoTest;
import org.fusesource.hawtbuf.ByteArrayInputStream;
import org.junit.Assert;
import org.junit.Test;

public class JavaPropertiesHelperTest extends AbstractDaoTest
{
    @Test
    public void testToMap()
    {
        Properties properties = new Properties();
        properties.setProperty("a", "b");
        properties.setProperty("c", "d");
        Map<String, Object> map = javaPropertiesHelper.toMap(properties);

        Assert.assertEquals("map size", 2, map.size());
        Assert.assertEquals("map ['a']", map.get("a"), "b");
        Assert.assertEquals("map ['b']", map.get("c"), "d");
    }

    @Test
    public void testGetPropertiesFromInputStream()
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream("foo=bar".getBytes());
        Properties properties = javaPropertiesHelper.getProperties(inputStream);
        Assert.assertEquals("properties value 'foo'", "bar", properties.get("foo"));
    }

    @Test
    public void testGetPropertiesFromString()
    {
        Properties properties = javaPropertiesHelper.getProperties("foo=bar");
        Assert.assertEquals("properties value 'foo'", "bar", properties.get("foo"));
    }
}
