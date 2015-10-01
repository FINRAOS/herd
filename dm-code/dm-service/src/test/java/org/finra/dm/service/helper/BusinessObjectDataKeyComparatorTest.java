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
package org.finra.dm.service.helper;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.finra.dm.model.api.xml.BusinessObjectDataKey;

/**
 * Tests the business object data key comparator class.
 */
public class BusinessObjectDataKeyComparatorTest
{
    // Create a comparator to test.
    private BusinessObjectDataKeyComparator comparator = new BusinessObjectDataKeyComparator();

    @Test
    public void testCompareNullKey() throws Exception
    {
        // Two null objects are equal.
        assertEquals(0, comparator.compare(null, null));

        // Null is less than an object.
        assertEquals(-1, comparator.compare(null, new BusinessObjectDataKey()));

        // An object is greater than null.
        assertEquals(1, comparator.compare(new BusinessObjectDataKey(), null));
    }

    @Test
    public void testCompareBusinessObjectDefinitionName() throws Exception
    {
        BusinessObjectDataKey businessObjectDataKey1 = new BusinessObjectDataKey();
        BusinessObjectDataKey businessObjectDataKey2 = new BusinessObjectDataKey();

        // "a" = "b"
        businessObjectDataKey1.setBusinessObjectDefinitionName("a");
        businessObjectDataKey2.setBusinessObjectDefinitionName("a");
        assertEquals(0, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "a" < "b"
        businessObjectDataKey1.setBusinessObjectDefinitionName("a");
        businessObjectDataKey2.setBusinessObjectDefinitionName("b");
        assertEquals(-1, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "b" > "a"
        assertEquals(1, comparator.compare(businessObjectDataKey2, businessObjectDataKey1));
    }

    @Test
    public void testCompareBusinessObjectFormatUsage() throws Exception
    {
        BusinessObjectDataKey businessObjectDataKey1 = new BusinessObjectDataKey();
        BusinessObjectDataKey businessObjectDataKey2 = new BusinessObjectDataKey();

        // "a" = "b"
        businessObjectDataKey1.setBusinessObjectFormatUsage("a");
        businessObjectDataKey2.setBusinessObjectFormatUsage("a");
        assertEquals(0, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "a" < "b"
        businessObjectDataKey1.setBusinessObjectFormatUsage("a");
        businessObjectDataKey2.setBusinessObjectFormatUsage("b");
        assertEquals(-1, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "b" > "a"
        assertEquals(1, comparator.compare(businessObjectDataKey2, businessObjectDataKey1));
    }

    @Test
    public void testCompareBusinessObjectFormatFileType() throws Exception
    {
        BusinessObjectDataKey businessObjectDataKey1 = new BusinessObjectDataKey();
        BusinessObjectDataKey businessObjectDataKey2 = new BusinessObjectDataKey();

        // "a" = "b"
        businessObjectDataKey1.setBusinessObjectFormatFileType("a");
        businessObjectDataKey2.setBusinessObjectFormatFileType("a");
        assertEquals(0, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "a" < "b"
        businessObjectDataKey1.setBusinessObjectFormatFileType("a");
        businessObjectDataKey2.setBusinessObjectFormatFileType("b");
        assertEquals(-1, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "b" > "a"
        assertEquals(1, comparator.compare(businessObjectDataKey2, businessObjectDataKey1));
    }

    @Test
    public void testCompareBusinessObjectFormatVersion() throws Exception
    {
        BusinessObjectDataKey businessObjectDataKey1 = new BusinessObjectDataKey();
        BusinessObjectDataKey businessObjectDataKey2 = new BusinessObjectDataKey();

        // "a" = "b"
        businessObjectDataKey1.setBusinessObjectFormatVersion(0);
        businessObjectDataKey2.setBusinessObjectFormatVersion(0);
        assertEquals(0, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "a" < "b"
        businessObjectDataKey1.setBusinessObjectFormatVersion(0);
        businessObjectDataKey2.setBusinessObjectFormatVersion(1);
        assertEquals(-1, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "b" > "a"
        assertEquals(1, comparator.compare(businessObjectDataKey2, businessObjectDataKey1));
    }

    @Test
    public void testComparePartitionValue() throws Exception
    {
        BusinessObjectDataKey businessObjectDataKey1 = new BusinessObjectDataKey();
        BusinessObjectDataKey businessObjectDataKey2 = new BusinessObjectDataKey();

        // "a" = "b"
        businessObjectDataKey1.setPartitionValue("a");
        businessObjectDataKey2.setPartitionValue("a");
        assertEquals(0, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "a" < "b"
        businessObjectDataKey1.setPartitionValue("a");
        businessObjectDataKey2.setPartitionValue("b");
        assertEquals(-1, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "b" > "a"
        assertEquals(1, comparator.compare(businessObjectDataKey2, businessObjectDataKey1));
    }

    @Test
    public void testCompareBusinessObjectDataVersion() throws Exception
    {
        BusinessObjectDataKey businessObjectDataKey1 = new BusinessObjectDataKey();
        BusinessObjectDataKey businessObjectDataKey2 = new BusinessObjectDataKey();

        // "a" = "b"
        businessObjectDataKey1.setBusinessObjectDataVersion(0);
        businessObjectDataKey2.setBusinessObjectDataVersion(0);
        assertEquals(0, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "a" < "b"
        businessObjectDataKey1.setBusinessObjectDataVersion(0);
        businessObjectDataKey2.setBusinessObjectDataVersion(1);
        assertEquals(-1, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "b" > "a"
        assertEquals(1, comparator.compare(businessObjectDataKey2, businessObjectDataKey1));
    }

    @Test
    public void testCompareAllFields() throws Exception
    {
        BusinessObjectDataKey businessObjectDataKey1 = new BusinessObjectDataKey();
        BusinessObjectDataKey businessObjectDataKey2 = new BusinessObjectDataKey();

        businessObjectDataKey1.setBusinessObjectDefinitionName("a");
        businessObjectDataKey1.setBusinessObjectFormatUsage("a");
        businessObjectDataKey1.setBusinessObjectFormatFileType("a");
        businessObjectDataKey1.setBusinessObjectFormatVersion(0);
        businessObjectDataKey1.setPartitionValue("a");
        businessObjectDataKey1.setBusinessObjectDataVersion(0);

        businessObjectDataKey2.setBusinessObjectDefinitionName("a");
        businessObjectDataKey2.setBusinessObjectFormatUsage("a");
        businessObjectDataKey2.setBusinessObjectFormatFileType("a");
        businessObjectDataKey2.setBusinessObjectFormatVersion(0);
        businessObjectDataKey2.setPartitionValue("a");
        businessObjectDataKey2.setBusinessObjectDataVersion(0);

        // "a" = "b"
        assertEquals(0, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "a" < "b"
        businessObjectDataKey1.setBusinessObjectDataVersion(0);
        businessObjectDataKey2.setBusinessObjectDataVersion(1);
        assertEquals(-1, comparator.compare(businessObjectDataKey1, businessObjectDataKey2));

        // "b" > "a"
        assertEquals(1, comparator.compare(businessObjectDataKey2, businessObjectDataKey1));
    }
}
