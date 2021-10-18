package org.finra.herd.service.helper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.finra.herd.service.AbstractServiceTest;

/**
 * BusinessObjectDataDdlPartitionsHelperTest
 */
public class BusinessObjectDataDdlPartitionsHelperTest extends AbstractServiceTest
{
    @Test
    public void testSuppressQuotesInNumericTypePartitionValues()
    {
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "TINYINT"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "SMALLINT"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "INT"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "BIGINT"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "FLOAT"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "DOUBLE"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "DECIMAL"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "NUMBER"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "tinyint"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "smallint"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "int"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "bigint"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "float"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "double"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "decimal"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "number"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "TinyInt"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "SmallInt"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "Int"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "BigInt"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "Float"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "Double"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "Decimal"));
        assertTrue(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(true, "Number"));

        // Suppress quotes in numeric type partition values set to false.
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "TINYINT"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "SMALLINT"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "INT"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "BIGINT"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "FLOAT"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "DOUBLE"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "DECIMAL"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "NUMBER"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "tinyint"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "smallint"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "int"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "bigint"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "float"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "double"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "decimal"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "number"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "TinyInt"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "SmallInt"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "Int"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "BigInt"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "Float"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "Double"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "Decimal"));
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues(false, "Number"));

        // Null value
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues( null, "TINYINT"));

        // Date partition type
        assertFalse(businessObjectDataDdlPartitionsHelper.suppressQuotesInNumericTypePartitionValues( true, "DATE"));
    }
}
