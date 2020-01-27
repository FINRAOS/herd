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
package org.finra.herd.service.helper;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the helper for relational table registration related operations.
 */
public class RelationalTableRegistrationHelperTest extends AbstractServiceTest
{
    private static final String COLUMN_DEFAULT_VALUE = "0";

    private static final String COLUMN_SIZE = "16";

    private static final String DECIMAL_DIGITS = "2";

    private static final int NOT_NULLABLE = 0;

    private static final String NUMERIC_TYPE_NAME = "NUMERIC";

    private static final boolean REQUIRED = true;

    private static final String SCHEMA_COLUMNS_SIZE = COLUMN_SIZE + "," + DECIMAL_DIGITS;

    private static final String VARCHAR_TYPE_NAME = "VARCHAR";


    @Test
    public void testGetSchemaColumns() throws SQLException
    {
        // Create a mock result set.
        ResultSet resultSetMock = mock(ResultSet.class);

        // Setup the mock results.
        when(resultSetMock.next()).thenReturn(true).thenReturn(false);
        when(resultSetMock.getString("COLUMN_NAME")).thenReturn(COLUMN_NAME);
        when(resultSetMock.getString("TYPE_NAME")).thenReturn(NUMERIC_TYPE_NAME);
        when(resultSetMock.getString("COLUMN_SIZE")).thenReturn(COLUMN_SIZE);
        when(resultSetMock.getString("DECIMAL_DIGITS")).thenReturn(DECIMAL_DIGITS);
        when(resultSetMock.getInt("NULLABLE")).thenReturn(NOT_NULLABLE);
        when(resultSetMock.getString("COLUMN_DEF")).thenReturn(COLUMN_DEFAULT_VALUE);

        // Call the method under test.
        List<SchemaColumn> schemaColumns = relationalTableRegistrationHelper.getSchemaColumns(resultSetMock);

        // Verify the test results.
        assertThat("The list size is not correct.", schemaColumns.size(), is(equalTo(1)));
        assertThat("The column name is not correct.", schemaColumns.get(0).getName(), is(equalTo(COLUMN_NAME)));
        assertThat("The column type is not correct.", schemaColumns.get(0).getType(), is(equalTo(NUMERIC_TYPE_NAME)));
        assertThat("The column size is not correct.", schemaColumns.get(0).getSize(), is(equalTo(SCHEMA_COLUMNS_SIZE)));
        assertThat("The required column is not correct.", schemaColumns.get(0).isRequired(), is(equalTo(REQUIRED)));
        assertThat("The column default value is not correct.", schemaColumns.get(0).getDefaultValue(), is(equalTo(COLUMN_DEFAULT_VALUE)));

        // Validate the mock calls.
        verify(resultSetMock, times(2)).next();
        verify(resultSetMock).getString("COLUMN_NAME");
        verify(resultSetMock, times(2)).getString("TYPE_NAME");
        verify(resultSetMock).getString("COLUMN_SIZE");
        verify(resultSetMock, times(2)).getString("DECIMAL_DIGITS");
        verify(resultSetMock).getInt("NULLABLE");
        verify(resultSetMock).getString("COLUMN_DEF");
        verifyNoMoreInteractions(resultSetMock);
    }

    @Test
    public void testGetSchemaColumnsNonNumeric() throws SQLException
    {
        // Create a mock result set.
        ResultSet resultSetMock = mock(ResultSet.class);

        // Setup the mock results.
        when(resultSetMock.next()).thenReturn(true).thenReturn(false);
        when(resultSetMock.getString("COLUMN_NAME")).thenReturn(COLUMN_NAME);
        when(resultSetMock.getString("TYPE_NAME")).thenReturn(VARCHAR_TYPE_NAME);
        when(resultSetMock.getString("COLUMN_SIZE")).thenReturn(COLUMN_SIZE);
        when(resultSetMock.getInt("NULLABLE")).thenReturn(NOT_NULLABLE);
        when(resultSetMock.getString("COLUMN_DEF")).thenReturn(COLUMN_DEFAULT_VALUE);

        // Call the method under test.
        List<SchemaColumn> schemaColumns = relationalTableRegistrationHelper.getSchemaColumns(resultSetMock);

        // Verify the test results.
        assertThat("The list size is not correct.", schemaColumns.size(), is(equalTo(1)));
        assertThat("The column name is not correct.", schemaColumns.get(0).getName(), is(equalTo(COLUMN_NAME)));
        assertThat("The column type is not correct.", schemaColumns.get(0).getType(), is(equalTo(VARCHAR_TYPE_NAME)));
        assertThat("The column size is not correct.", schemaColumns.get(0).getSize(), is(equalTo(COLUMN_SIZE)));
        assertThat("The required column is not correct.", schemaColumns.get(0).isRequired(), is(equalTo(REQUIRED)));
        assertThat("The column default value is not correct.", schemaColumns.get(0).getDefaultValue(), is(equalTo(COLUMN_DEFAULT_VALUE)));

        // Validate the mock calls.
        verify(resultSetMock, times(2)).next();
        verify(resultSetMock).getString("COLUMN_NAME");
        verify(resultSetMock, times(2)).getString("TYPE_NAME");
        verify(resultSetMock).getString("COLUMN_SIZE");
        verify(resultSetMock).getInt("NULLABLE");
        verify(resultSetMock).getString("COLUMN_DEF");
        verifyNoMoreInteractions(resultSetMock);
    }

    @Test
    public void testGetSchemaColumnsNoDecimalDigits() throws SQLException
    {
        // Create a mock result set.
        ResultSet resultSetMock = mock(ResultSet.class);

        // Setup the mock results.
        when(resultSetMock.next()).thenReturn(true).thenReturn(false);
        when(resultSetMock.getString("COLUMN_NAME")).thenReturn(COLUMN_NAME);
        when(resultSetMock.getString("TYPE_NAME")).thenReturn(NUMERIC_TYPE_NAME);
        when(resultSetMock.getString("COLUMN_SIZE")).thenReturn(COLUMN_SIZE);
        when(resultSetMock.getInt("NULLABLE")).thenReturn(NOT_NULLABLE);
        when(resultSetMock.getString("COLUMN_DEF")).thenReturn(COLUMN_DEFAULT_VALUE);

        // Call the method under test.
        List<SchemaColumn> schemaColumns = relationalTableRegistrationHelper.getSchemaColumns(resultSetMock);

        // Verify the test results.
        assertThat("The list size is not correct.", schemaColumns.size(), is(equalTo(1)));
        assertThat("The column name is not correct.", schemaColumns.get(0).getName(), is(equalTo(COLUMN_NAME)));
        assertThat("The column type is not correct.", schemaColumns.get(0).getType(), is(equalTo(NUMERIC_TYPE_NAME)));
        assertThat("The column size is not correct.", schemaColumns.get(0).getSize(), is(equalTo(COLUMN_SIZE)));
        assertThat("The required column is not correct.", schemaColumns.get(0).isRequired(), is(equalTo(REQUIRED)));
        assertThat("The column default value is not correct.", schemaColumns.get(0).getDefaultValue(), is(equalTo(COLUMN_DEFAULT_VALUE)));

        // Validate the mock calls.
        verify(resultSetMock, times(2)).next();
        verify(resultSetMock).getString("COLUMN_NAME");
        verify(resultSetMock, times(2)).getString("TYPE_NAME");
        verify(resultSetMock).getString("COLUMN_SIZE");
        verify(resultSetMock).getString("DECIMAL_DIGITS");
        verify(resultSetMock).getInt("NULLABLE");
        verify(resultSetMock).getString("COLUMN_DEF");
        verifyNoMoreInteractions(resultSetMock);
    }

    @Test
    public void testGetSchemaColumnsEmptyDecimalDigits() throws SQLException
    {
        // Create a mock result set.
        ResultSet resultSetMock = mock(ResultSet.class);

        // Setup the mock results.
        when(resultSetMock.next()).thenReturn(true).thenReturn(false);
        when(resultSetMock.getString("COLUMN_NAME")).thenReturn(COLUMN_NAME);
        when(resultSetMock.getString("TYPE_NAME")).thenReturn(NUMERIC_TYPE_NAME);
        when(resultSetMock.getString("COLUMN_SIZE")).thenReturn(COLUMN_SIZE);
        when(resultSetMock.getString("DECIMAL_DIGITS")).thenReturn(EMPTY_STRING);
        when(resultSetMock.getInt("NULLABLE")).thenReturn(NOT_NULLABLE);
        when(resultSetMock.getString("COLUMN_DEF")).thenReturn(COLUMN_DEFAULT_VALUE);

        // Call the method under test.
        List<SchemaColumn> schemaColumns = relationalTableRegistrationHelper.getSchemaColumns(resultSetMock);

        // Verify the test results.
        assertThat("The list size is not correct.", schemaColumns.size(), is(equalTo(1)));
        assertThat("The column name is not correct.", schemaColumns.get(0).getName(), is(equalTo(COLUMN_NAME)));
        assertThat("The column type is not correct.", schemaColumns.get(0).getType(), is(equalTo(NUMERIC_TYPE_NAME)));
        assertThat("The column size is not correct.", schemaColumns.get(0).getSize(), is(equalTo(COLUMN_SIZE)));
        assertThat("The required column is not correct.", schemaColumns.get(0).isRequired(), is(equalTo(REQUIRED)));
        assertThat("The column default value is not correct.", schemaColumns.get(0).getDefaultValue(), is(equalTo(COLUMN_DEFAULT_VALUE)));

        // Validate the mock calls.
        verify(resultSetMock, times(2)).next();
        verify(resultSetMock).getString("COLUMN_NAME");
        verify(resultSetMock, times(2)).getString("TYPE_NAME");
        verify(resultSetMock).getString("COLUMN_SIZE");
        verify(resultSetMock).getString("DECIMAL_DIGITS");
        verify(resultSetMock).getInt("NULLABLE");
        verify(resultSetMock).getString("COLUMN_DEF");
        verifyNoMoreInteractions(resultSetMock);
    }
}
