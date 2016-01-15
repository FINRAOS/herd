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
package org.finra.herd.dao.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MockResultSetMetaData implements ResultSetMetaData
{
    private List<String> columnNames = new ArrayList<>();

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames)
    {
        this.columnNames = columnNames;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnCount() throws SQLException
    {
        return columnNames.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int isNullable(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSigned(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getColumnName(int column) throws SQLException
    {
        return columnNames.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPrecision(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getScale(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTableName(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCatalogName(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnType(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWritable(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException
    {
        throw new UnsupportedOperationException();
    }
}
