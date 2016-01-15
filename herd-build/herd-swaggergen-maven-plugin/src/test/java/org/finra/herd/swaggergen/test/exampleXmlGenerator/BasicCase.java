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
package org.finra.herd.swaggergen.test.exampleXmlGenerator;

import java.math.BigDecimal;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.finra.herd.swaggergen.test.TestEnum;

@XmlRootElement(name = "foo")
@XmlType
public class BasicCase
{
    private String string;

    private Integer objectInteger;

    private int primitiveInteger;

    private Long objectLong;

    private long primitiveLong;

    private BigDecimal bigDecimal;

    private Boolean objectBoolean;

    private boolean primitiveBoolean;

    private TestEnum enumType;

    private List<String> list;

    public String getString()
    {
        return string;
    }

    public void setString(String string)
    {
        this.string = string;
    }

    public Integer getObjectInteger()
    {
        return objectInteger;
    }

    public void setObjectInteger(Integer objectInteger)
    {
        this.objectInteger = objectInteger;
    }

    public int getPrimitiveInteger()
    {
        return primitiveInteger;
    }

    public void setPrimitiveInteger(int primitiveInteger)
    {
        this.primitiveInteger = primitiveInteger;
    }

    public Long getObjectLong()
    {
        return objectLong;
    }

    public void setObjectLong(Long objectLong)
    {
        this.objectLong = objectLong;
    }

    public long getPrimitiveLong()
    {
        return primitiveLong;
    }

    public void setPrimitiveLong(long primitiveLong)
    {
        this.primitiveLong = primitiveLong;
    }

    public BigDecimal getBigDecimal()
    {
        return bigDecimal;
    }

    public void setBigDecimal(BigDecimal bigDecimal)
    {
        this.bigDecimal = bigDecimal;
    }

    public Boolean getObjectBoolean()
    {
        return objectBoolean;
    }

    public void setObjectBoolean(Boolean objectBoolean)
    {
        this.objectBoolean = objectBoolean;
    }

    public boolean isPrimitiveBoolean()
    {
        return primitiveBoolean;
    }

    public void setPrimitiveBoolean(boolean primitiveBoolean)
    {
        this.primitiveBoolean = primitiveBoolean;
    }

    public TestEnum getEnumType()
    {
        return enumType;
    }

    public void setEnumType(TestEnum enumType)
    {
        this.enumType = enumType;
    }

    public List<String> getList()
    {
        return list;
    }

    public void setList(List<String> list)
    {
        this.list = list;
    }
}
