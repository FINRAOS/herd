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
package org.finra.herd.swaggergen.test.definitionGenerator;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "xsdMappingCase")
@XmlType(name = "xsdMappingCase")
@XmlAccessorType(XmlAccessType.FIELD)
public class XsdMappingCase
{
    @XmlElement
    private String scalar1;

    @XmlElement
    private String scalar2;

    public String getScalar1()
    {
        return scalar1;
    }

    public void setScalar1(String scalar1)
    {
        this.scalar1 = scalar1;
    }

    public String getScalar2()
    {
        return scalar2;
    }

    public void setScalar2(String scalar2)
    {
        this.scalar2 = scalar2;
    }
}
