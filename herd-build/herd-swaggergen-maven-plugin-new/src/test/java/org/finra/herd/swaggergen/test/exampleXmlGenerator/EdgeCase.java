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

import java.io.Serializable;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.finra.herd.swaggergen.test.EmptyEnum;

@XmlRootElement(name = "foo")
@XmlType
@XmlAccessorType(XmlAccessType.FIELD)
public class EdgeCase
{
    private Set<String> set;

    private EdgeCase recursive;

    private Queue<String> nonListSetCollection;

    private EmptyEnum emptyEnum;

    @XmlElements({@XmlElement(type = String.class), @XmlElement(type = Integer.class)})
    private List<Serializable> serializables;

    @XmlElements({@XmlElement(type = String.class), @XmlElement(type = Integer.class)})
    private Serializable serializable;

    public Set<String> getSet()
    {
        return set;
    }

    public void setSet(Set<String> set)
    {
        this.set = set;
    }

    public EdgeCase getRecursive()
    {
        return recursive;
    }

    public void setRecursive(EdgeCase recursive)
    {
        this.recursive = recursive;
    }

    public Queue<String> getNonListSetCollection()
    {
        return nonListSetCollection;
    }

    public void setNonListSetCollection(Queue<String> nonListSetCollection)
    {
        this.nonListSetCollection = nonListSetCollection;
    }

    public EmptyEnum getEmptyEnum()
    {
        return emptyEnum;
    }

    public void setEmptyEnum(EmptyEnum emptyEnum)
    {
        this.emptyEnum = emptyEnum;
    }

    public List<Serializable> getSerializables()
    {
        return serializables;
    }

    public void setSerializables(List<Serializable> serializables)
    {
        this.serializables = serializables;
    }

    public Serializable getSerializable()
    {
        return serializable;
    }

    public void setSerializable(Serializable serializable)
    {
        this.serializable = serializable;
    }
}
