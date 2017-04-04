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

import java.util.List;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Tag;

/**
 * Simulates an S3 object.
 */
public class MockS3Object
{
    /**
     * The unique key of this object.
     */
    private String key;

    /**
     * Optional version attribute of this object.
     */
    private String version;

    /**
     * The metadata of this object.
     */
    private ObjectMetadata objectMetadata;

    /**
     * The tags set on the object.
     */
    private List<Tag> tags;

    /**
     * The data byte array of this object.
     */
    private byte[] data;

    public String getKey()
    {
        return key;
    }

    public void setKey(String key)
    {
        this.key = key;
    }

    public String getVersion()
    {
        return version;
    }

    public void setVersion(String version)
    {
        this.version = version;
    }

    public ObjectMetadata getObjectMetadata()
    {
        return objectMetadata;
    }

    public void setObjectMetadata(ObjectMetadata objectMetadata)
    {
        this.objectMetadata = objectMetadata;
    }

    public List<Tag> getTags()
    {
        return tags;
    }

    public void setTags(List<Tag> tags)
    {
        this.tags = tags;
    }

    public byte[] getData()
    {
        return data;
    }

    public void setData(byte[] data)
    {
        this.data = data;
    }

    @Override
    public int hashCode()
    {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof MockS3Object))
        {
            return false;
        }

        MockS3Object that = (MockS3Object) o;

        if (key != null ? !key.equals(that.key) : that.key != null)
        {
            return false;
        }
        if (version != null ? !version.equals(that.version) : that.version != null)
        {
            return false;
        }

        return true;
    }
}
