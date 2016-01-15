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

import com.amazonaws.services.s3.model.ObjectMetadata;

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
     * The metadata of this object.
     */
    private ObjectMetadata objectMetadata;

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

    public ObjectMetadata getObjectMetadata()
    {
        return objectMetadata;
    }

    public void setObjectMetadata(ObjectMetadata objectMetadata)
    {
        this.objectMetadata = objectMetadata;
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
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MockS3Object other = (MockS3Object) obj;
        if (key == null)
        {
            if (other.key != null)
                return false;
        }
        else if (!key.equals(other.key))
            return false;
        return true;
    }
}
