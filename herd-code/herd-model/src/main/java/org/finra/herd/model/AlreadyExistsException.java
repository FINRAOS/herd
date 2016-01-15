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
package org.finra.herd.model;

/**
 * An exception that is thrown when an attempt is made to create an object that already exists.
 */
public class AlreadyExistsException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    public AlreadyExistsException()
    {
        super();
    }

    public AlreadyExistsException(String message)
    {
        super(message);
    }

    public AlreadyExistsException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public AlreadyExistsException(Throwable cause)
    {
        super(cause);
    }
}
