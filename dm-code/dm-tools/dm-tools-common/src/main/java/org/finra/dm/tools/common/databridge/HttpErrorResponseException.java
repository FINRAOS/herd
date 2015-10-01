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
package org.finra.dm.tools.common.databridge;

/**
 * An exception that is thrown when an HTTP response is received with an error.
 */
public class HttpErrorResponseException extends RuntimeException
{
    private int statusCode;
    private String statusDescription;
    private String responseMessage;

    /**
     * Constructs an HTTP error response exception with status code, status description, and response message.
     *
     * @param statusCode the status code.
     * @param statusDescription the status description.
     * @param responseMessage the response message.
     */
    public HttpErrorResponseException(int statusCode, String statusDescription, String responseMessage)
    {
        super();
        this.statusCode = statusCode;
        this.statusDescription = statusDescription;
        this.responseMessage = responseMessage;
    }

    /**
     * Constructs an HTTP error response exception with a message, status code, status description, and response message.
     *
     * @param message the message.
     * @param statusCode the status code.
     * @param statusDescription the status description.
     * @param responseMessage the response message.
     */
    public HttpErrorResponseException(String message, int statusCode, String statusDescription, String responseMessage)
    {
        super(message);
        this.statusCode = statusCode;
        this.statusDescription = statusDescription;
        this.responseMessage = responseMessage;
    }

    /**
     * Constructs an HTTP error response exception with a message, cause, status code, status description, and response message.
     *
     * @param message the message.
     * @param cause the original cause.
     * @param statusCode the status code.
     * @param statusDescription the status description.
     * @param responseMessage the response message.
     */
    public HttpErrorResponseException(String message, Throwable cause, int statusCode, String statusDescription, String responseMessage)
    {
        super(message, cause);
        this.statusCode = statusCode;
        this.statusDescription = statusDescription;
        this.responseMessage = responseMessage;
    }

    /**
     * Constructs an HTTP error response exception with a cause, status code, status description, and response message.
     *
     * @param cause the original cause.
     * @param statusCode the status code.
     * @param statusDescription the status description.
     * @param responseMessage the response message.
     */
    public HttpErrorResponseException(Throwable cause, int statusCode, String statusDescription, String responseMessage)
    {
        super(cause);
        this.statusCode = statusCode;
        this.statusDescription = statusDescription;
        this.responseMessage = responseMessage;
    }

    /**
     * Constructs an HTTP error response exception with a message, cause, status code, status description, response message, and enable suppression and writable
     * stack trace booleans.
     *
     * @param message the message.
     * @param cause the original cause.
     * @param enableSuppression whether suppression should be enabled or not.
     * @param writableStackTrace whether the stack trace should be written or not.
     * @param statusCode the status code.
     * @param statusDescription the status description.
     * @param responseMessage the response message.
     */
    protected HttpErrorResponseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, int statusCode,
        String statusDescription, String responseMessage)
    {
        super(message, cause, enableSuppression, writableStackTrace);
        this.statusCode = statusCode;
        this.statusDescription = statusDescription;
        this.responseMessage = responseMessage;
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public String getStatusDescription()
    {
        return statusDescription;
    }

    public String getResponseMessage()
    {
        return responseMessage;
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        addTextToString("Message: " + getMessage(), stringBuilder);
        addTextToString("Status Code: " + statusCode, stringBuilder);
        addTextToString("Status Description: " + statusDescription, stringBuilder);
        addTextToString("Response Message: " + responseMessage, stringBuilder);
        return stringBuilder.toString();
    }

    /**
     * Adds text to the string builder and inserting a preceding comma if text already exists in the builder.
     *
     * @param text the text to add.
     * @param stringBuilder the string builder to add the text to.
     */
    private void addTextToString(String text, StringBuilder stringBuilder)
    {
        if (stringBuilder.length() > 0)
        {
            stringBuilder.append(", ");
        }
        stringBuilder.append(text);
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }

        HttpErrorResponseException that = (HttpErrorResponseException) object;
        return statusCode == that.getStatusCode();
    }

    @Override
    public int hashCode()
    {
        return Integer.valueOf(statusCode).hashCode();
    }
}
