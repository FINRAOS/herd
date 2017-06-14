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
package org.finra.herd.dao.helper;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.threadpool.ThreadPool;


/**
 * ElasticsearchClientImpl is a stubbed version of the ElasticsearchClient used to setup a node for creating a SearchBuilder.
 */
public class ElasticsearchClientImpl implements ElasticsearchClient
{
    @Override
    public <RequestT extends ActionRequest, ResponseT extends ActionResponse,
        RequestBuilderT extends ActionRequestBuilder<RequestT, ResponseT, RequestBuilderT>> ActionFuture<ResponseT> execute(
        Action<RequestT, ResponseT, RequestBuilderT> action, RequestT request)
    {
        return null;
    }

    @Override
    public <RequestT extends ActionRequest, ResponseT extends ActionResponse,
        RequestBuilderT extends ActionRequestBuilder<RequestT, ResponseT, RequestBuilderT>> void execute(
        Action<RequestT, ResponseT, RequestBuilderT> action, RequestT request, ActionListener<ResponseT> actionListener)
    {
      // stub method
    }

    @Override
    public <RequestT extends ActionRequest, ResponseT extends ActionResponse,
        RequestBuilderT extends ActionRequestBuilder<RequestT, ResponseT, RequestBuilderT>> RequestBuilderT prepareExecute(
        Action<RequestT, ResponseT, RequestBuilderT> action)
    {
        return null;
    }

    @Override
    public ThreadPool threadPool()
    {
        return null;
    }
}

