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
(function()
{
    'use strict';

    describe('RestService', function()
    {
        var $location;
        var $httpBackend;
        var RestService;

        beforeEach(module('dm'));
        beforeEach(inject(function($injector)
        {
            $location = $injector.get('$location');
            $httpBackend = $injector.get('$httpBackend');
            RestService = $injector.get('RestService');

            $location.protocol = function()
            {
                return 'testProtocol';
            };

            $location.host = function()
            {
                return 'testHost';
            };

            $location.port = function()
            {
                return 1234;
            };
        }));

        describe('request()', function()
        {
            describe('when requesting a GET with a port in $location', function()
            {
                it('constructs the correct GET request to $http, and returns promise with successful response status and data', function()
                {
                    $httpBackend.expectGET('testProtocol://testHost:1234/dm-app/rest/testUrl').respond(200, 'testResponse');

                    var successResponse = null;
                    var errorResponse = null;

                    RestService.request({
                        url : '/testUrl',
                        method : 'GET'
                    }).then(function(response)
                    {
                        successResponse = response;
                    }, function(response)
                    {
                        errorResponse = response;
                    });

                    $httpBackend.flush();

                    expect(successResponse).not.toBeNull();
                    expect(successResponse.status).toBe(200);
                    expect(successResponse.data).toBe('testResponse');
                    expect(errorResponse).toBeNull();
                });

                it('constructs the correct GET request to $http, and returns promise with error response status and data', function()
                {
                    $httpBackend.expectGET('testProtocol://testHost:1234/dm-app/rest/testUrl').respond(400, 'testResponse');

                    var successResponse = null;
                    var errorResponse = null;

                    RestService.request({
                        url : '/testUrl',
                        method : 'GET'
                    }).then(function(response)
                    {
                        successResponse = response;
                    }, function(response)
                    {
                        errorResponse = response;
                    });

                    $httpBackend.flush();

                    expect(errorResponse).not.toBeNull();
                    expect(errorResponse.status).toBe(400);
                    expect(errorResponse.data).toBe('testResponse');
                    expect(successResponse).toBeNull();
                });
            });

            describe('when requesting a GET without a port in $location', function()
            {
                it('constructs the correct GET request to $http, and returns promise with successful response status and data', function()
                {
                    $location.port = function()
                    {
                        return null;
                    };

                    $httpBackend.expectGET('testProtocol://testHost/dm-app/rest/testUrl').respond(200, 'testResponse');

                    RestService.request({
                        url : '/testUrl',
                        method : 'GET'
                    });

                    $httpBackend.flush();
                });
            });
        });
    });
})();