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

    describe('ExpectedPartitionValuesService', function()
    {
        var ExpectedPartitionValuesService;
        var UrlBuilderService;
        var RestService;

        beforeEach(module('dm'));
        beforeEach(inject(function($injector)
        {
            ExpectedPartitionValuesService = $injector.get('ExpectedPartitionValuesService');
            UrlBuilderService = $injector.get('UrlBuilderService');
            RestService = $injector.get('RestService');
        }));

        describe('getExpectedPartitionValues()', function()
        {
            describe('given a mock UrlBuilderService and a success response from RestService', function()
            {
                beforeEach(function()
                {
                    spyOn(UrlBuilderService, 'buildUrl').and.returnValue('test_url');
                    spyOn(RestService, 'request').and.returnValue('test_response');
                });

                describe('when all parameters are set in the request', function()
                {
                    beforeEach(function()
                    {
                        this.response = ExpectedPartitionValuesService.getExpectedPartitionValues({
                            partitionKeyGroupName : 'test_partitionKeyGroupName',
                            startExpectedPartitionValue : 'test_startExpectedPartitionValue',
                            endExpectedPartitionValue : 'test_endExpectedPartitionValue'
                        });
                    });

                    it('calls UrlBuilderService.buildUrl', function()
                    {
                        expect(UrlBuilderService.buildUrl).toHaveBeenCalledWith({
                            paths : [ 'expectedPartitionValues', 'partitionKeyGroups', 'test_partitionKeyGroupName' ]
                        });
                    });

                    it('calls RestService.request with return value of UrlBuilderService.buildUrl', function()
                    {
                        expect(RestService.request).toHaveBeenCalledWith({
                            url : 'test_url',
                            params : {
                                startExpectedPartitionValue : 'test_startExpectedPartitionValue',
                                endExpectedPartitionValue : 'test_endExpectedPartitionValue'
                            },
                            method : 'GET'
                        });
                    });

                    it('returns the response of RestService.request', function()
                    {
                        expect(this.response).toBe('test_response');
                    });
                });
            });
        });
    });
})();