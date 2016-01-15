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

    describe('BusinessObjectDataAvailabilityService', function()
    {
        var BusinessObjectDataAvailabilityService;
        var UrlBuilderService;
        var RestService;

        beforeEach(module('herd'));
        beforeEach(inject(function($injector)
        {
            BusinessObjectDataAvailabilityService = $injector.get('BusinessObjectDataAvailabilityService');
            UrlBuilderService = $injector.get('UrlBuilderService');
            RestService = $injector.get('RestService');

            spyOn(UrlBuilderService, 'buildUrl').and.returnValue('test_url');
            spyOn(RestService, 'request').and.returnValue('test_response');
        }));

        describe('getBusinessObjectDataAvailability()', function()
        {
            var getBusinessObjectDataAvailabilityRequest = 'test_getBusinessObjectDataAvailabilityRequest';

            beforeEach(function()
            {
                this.response = BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability(getBusinessObjectDataAvailabilityRequest);
            });

            it('calls UrlBuilderService.buildUrl', function()
            {
                expect(UrlBuilderService.buildUrl).toHaveBeenCalledWith({
                    paths : [ 'businessObjectData', 'availability' ]
                });
            });

            it('calls RestService.request with response of UrlBuilderService.buildUrl and test request', function()
            {
                expect(RestService.request).toHaveBeenCalledWith({
                    url : 'test_url',
                    method : 'POST',
                    data : getBusinessObjectDataAvailabilityRequest
                });
            });

            it('responds with the result of RestService.request', function()
            {
                expect(this.response).toBe('test_response');
            });
        });
    });
})();