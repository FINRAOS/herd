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

    describe('FileTypesService', function()
    {
        var FileTypesService;
        var UrlBuilderService;
        var RestService;

        beforeEach(module('herd'));
        beforeEach(inject(function($injector)
        {
            FileTypesService = $injector.get('FileTypesService');
            UrlBuilderService = $injector.get('UrlBuilderService');
            RestService = $injector.get('RestService');

            spyOn(UrlBuilderService, 'buildUrl').and.returnValue('test_url');
            spyOn(RestService, 'request').and.returnValue('test_response');
        }));

        describe('getFileTypes()', function()
        {
            beforeEach(function()
            {
                this.result = FileTypesService.getFileTypes();
            });
            
            it('calls UrlBuilderService.buildUrl with correct parameters', function()
            {
                expect(UrlBuilderService.buildUrl).toHaveBeenCalledWith({
                    paths : [ 'fileTypes' ]
                });
            });

            it('calls RestService.request with correct parameters', function()
            {
                expect(RestService.request).toHaveBeenCalledWith({
                    url : 'test_url',
                    type : 'GET'
                });
            });

            it('returns response of RestService.request', function()
            {
                expect(this.result).toBe('test_response');
            });
        });
    });
})();