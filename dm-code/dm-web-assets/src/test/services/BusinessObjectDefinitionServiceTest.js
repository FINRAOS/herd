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

    describe('BusinessObjectDefinitionService', function()
    {
        var BusinessObjectDefinitionService;
        var RestService;

        beforeEach(module('dm'));
        beforeEach(inject(function($injector)
        {
            BusinessObjectDefinitionService = $injector.get('BusinessObjectDefinitionService');
            RestService = $injector.get('RestService');
            spyOn(RestService, 'request').and.returnValue('testValue');
        }));

        describe('getBusinessObjectDefinitionKeys()', function()
        {
            it('calls RestService.request() with correct options, and returns result of RestService.request()', function()
            {
                var returnValue = BusinessObjectDefinitionService.getBusinessObjectDefinitionKeys({
                    namespaceCode : 'testNamespaceCode'
                });

                expect(RestService.request).toHaveBeenCalledWith({
                    url : '/businessObjectDefinitions/namespaces/testNamespaceCode',
                    method : 'GET'
                });
                expect(returnValue).toBe('testValue');
            });
        });
    });
})();