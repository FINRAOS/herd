rd dist /s /q
rd build /s /q

"node_modules/.bin/ngc" -p tsconfig-build.json^
 && "node_modules/.bin/rollup" build/angular-client.js --output.file dist/angular-client.js --output.format es --external @angular/core,@angular/common,rxjs/add/observable/throw,rxjs/add/operator/catch,rxjs/add/operator/map,tslib^
 && "node_modules/.bin/ngc" -p tsconfig-build.es5.json^
 && "node_modules/.bin/rollup" build/angular-client.js --output.file dist/angular-client.es5.js --output.format es --external @angular/core,@angular/common,rxjs/add/observable/throw,rxjs/add/operator/catch,rxjs/add/operator/map,tslib^
 && "node_modules/.bin/rollup" build/angular-client.js --name "clientBundle" --output.file dist/angular-client.umd.js --output.format umd --external @angular/core,@angular/common,rxjs/add/observable/throw,rxjs/add/operator/catch,rxjs/add/operator/map,tslib^
 && robocopy build dist /s /XF *.js
