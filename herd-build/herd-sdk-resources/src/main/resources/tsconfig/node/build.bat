rd dist /s /q
rd build /s /q

"node_modules/.bin/tsc" -p tsconfig-build.json^
 && "node_modules/.bin/rollup" build/api.js --output.file dist/api.js --output.format es^
 && "node_modules/.bin/tsc" -p tsconfig-build.es5.json^
 && "node_modules/.bin/rollup" build/api.js --output.file dist/api.es5.js --output.format es^
 && robocopy build dist /s /XF *.js
