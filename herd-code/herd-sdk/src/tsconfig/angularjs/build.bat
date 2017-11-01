rd dist /s /q
rd build /s /q

"node_modules/.bin/tsc" -p tsconfig-build.json^
 && "node_modules/.bin/rollup" build/index.js --output.file dist/index.js --output.format es^
 && "node_modules/.bin/tsc" -p tsconfig-build.es5.json^
 && "node_modules/.bin/rollup" build/index.js --output.file dist/index.es5.js --output.format es^
 && "node_modules/.bin/rollup" build/index.js --name "clientBundle" --output.file dist/index.umd.js --output.format umd^
 && robocopy build dist /s /XF *.js
