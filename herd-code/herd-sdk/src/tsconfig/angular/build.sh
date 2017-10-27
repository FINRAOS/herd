# Clean up previous distributions
rm -rf dist
rm -rf build

if [ ! -d "node_modules" ]; then
  npm install
fi

# Variable pointing to NGC
NGC="node_modules/.bin/ngc"
ROLLUP="node_modules/.bin/rollup"
ROLLUP_EXTERNALS="@angular/core,@angular/http,@angular/common,rxjs/add/observable/throw,rxjs/add/operator/catch,rxjs/add/operator/map,tslib"

$NGC -p tsconfig-build.json
$ROLLUP build/angular-client.js --output.file dist/angular-client.js --output.format es --external $ROLLUP_EXTERNALS

$NGC -p tsconfig-build.es5.json
$ROLLUP build/angular-client.js --output.file dist/angular-client.es5.js --output.format es --external $ROLLUP_EXTERNALS

$NGC -p tsconfig-build.umd.json
$ROLLUP build/angular-client.js --output.file dist/angular-client.umd.js --output.format umd --external $ROLLUP_EXTERNALS

rsync -a --exclude=*.js build/ dist
