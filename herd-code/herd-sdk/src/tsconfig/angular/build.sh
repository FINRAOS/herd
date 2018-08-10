if [ ! -d "dist" ]; then
  echo "cleaning dist"
  rm -rf dist
fi

if [ ! -d "build" ]; then
  echo "cleaning build"
  rm -rf build
fi

if [ ! -d "node_modules" ]; then
  echo "installing node modules"
  npm install
fi

# Variable pointing to NGC
NGC="node_modules/.bin/ngc"
ROLLUP="node_modules/.bin/rollup"
ROLLUP_EXTERNALS="@angular/core,@angular/common,rxjs/add/observable/throw,rxjs/add/operator/catch,rxjs/add/operator/map,tslib"

echo "Running NGC on tsconfig-build.json"
$NGC -p tsconfig-build.json
echo "Running creating ROLLUP bundle dist/angular-client.js"
$ROLLUP build/angular-client.js --output.file dist/angular-client.js --output.format es --external $ROLLUP_EXTERNALS

echo "Running NGC on tsconfig-build.es5.json"
$NGC -p tsconfig-build.es5.json
echo "Running creating ROLLUP bundle dist/angular-client.es5.js"
$ROLLUP build/angular-client.js --output.file dist/angular-client.es5.js --output.format es --external $ROLLUP_EXTERNALS

echo "Running creating ROLLUP bundle dist/angular-client.umd.js"
$ROLLUP build/angular-client.js --name "clientBundle" --output.file dist/angular-client.umd.js --output.format umd --external $ROLLUP_EXTERNALS

echo "Copying non js files to dist"
rsync -a --exclude=*.js build/ dist
