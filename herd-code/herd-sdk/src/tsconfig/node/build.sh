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
TSC="node_modules/.bin/ngc"
ROLLUP="node_modules/.bin/rollup"

echo "Running TSC on tsconfig-build.json"
$TSC -p tsconfig-build.json
echo "Running creating ROLLUP bundle dist/api.js"
$ROLLUP build/api.js --output.file dist/api.js --output.format es

echo "Running TSC on tsconfig-build.es5.json"
$TSC -p tsconfig-build.es5.json
echo "Running creating ROLLUP bundle dist/api.es5.js"
$ROLLUP build/api.js --output.file dist/api.es5.js --output.format es

rsync -a --exclude=*.js build/ dist
