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

# Variable pointing to TSC
TSC="node_modules/.bin/tsc"
ROLLUP="node_modules/.bin/rollup"

echo "Running TSC on tsconfig-build.json"
$TSC -p tsconfig-build.json
echo "Running creating ROLLUP bundle dist/index.js"
$ROLLUP build/index.js --output.file dist/index.js --output.format es

echo "Running TSC on tsconfig-build.es5.json"
$TSC -p tsconfig-build.es5.json
echo "Running creating ROLLUP bundle dist/index.es5.js"
$ROLLUP build/index.js --output.file dist/index.es5.js --output.format es

echo "Running creating ROLLUP bundle dist/index.umd.js"
$ROLLUP build/index.js --name "clientBundle"--output.file dist/index.umd.js --output.format umd

echo "Copying non js files to dist"
rsync -a --exclude=*.js build/ dist
