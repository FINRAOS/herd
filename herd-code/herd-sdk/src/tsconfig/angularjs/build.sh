# Clean up previous distributions
rm -rf dist
rm -rf build

if [ ! -d "node_modules" ]; then
  npm install
fi

# Variable pointing to NGC
TSC="node_modules/.bin/ngc"
ROLLUP="node_modules/.bin/rollup"

$TSC -p tsconfig-build.json
$ROLLUP build/index.js --output.file dist/index.js --output.format es

$TSC -p tsconfig-build.es5.json
$ROLLUP build/index.js --output.file dist/index.es5.js --output.format es

$ROLLUP build/index.js --name "clientBundle"--output.file dist/index.umd.js --output.format umd

rsync -a --exclude=*.js build/ dist
