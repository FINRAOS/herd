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
$ROLLUP build/api.js --output.file dist/api.js --output.format es

$TSC -p tsconfig-build.es5.json
$ROLLUP build/api.js --output.file dist/api.es5.js --output.format es

rsync -a --exclude=*.js build/ dist
