#!/bin/sh
npm run build
MAIN_JS=$(python3 -c "import json; import sys; sys.stdout.write(json.loads(open('build/asset-manifest.json').read())['files']['main.js'])")
MAIN_CSS=$(python3 -c "import json; import sys; sys.stdout.write(json.loads(open('build/asset-manifest.json').read())['files']['main.css'])")
sed -i'.bak' "s|${MAIN_JS}|/debug2/main.js|g" build/index.html
sed -i'.bak' "s|${MAIN_CSS}|/debug2/main.css|g" build/index.html
mv build${MAIN_JS} dist/main.js
mv build${MAIN_CSS} dist/main.css
mv build/index.html dist/index.html
