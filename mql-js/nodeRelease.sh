#!/usr/bin/env bash
rm -rf src/
cp -r src-base src
cp -r ../core/src ./
echo "Ensure you've revved the version number in package-browser.json"
rm -rf public/
shadow-cljs release library
cp package-node.json public/package.json
mv public/js/mql.js public/index.js
cd public
#npm publish
#rm -rf public/
