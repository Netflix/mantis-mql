#!/usr/bin/env bash
echo "Ensure you've revved the version number in package-browser.json"
rm -rf public/
shadow-cljs release browser
cp package-browser.json public/package.json
mv public/js/mql.js public/index.js
cd public
#npm publish
rm -rf public/
