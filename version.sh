#!/bin/bash

set -eux

cd /source

gitVersion=$(git describe --match 'v[0-9]*' --always --long --tags)
timeStamp=$(date --utc +%Y%m%d)
version=cdpx-${gitVersion}+${BUILD_SOURCEBRANCH////_}.${timeStamp}.${CDP_DEFINITION_BUILD_COUNT}

echo '##vso[build.updatebuildnumber]'$version