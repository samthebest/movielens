#!/bin/bash

set -e

mkdir -p data
test -f data/ml-1m.zip || wget -P data/ http://files.grouplens.org/datasets/movielens/ml-1m.zip

which unzip >/dev/null
unzip_installed=$?

if [ "${unzip_installed}" != 0 ]; then
    echo "ERROR: Please install unzip"
    echo "e.g. Debian distros: sudo apt-get install unzip"
    echo "e.g. Yum distros: sudo yum install unzip"
    exit 1
fi

cd data
test -d ml-1m || unzip ml-1m.zip
