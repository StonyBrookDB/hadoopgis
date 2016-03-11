#! /bin/bash

# This script generates the default path for installation of hadoopgis

PREFIX_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}")/../built/" && pwd)"
# install GMP
echo "-DCMAKE_PREFIX_PATH=${PREFIX_PATH} -DCMAKE_INSTALL_PREFIX:PATH=${PREFIX_PATH} -DBoost_NO_BOOST_CMAKE=BOOL:ON -DBOOST_LIBRARYDIR=${PREFIX_PATH}/lib -DBOOST_ROOT=${PREFIX_PATH}"

