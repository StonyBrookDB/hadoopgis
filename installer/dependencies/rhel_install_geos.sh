#! /bin/bash

wget http://download.osgeo.org/geos/geos-3.3.9.tar.bz2
tar xvf geos-3.3.9.tar.bz2
cd geos-3.3.9
cmake -DCMAKE_INSTALL_PREFIX:PATH=../../built .
make
make install
