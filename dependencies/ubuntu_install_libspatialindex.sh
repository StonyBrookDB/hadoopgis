#! /bin/bash

wget http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.1.tar.bz2
tar xvf spatialindex-src-1.8.1.tar.bz2
cd spatialindex-src-1.8.1
cmake -DCMAKE_INSTALL_PREFIX:PATH=../../built .
make
make install

cd
sudo /sbin/ldconfig
