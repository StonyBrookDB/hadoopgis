#! /bin/bash
PREFIX_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}")/../built/" && pwd)"

wget http://downloads.sourceforge.net/boost/boost_1_59_0.tar.bz2
tar xvf boost_1_59_0.tar.bz2
cd boost_1_59_0

./bootstrap.sh --prefix=${PREFIX_PATH}
# Optional - this will take a while
./b2 --prefix=${PREFIX_PATH} install

cd ..

#cmake -DCMAKE_INSTALL_PREFIX:PATH=../../built .
#make
#make install
