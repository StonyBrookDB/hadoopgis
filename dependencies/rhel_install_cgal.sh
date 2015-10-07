#! /bin/bash

PREFIX_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}")/../built/" && pwd)"
# install GMP
wget https://gmplib.org/download/gmp/gmp-6.0.0a.tar.bz2
tar xvf gmp-6.0.0a.tar.bz2
cd gmp-6.0.0
./configure --prefix=${PREFIX_PATH}
make
make check
make install
cd ../

# install MPFR
wget http://www.mpfr.org/mpfr-current/mpfr-3.1.3.tar.gz
tar xvf mpfr-3.1.3.tar.gz
cd mpfr-3.1.3
./configure --prefix=${PREFIX_PATH} --with-gmp=${PREFIX_PATH}
make
make check
make install
cd ../

# install CGAL
wget https://gforge.inria.fr/frs/download.php/file/35138/CGAL-4.6.3.tar.gz
tar xvf CGAL-4.6.3.tar.gz
cd CGAL-4.6.3
cmake -DCMAKE_PREFIX_PATH=${PREFIX_PATH} -DCMAKE_INSTALL_PREFIX:PATH=${PREFIX_PATH} -DBoost_NO_BOOST_CMAKE=BOOL:ON -DBOOST_LIBRARYDIR=${PREFIX_PATH}/lib -DBOOST_ROOT=${PREFIX_PATH} .
make
make install

cd ../

