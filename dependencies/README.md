# hadoopgis dependencies

This directory contains installer scripts for dependencies of HadoopGIS
First install required libraries as sudo users.
```
gcc
g++
cmake
gnuplot (for visualizer)
```

The following dependencies are installed to the built directory and do not require sudo permission.
First cd to dependencies directory

It is recommended for RHEL/CentOS:
```
  ./rhel_install_system_packages.sh
  ./rhel_install_boost.sh
  ./rhel_install_geos.sh
  ./rhel_install_libspatialindex.sh
  ./rhel_install_cgal.sh (for 3D support)
```

It is recommended for Ubuntu:
```
  ./ubuntu_install_system_packages.sh
  ./ubuntu_install_geos.sh
  ./ubuntu_install_libspatialindex.sh
  ./ubuntu_install_boost.sh
  ./rhel_install_cgal.sh (for 3D support)
```
