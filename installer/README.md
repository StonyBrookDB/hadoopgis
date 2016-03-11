Library location requirement:
All external libraries must be located in the same path on every node of the cluster.

Steps to install HadoopGIS

1. Install dependencies listed in `listdependencies`.

2. Copy all libraries to their respective exact paths on every other node.

3. Run the install script `installhadoopgis.sh`  to update `LD_LIBRARY_PATH`, include and lib paths, as well as to compile all binary files from the `src` directory.
execute `installhadoopgis.sh --help` to see list of available parameters.
