# hadoopgis
Comprehensive Hadoop-GIS with extensions.
A spatial big data management and query processing system.

Supported Extensions
- 2-Dimensional data
- 3-Dimensional data (in progress)
- 2-Dimensional and temporal data (in progress)
- Temporal data (in progress)

Supported Queries
- Range (Containment)
- Spatial Join with various predicates
- K-nearest neighbor (single window as well as aggregate nearest neighbor).

Supported Partitioning Methods
- Fixed Grid Partitioning (FG)
- Binary Space Partitioning (BSP)
- Quad tree Partitioning (QT)
- Hilbert Curve Value Partitioning (HC)
- Sort Tile Recursive Partitioning (STR)
- Boundary Optimized Strip Partitioning (BOS)
- Strip Line Partitioning (SLC)

MapReduce Parallel Indexing/Partitioning Support
- Single-step Partitioning
- Two-step Partitioning

Input Data Requirement
- Each line corresponds to a spatial object.
- Tab separated file format with a fixed geometry field (and/or temporal field) in Well-Known Text format.

Installation
- Install all dependencies as listed in dependencies/README.md
- Execute the following commands: `cd installer` and  `./installhadoopgis.sh` 
- Copy the content of directory `built` to the same path on every node on the cluster (to distributed the libraries).
- Modify your `LD_LIBRARY_PATH` environment variable in `~/.bashrc` to include the above `built` path.

Program Execution
- Invoke `build/bin/queryproc --help` for the supports. 
- Similarly for 3D and spatio-temporal support, invoke `build/bin/queryproc3d --help` and `build/bin/queryproc_3d --help`.

```
Options:
  --help                    This help message
  -q [ --querytype ] arg    Query type [ partition | contaiment | spjoin]
  -p [ --bucket ] arg       Fine-grain level tile size for spjoin
  --blocksize arg           Fine-grain level bucket size for partitioning
                            (loading data)
  --roughbucket arg         Rough level bucket size for partitioning
  -a [ --input1 ] arg       HDFS file path to data set 1
  -b [ --input2 ] arg       HDFS file path to data set 2
  --mbb1 arg                HDFS path to MBBs of data set 1
  --mbb2 arg                HDFS path to MBBs of data set 2
  -i [ --geom1 ] arg        Field number of data set 1 containing the geometry
  -j [ --geom2 ] arg        Field number of data set 2 containing the geometry
  -d [ --distance ] arg     Distance (used for certain predicates)
  -f [ --outputfields ] arg Fields to be included in the final output separated
                            by commas. See the full documentation. Regular
                            fields from datasets are in the format
                            datasetnum:fieldnum, e.g. 1:1,2:3,1:14. Field
                            counting starts from 1. Optional statistics
                            include: area1, area2, union, intersect, jaccard,
                            dice, mindist
  -h [ --outputpath ] arg   Output path
  --containfile arg         User file containing window used for containment
                            query
  --containrange arg        Comma separated list of window used for containment
                            query
  -t [ --predicate ] arg    Predicate for spatial join and nn queries [
                            st_intersects | st_touches | st_crosses |
                            st_contains | st_adjacent | st_disjoint | st_equals
                            | st_dwithin | st_within | st_overlaps | st_nearest
                            | st_nearest2 ]
  -s [ --samplingrate ] arg Sampling rate (0, 1]
  -u [ --partitioner ] arg  Partitioning method [fg | bsp hc | str | bos | slc
                            | qt ]
  -v [ --partitioner2 ] arg (Optional) Partitioning for second method [fg | bsp
                            | hc | str | bos | slc | qt ]
  -o [ --overwrite ]        Overwrite existing hdfs directories
  -z [ --parapartition ]    Use 2 partitioning steps
  -n [ --numreducers ] arg  The number of reducers
  --removetmp               Remove temporary directories on HDFS
  --removembb               Remove MBB directory on HDFS.
```
To visualize partition boundaries and/or objects, invoke `build/bin/partiton_vis --help`. Note that this might not work well on data set with a large number of objects or very large number of partitions.
```
Options:
  --help                     this help message
  -p [ --partidxfile ] arg   File name of the partition boundaries in MBB
                             format. Partition boundaries have random colors.
  -f [ --offsetpartidx ] arg Field offset for the start of MBB fields. Default
                             is 1
  -q [ --objfile ] arg       File name of the objects in MBB format. Objects
                             have black border.
  -s [ --spacefile ] arg     File containing the global space information.
                             Global space info should have the same format as
                             partition boundary. If none is specified, the
                             max/min of partition boundaries will be used as
                             the global space.
  -o [ --outputname ] arg    Name of the output file. Should contain .png
                             extension.
  ```
