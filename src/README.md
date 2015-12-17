This directory contains source code and make file that compiles all binary files used by hadoopgis system in the pipeline.

List of programs:
```
queryproc queryproc3d queryproc_spt
resque resque3d resque_spt
map_obj_to_tile map_obj_to_tile_3d map_obj_to_tile_spt
get_space_dimension  get_space_dimension_3d get_space_dimension_spt
mbb_normalizer_3d 
collect_tile_stat combine_stat
duplicate_remover 
compute_partition_size
sampler 
hc bsp fg bos str slc qt
fg3d oc
partition_vs
```

To compile a source code after modification.
```make program_name [IFDEFFLAG]```

Use no `IFDEFFLAG` if you will be using executables in Hadoop. For local system debugging, the following flags are available:
`DEBUG`, `DEBUGAREA`, `DEBUGSTAT`.

E.g.

```make queryproc DEBUG=1```

```make resque DEBUG=1 DEBUGTIME=1```

```make fg```

The compiled binaries are stored in `build/bin` directory.
List of directories and their contents:
- `boundaryhandler`	Source code of executables used in handling boundary objects and a substitute for system `cat` command.
- `common` Header files that are used by multiple programs.
- `controller` Source code of executables that manage the query pipeline.
- `extensions` Additional header files used for 3-D and spatiotemporal extensions, as well as special measures required by different applications.
- `partitioner`	Source code of executables that partition the data and generate the tile boundaries. Each partitioning method has its own sub-directory.
- `sampler` Source code of executables that sample the input data.
- `scripts`	Miscellanous shell scripts.
- `spatialproc` Source code of the core spatial and spatio-temporal processing. Contains primary two categories of executables: mapping and handling individual objects (`map_obj_to_tile` prefix), and performing spatial computation and aggregation between objects (`resque` prefix).
- `visualizer` Source code of visualizer program `partition_vis` that generates `png` images of partition boundaries and objects.



