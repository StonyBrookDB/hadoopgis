This directory contains source code and make file that compiles all binary files used by hadoopgis system in the pipeline.

List of programs:
```
queryproc queryproc3d resque resque3d
map_obj_to_tile map_obj_to_tile_3d get_space_dimension
get_space_dimension_3d
mbb_normalizer_3d collect_tile_stat combine_stat
duplicate_remover compute_partition_size
sampler hc bsp fg bos fg3d str slc
```

To compile a source code after modification:
```make program_name IFDEFGLAG```

E.g. 

```make queryproc DEBUG=1```

```make resque DEBUG=1 DEBUGTIME=1```

```make fg```

The compiled binaries are stored in `build/bin` directory.
