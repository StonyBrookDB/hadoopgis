
#include <iostream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib>
#include <getopt.h>
#include <time.h>

#include <sys/time.h>

#include <geos/geom/PrecisionModel.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Geometry.h>
#include <geos/operation/distance/DistanceOp.h>
#include <geos/geom/Point.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include <geos/opBuffer.h>
#include <spatialindex/SpatialIndex.h>

#include <progparams/string_constants.h>
#include <utilities/tokenizer.h>

// Constants used for building the R-tree
#define FillFactor 0.9
#define IndexCapacity 10 
#define LeafCapacity 50
#define COMPRESS true
#include <indices/rtree_builder_2d.hpp>

// Constants
#include <progparams/resque_constants_2d.h>

// Program parameters
#include <progparams/resque_params_2d.hpp>

/* Function protoypes */
bool build_index_tiles(SpatialIndex::IStorageManager * &storage, SpatialIndex::ISpatialIndex * &spidx);
bool process_input(const int join_idx, const int geom_idx, 
		SpatialIndex::IStorageManager * &storage, SpatialIndex::ISpatialIndex * &spidx);

