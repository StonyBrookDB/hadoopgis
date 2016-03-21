/* This contains headers (dependencies) of RESQUE spatial processing engine */
#include <iostream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib>
#include <getopt.h>
#include <time.h>

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


// Extensions
#include <extensions/specialmeasures/pathology_metrics.h>
#include <extensions/specialmeasures/geographical.h>


/* Function prototypes */
void init(struct query_op &stop, struct query_temp &sttemp);
int join_bucket(struct query_op &stop, struct query_temp &sttemp);
int execute_query(struct query_op &stop, struct query_temp &sttemp);
int execute_query_cache_file(struct query_op &stop, struct query_temp &sttemp);
int read_cache_file(struct query_op &stop, struct query_temp &sttemp);
void release_mem(struct query_op &stop, struct query_temp &sttemp, int maxCard);
void obtain_field(struct query_op &stop, struct query_temp &sttemp, 
	int position, int pos1, int pos2);
void obtain_field(struct query_op &stop, struct query_temp &sttemp, 
	int position, vector<std::string> &set1fields, int pos2);
void update_nn(struct query_op &stop, struct query_temp &sttemp, 
	int object_id, double distance);
//void update_bucket_dimension(const geos::geom::Envelope * env);
double get_distance(const geos::geom::Point* p1, const geos::geom::Point* p2);
double get_distance_earth(const geos::geom::Point* p1,const geos::geom::Point* p2);
//bool build_index_geoms();
//using namespace SpatialIndex;
//using namespace geos;
//using namespace geos::geom;

//bool build_index_geoms(map<int,Geometry*> & geom_polygons, ISpatialIndex* & spidx, IStorageManager* & storage);
bool build_index_geoms(std::map<int, geos::geom::Geometry*> & geom_polygons, 
	SpatialIndex::ISpatialIndex * &spidx, 
	SpatialIndex::IStorageManager * &storage);

bool join_with_predicate(struct query_op &stop, struct query_temp &sttemp,
		const geos::geom::Geometry * geom1 , const geos::geom::Geometry * geom2,
		const geos::geom::Envelope * env1, const geos::geom::Envelope * env2, const int jp);
void report_result(struct query_op &stop, struct query_temp &sttemp, int i, int j);
void report_result(struct query_op &stop, struct query_temp &sttemp, 
	vector<string> &set1fields, int j, bool skip_window_data);
int join_bucket_spjoin(struct query_op &stop, struct query_temp &sttemp);
int join_bucket_knn(struct query_op &stop, struct query_temp &sttemp);
void update_nn(struct query_op &stop, struct query_temp &sttemp, int object_id, double distance);
void update_bucket_dimension(struct query_op &stop, struct query_temp &sttemp, 
	const geos::geom::Envelope * env);
