#include <iostream>
#include <sstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <vector>
#include <list>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Geometry.h>
#include <geos/operation/distance/DistanceOp.h>
#include <geos/geom/Point.h>

using namespace std;

/* Placeholder for nearest neighbor ranks */
struct query_nn_dist {
	int object_id;
	double distance;
} query_nn_dist;


/* Struct to hold temporary values */
struct query_temp {
	double area1;
	double area2;
	double union_area;
	double intersect_area;
	double dice;
	double jaccard;
	double distance;

	std::stringstream stream;
	std::string tile_id;

	/* Bucket information */
	double bucket_min_x;
	double bucket_min_y;
	double bucket_max_x;
	double bucket_max_y;

	/* Data current to the tile/bucket */
	std::map<int, std::vector< std::vector<string> > > rawdata;
	std::map<int, std::vector<geos::geom::Geometry*> > polydata;

	/* Nearest neighbor temporary placeholders */
	std::list<struct query_nn_dist*> nearest_distances;
} sttemp;


/* Query operator */
struct query_op {
	bool use_cache_file;
	char* cachefilename;

	int JOIN_PREDICATE; /* Join predicate - see resquecommon.h for the full list*/
	int shape_idx_1; /* geometry field number of the 1st set */
	int shape_idx_2; /* geometry field number of the 2nd set */
	int join_cardinality; /* Join cardinality */
	double expansion_distance; /* Distance used in dwithin and k-nearest query */
	int k_neighbors; /* k - the number of neighbors used in k-nearest neighbor */
	/* the number of additional meta data fields
	 i.e. those first fields not counting towards object data
	*/

	int sid_second_set; // set id for the "second" dataset.
	// Value == 2 for joins between 2 data sets
	// Value == 1 for selfjoin

	/* Mapping-specific */
	map<int, string> id_tiles; // Mapping of tile names
	map<int, geos::geom::Geometry*> geom_tiles; // mapping of actual geomery for tiles
	map<int, long> count_tiles; // mapping of actual geomery for tiles
	char* prefix_1; // directory prefix for the files from dataset 1
	char* prefix_2; // directory prefix for the files from dataset 2

	/* Reducing-specific - RESQUE */
	int offset; // offset/adjustment for meta data field in RESQUE
	vector<int> proj1; /* Output fields for 1st set  */
	vector<int> proj2; /* Output fields for 2nd set */

	/* Output fields -  parallel arrays/lists */
	vector<int> output_fields; // fields to the output
	// e.g. 1 is field #1, 2 is fiend #2, the dataset they belong to 
	// is stored in corresponding position 
	//in the the output_fields_set_id list 
	vector<int> output_fields_set_id; // meta information to indicate fields to output

	/* Indicate whether symmetric result pair should be included or not */	
	bool result_pair_duplicate;

	/* Indicate whether to use the geographical distance for points on earth */
	bool use_earth_distance;

	/* Special bits to indicate whether statistics/fields to compute */
	bool needs_area_1;
	bool needs_area_2;
	bool needs_union;
	bool needs_intersect;
	bool needs_dice;
	bool needs_jaccard;
	bool needs_min_distance;
} stop;
