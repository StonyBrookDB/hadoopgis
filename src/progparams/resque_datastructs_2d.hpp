#include <string>
#include <iostream>
#include <map>
#include <vector>
#include <sstream>

#include <geos/geom/PrecisionModel.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Geometry.h>
#include <geos/operation/distance/DistanceOp.h>
#include <geos/geom/Point.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include <geos/opBuffer.h>
#include <spatialindex/SpatialIndex.h>

const int OSM_SRID = 4326;
const int ST_INTERSECTS = 1;
const int ST_TOUCHES = 2;
const int ST_CROSSES = 3;
const int ST_CONTAINS = 4;
const int ST_ADJACENT = 5;
const int ST_DISJOINT = 6;
const int ST_EQUALS = 7;
const int ST_DWITHIN = 8;
const int ST_WITHIN = 9;
const int ST_OVERLAPS = 10;
const int ST_NEAREST = 11;
const int ST_NEAREST_2 = 12;

const std::string PARAM_PREDICATE_INTERSECTS = "st_intersects";
const std::string PARAM_PREDICATE_TOUCHES = "st_touches";
const std::string PARAM_PREDICATE_CROSSES = "st_crosses";
const std::string PARAM_PREDICATE_CONTAINS = "st_contains";
const std::string PARAM_PREDICATE_ADJACENT = "st_adjacent";
const std::string PARAM_PREDICATE_DISJOINT = "st_disjoint";
const std::string PARAM_PREDICATE_EQUALS = "st_equals";
const std::string PARAM_PREDICATE_DWITHIN = "st_dwithin";
const std::string PARAM_PREDICATE_WITHIN = "st_within";
const std::string PARAM_PREDICATE_OVERLAPS = "st_overlaps";
const std::string PARAM_PREDICATE_NEAREST = "st_nearest";
const std::string PARAM_PREDICATE_NEAREST_NO_BOUND = "st_nearest2";

const int SID_1 = 1;
const int SID_2 = 2;
const int SID_NEUTRAL = 0;

const int STATS_AREA_1 = -1;
const int STATS_AREA_2 = -2;
const int STATS_UNION_AREA = -3;
const int STATS_INTERSECT_AREA = -4;
const int STATS_JACCARD_COEF = -5;
const int STATS_DICE_COEF = -6;
const int STATS_TILE_ID = -7;
const int STATS_MIN_DIST = -8;

const std::string PARAM_STATS_AREA_1 = "area1";
const std::string PARAM_STATS_AREA_2 = "area2";
const std::string PARAM_STATS_UNION_AREA = "union";
const std::string PARAM_STATS_INTERSECT_AREA = "intersect";
const std::string PARAM_STATS_JACCARD_COEF = "jaccard";
const std::string PARAM_STATS_DICE_COEF = "dice";
const std::string PARAM_STATS_TILE_ID= "tileid";
const std::string PARAM_STATS_MIN_DIST = "mindist";

const std::string STR_SET_DELIM = ":";
const std::string STR_OUTPUT_DELIM = ",";


/* Placeholder for nearest neighbor rankings */
struct query_nn_dist {
	int object_id;
	double distance;
};


/* Struct to hold temporary values during spatial operations */
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
	std::map<int, std::vector< std::vector<std::string> > > rawdata;
	std::map<int, std::vector<geos::geom::Geometry*> > polydata;

	/* Nearest neighbor temporary placeholders */
	std::list<struct query_nn_dist*> nearest_distances;
};


/* Query operator */
struct query_op {
	bool use_cache_file;
	bool reading_mbb; // input is mbb
	char* cachefilename;

	int join_predicate; /* Join predicate - see resquecommon.h for the full list*/
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
	bool extract_mbb;
	bool collect_mbb_stat;
	bool use_sampling;
	double sample_rate;
	bool drop_join_idx; // Does not append join index when emitting object to tile

	//map<int, geos::geom::Geometry*> geom_tiles; // mapping of actual geomery for tiles
	map<int, long> count_tiles; // mapping of actual geomery index for tiles
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
};

// Function prototypes defined in resque_params
void usage();
void set_projection_param(char * arg);
int get_join_predicate(char * predicate_str);
bool extract_params(int argc, char** argv, struct query_op & stop, struct query_temp &sttemp);
