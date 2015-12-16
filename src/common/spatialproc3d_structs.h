#include <iostream>
#include <sstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <vector>
#include <list>
#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Polyhedron_3.h>
#include <CGAL/IO/Polyhedron_iostream.h>
#include <CGAL/box_intersection_d.h>
#include <CGAL/Bbox_3.h>
#include <CGAL/intersections.h>

typedef CGAL::Bbox_3                                     Bbox;
typedef CGAL::Exact_predicates_exact_constructions_kernel  Kernel;
typedef Kernel::Triangle_3                               Triangle;
typedef Kernel::Segment_3                                Segment;
typedef CGAL::Polyhedron_3<Kernel>                       Polyhedron;
typedef Polyhedron::Halfedge_const_handle                Halfedge_const_handle;
typedef Polyhedron::Facet_const_iterator                 Facet_const_iterator;
typedef Polyhedron::Facet_const_handle                   Facet_const_handle;
typedef CGAL::Box_intersection_d::Box_with_handle_d<double, 3, Facet_const_handle>               Box;
typedef Kernel::FT FT;

using namespace std;

/* Placeholder for nearest neighbor ranks */
struct query_nn_dist {
	int object_id;
	double distance;
} query_nn_dist;

struct mbb_3d {
	double low[3];
	double high[3];
};

/* Struct to hold temporary values */
struct query_temp {
	double area1;
	double area2;
	double union_area;
	double intersect_area;
	double dice;
	double jaccard;
	double distance;

	// for 3d
	double volume1;
	double volume2;	
	double intersect_volume;
	
	std::stringstream stream;
	std::string tile_id;

	/* Bucket information */
	double bucket_min_x;
	double bucket_min_y;
	double bucket_min_z;
	double bucket_max_x;
	double bucket_max_y;
	double bucket_max_z;

	/* Data current to the tile/bucket */
	std::map<int, std::vector< std::vector<string> > > rawdata;
	std::map<int, std::vector<struct mbb_3d *> > mbbdata;
//	std::vector<double> mbbdata_vector;
	std::map<int, std::vector<Polyhedron *> > polydata;

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
	bool extract_mbb;
	bool collect_mbb_stat;
	bool use_sampling;
	double sample_rate;

	map<int, string> id_tiles; // Mapping of tile names
	map<int, Polyhedron*> geom_tiles; // mapping of actual geomery for tiles
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

	//for 3d case
	bool needs_volume_1;
	bool needs_volume_2;
	bool needs_intersect_volume;
} stop;

