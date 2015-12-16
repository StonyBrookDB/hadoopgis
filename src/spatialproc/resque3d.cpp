#include <iostream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <getopt.h>
#include <float.h>
#include <time.h>
//#include <CGAL/Exact_predicates_inexact_constructions_kernel.h>
#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Polyhedron_3.h>
#include <CGAL/IO/Polyhedron_iostream.h>
#include <CGAL/box_intersection_d.h>
#include <CGAL/Bbox_3.h>
#include <CGAL/intersections.h>
#include <CGAL/Timer.h>
#include <algorithm>
#include <vector>
#include <boost/algorithm/string/replace.hpp>
#include <spatialindex/SpatialIndex.h>
#include "../common/string_constants.h"
#include "../common/tokenizer.h"
#include "../common/resque_constants.h"
#include "../common/spatialproc3d_structs.h"
#include "../common/spatialproc3d_params.h"
// #include "../common/rtree_traversal.h"
#include "../common/spatialproc3d_functions.hpp"
#include "../extensions/rtree3d/rtree_traversal3d.h"

#include <CGAL/Nef_polyhedron_3.h>
#include <CGAL/Triangulation_3.h>
#include <CGAL/convex_decomposition_3.h> 
#include <CGAL/Tetrahedron_3.h>

#define NUMBER_DIMENSIONS 3

typedef CGAL::Bbox_3                                     Bbox;
//typedef CGAL::Exact_predicates_inexact_constructions_kernel  Kernel;
typedef CGAL::Exact_predicates_exact_constructions_kernel  Kernel;
typedef Kernel::Triangle_3                               Triangle;
typedef Kernel::Segment_3                                Segment;
typedef CGAL::Polyhedron_3<Kernel>                       Polyhedron;
typedef Polyhedron::Halfedge_const_handle                Halfedge_const_handle;
typedef Polyhedron::Facet_const_iterator                 Facet_const_iterator;
typedef Polyhedron::Facet_const_handle                   Facet_const_handle;
typedef Polyhedron::Vertex_iterator 			Vertex_iterator;
typedef CGAL::Box_intersection_d::Box_with_handle_d<double, 3, Facet_const_handle>               Box;

typedef CGAL::Nef_polyhedron_3<Kernel> Nef_polyhedron;
typedef Kernel::Vector_3  Vector_3;
typedef CGAL::Triangulation_3<Kernel> Triangulation;
typedef Triangulation::Point        CGAL_Point;
typedef Triangulation::Tetrahedron 	Tetrahedron;
typedef Nef_polyhedron::Volume_const_iterator Volume_const_iterator;

/* 
 * RESQUE 3D processing engine v0.8
 *   1) parseParameters
 *   3) for every input line in the current tile
 *         an input line represents an object
 *         save geometry and original data in memory
 *         execute join operation when finish reading a tile
 *   4) Join operation between 2 sets or a single set
 *         build etree index on the second data set
 *         for every object in the first data set
 *            using Rtree index of the second data set
 *              check for MBR/envelope intersection
 *              output the pair result or save pair statistics
 * */

using namespace SpatialIndex;


/* Performance metrics */
clock_t start_reading_data;
clock_t start_query_exec;

clock_t total_reading;
clock_t total_query_exec;

/* Function prototypes */
void init();
void print_stop();
int join_bucket();
int execute_query();
void read_cache_file();
void release_mem(int maxCard);
void report_result(int i, int j);
void obtain_field(int position, int pos1, int pos2);
void update_nn(int object_id, double distance);
// void update_bucket_dimension(const struct mbb_3d * env);
bool build_index_geoms(map<int,Polyhedron*> & geom_polygons, ISpatialIndex* & spidx, IStorageManager* & storage);

/* Externed function from other files */
extern void set_projection_param(char * arg);
extern bool extract_params(int argc, char** argv );
extern int get_join_predicate(char * predicate_str);
extern void usage();
extern bool build_index_geoms(int sid, ISpatialIndex* & spidx, IStorageManager* & storage);
extern double get_volume(Nef_polyhedron &inputpoly);

extern double compute_jaccard(double union_area, double intersection_area);
extern double compute_dice(double area1, double area2, double intersection_area);
extern double to_radians(double degrees);
extern double earth_distance(double lat1, double lng1, double lat2, double lng2);


/* Initialize default values in query structs (operator and temporary placeholders) */
/* To be potentially removed to adjust for initialization already 
 * 	been done in param extraction method */
void init(){
	stop.use_cache_file = false;
	// initlize query operator 
	stop.expansion_distance = 0.0;
	stop.k_neighbors = 0;
	stop.JOIN_PREDICATE = 0;
	stop.shape_idx_1 = 0;
	stop.shape_idx_2 = 0 ;
	stop.join_cardinality = 0;
	stop.offset = 3; // default format or value for offset

	stop.prefix_1 = NULL;
	stop.prefix_2 = NULL;

	stop.needs_area_1 = false;
	stop.needs_area_2 = false;
	stop.needs_union = false;
	stop.needs_intersect = true;
	stop.needs_dice = false;
	stop.needs_jaccard = false;

	stop.result_pair_duplicate = true;

	stop.use_earth_distance = false;

	stop.output_fields.clear();
	stop.output_fields_set_id.clear();

	sttemp.nearest_distances.clear();
	
	sttemp.area1 = -1;
	sttemp.area2 = -1;
	sttemp.union_area = -1;
	sttemp.intersect_area = -1;
	sttemp.dice = -1;
	sttemp.jaccard = -1;
	sttemp.distance = -1;	
}


int execute_query()
{
	// Processing variables
	string input_line; // Temporary line
	vector<string> fields; // Temporary fields
	int sid = 0; // Join index ID for the current object
	int index = -1;  // Polyhedron field position for the current object
	string tile_id = ""; // The current tile_id
	string previd = ""; // the tile_id of the previously read object
	int tile_counter = 0; // number of processed tiles

	/* CGAL variables for spatial computation */
	Polyhedron *poly = NULL;
	struct mbb_3d *mbb_ptr = NULL;

	/* Define the resource when using cache-file  */
	int maxCardRelease = min(stop.join_cardinality, stop.use_cache_file ? 1 : 2);

	#ifdef DEBUG
	std::cerr << "Bucket info:[ID] |A|x|B|=|R|" <<std::endl;
	#endif  

	#ifdef DEBUGTIME
	start_reading_data = clock();
	#endif

	/*
	if (stop.use_cache_file) {
		read_cache_file();
	}
	*/
	stringstream ss;

	while (cin && getline(cin, input_line) && !cin.eof()) {
		tokenize(input_line, fields, TAB, true);
	
		tile_id = fields[0];
		sid = atoi(fields[1].c_str());

		switch (sid) {
			case SID_1:
				index = stop.shape_idx_1 ; 
				break;
			case SID_2:
				index = stop.shape_idx_2 ; 
				break;
			default:
				cerr << "wrong sid : " << sid << endl;
				return false;
		}
		

		/* Parsing fields from input */
		try { 
			// Parsing MBB
			mbb_ptr = new struct mbb_3d();
			for (int k = 0; k < NUMBER_DIMENSIONS; k++) {
				mbb_ptr->low[k] = stod(fields[3 + k]);
			}
			for (int k = 0; k < NUMBER_DIMENSIONS; k++) {
				mbb_ptr->high[k] = stod(fields[6 + k]);
			}

			/* Below code achieves the same thing
			mbb_ptr->low[0] = atoi(fields[3]);
			mbb_ptr->low[1] = atoi(fields[4]);
			mbb_ptr->low[2] = atoi(fields[5]);
			mbb_ptr->high[0] = atoi(fields[6]);
			mbb_ptr->high[1] = atoi(fields[7]);
			mbb_ptr->high[2] = atoi(fields[8]);
			*/
			#ifdef DEBUG
			cerr << "MBB: ";
			for (int k = 0; k < NUMBER_DIMENSIONS; k++) {
				cerr << TAB << mbb_ptr->low[k];
			}
			for (int k = 0; k < NUMBER_DIMENSIONS; k++) {
				cerr << TAB << mbb_ptr->high[k];
			}
			cerr << endl;
			#endif
		}
		catch (...) {
			cerr << "******MBB Parsing Error******" << endl;
			return -1;
		}
		
		try { 
			// Parsing Geometry
			poly = new Polyhedron();
			boost::replace_all(fields[9], BAR, "\n");
			ss.str(fields[9]);	
			//cout << input_line << endl;
			ss >> *poly;
//			cout << ss.str() << endl;
			// extracting MBB information
			// mbb_ptr = get_mbb(poly);
			
		}
		catch (...) {
			cerr << "******Polyhedron Parsing Error******" << endl;
			return -1;
		}

		/* Process the current tile (bucket) when finishing reading all objects belonging
		   to the current tile */
		if (previd.compare(tile_id) != 0 && previd.size() > 0 ) {
			#ifdef DEBUGTIME
			total_reading += clock() - start_reading_data;
			start_query_exec = clock();
			#endif

			sttemp.tile_id = previd;
			int pairs = join_bucket(); // number of satisfied predicates

			#ifdef DEBUGTIME
			total_query_exec += clock() - start_query_exec;
			start_reading_data = clock();
			#endif


			#ifdef DEBUG
			cerr <<"T[" << previd << "] |" << sttemp.polydata[SID_1].size() 
				<< "|x|" << sttemp.polydata[stop.sid_second_set].size() 
				<< "|=|" << pairs << "|" << endl;
			#endif
			tile_counter++; 
			release_mem(maxCardRelease);
		}

		// populate the bucket for join 
		sttemp.polydata[sid].push_back(poly);
		sttemp.mbbdata[sid].push_back(mbb_ptr);
		fields.pop_back(); // Remove the last geometry field to save space
		sttemp.rawdata[sid].push_back(fields);

		/* Update the field */
		//cerr << "Is valid: " <<endl;
		//cout << poly->size_of_vertices() << endl;
		previd = tile_id; 
		fields.clear();
	}

	#ifdef DEBUGTIME
	total_reading += clock() - start_reading_data;
	start_query_exec = clock();
	#endif

	// Process the last tile (what remains in memory)
	sttemp.tile_id = tile_id;
	int  pairs = join_bucket();

	#ifdef DEBUGTIME
	total_query_exec += clock() - start_query_exec;
	start_reading_data = clock();
	#endif

	#ifdef DEBUG
	cerr <<"T[" << previd << "] |" << sttemp.polydata[SID_1].size() << "|x|" 
		<< sttemp.polydata[stop.sid_second_set].size() 
		<< "|=|" << pairs << "|" << endl;
	#endif  
	tile_counter++;

	release_mem(stop.join_cardinality);
	

	return tile_counter;
}


/* Release objects in memory (for the current tile/bucket) */
void release_mem(int maxCard) {
	if (stop.join_cardinality <= 0) {
		return ;
	}

  	for (int j = 0; j < stop.join_cardinality && j < maxCard; j++ ) {
    		int delete_index = j + 1; // index are adjusted to start from 1
    		int len = sttemp.polydata[delete_index].size();

    		for (int i = 0; i < len ; i++) {
   //   			(*sttemp.polydata[delete_index][i]).clear();
      			delete sttemp.polydata[delete_index][i];

      			delete sttemp.mbbdata[delete_index][i];

			sttemp.rawdata[delete_index][i].clear();
		}
    		sttemp.polydata[delete_index].clear();
    		sttemp.rawdata[delete_index].clear();
		sttemp.mbbdata[delete_index].clear();
  	}
}

/* Read in data in cache file into rawdata and polydata */
void read_cache_file() {
	string input_line; // Temporary line
	vector<string> fields; // Temporary fields
	int sid;
	string tile_id;
	ifstream input(stop.cachefilename);

	while(!input.eof()) {
		getline(input, input_line);
		tokenize(input_line, fields, TAB, true);
		sid = atoi(fields[1].c_str());
		tile_id = fields[0];
		/**************** To be completed in newer version */
	}

}

/* Perform spatial computation between 2 geometry objects */
bool join_with_predicate(Polyhedron * geom1 , Polyhedron * geom2, 
		const struct mbb_3d * env1, const struct mbb_3d * env2,
		const int jp){
	bool flag = false; // flag == true means the predicate is satisfied 
	
	Polyhedron* geom_buffer1;
	Polyhedron* geom_buffer2;
	Polyhedron* geomUni;
	Polyhedron* geomIntersect; 

	switch (jp){
		case ST_INTERSECTS:
			//flag = intersects(env1, env2) && intersects(geom1, geom2);
			flag = intersects(geom1, geom2);
			break;


		case ST_EQUALS:
			flag = false;
//			flag = env1->equals(env2) && geom1->equals(geom2);
			break;

		default:
			cerr << "ERROR: unknown spatial predicate " << endl;
			break;
	}
	/* Spatial computation is only performed once for a result pair */

	if (flag) {
		if (stop.needs_volume_1) {
			Nef_polyhedron N1(*geom1);
			sttemp.volume1 = get_volume(N1);
		}

		if (stop.needs_volume_2) {	
			Nef_polyhedron N2(*geom2);
			sttemp.volume2 = get_volume(N2);
		}

		if (stop.needs_intersect_volume) {
			  if((*geom1).is_closed() && (*geom2).is_closed()) {
				Nef_polyhedron N1(*geom1);
				Nef_polyhedron N2(*geom2);
				Nef_polyhedron Inter = N1 * N2;
				    
				if(Inter.number_of_vertices() > 0) {	
				   	sttemp.intersect_volume = get_volume(Inter);
				}
				else
					cerr << "ERROR: Polyhedrons are not intersected!" << endl;
 
				Inter.clear();
  			}
			else
				cerr << "ERROR: Polyhedron is not closed!" << endl;

		}

	}
/*
		if (stop.needs_area_1) {
			sttemp.area1 = geom1->getArea();
		}
		if (stop.needs_area_2) {
			sttemp.area2 = geom2->getArea();
		}
		if (stop.needs_union) {
			Polyhedron * geomUni = geom1->Union(geom2);
			sttemp.union_area = geomUni->getArea();
			delete geomUni;
		}
		if (stop.needs_intersect) {
			Polyhedron * geomIntersect = geom1->intersection(geom2);
			sttemp.intersect_area = geomIntersect->getArea();
			delete geomIntersect;
		}
*/
		/* Statistics dependent on previously computed statistics */

		/*
		if (stop.needs_jaccard) {
			sttemp.jaccard = compute_jaccard(sttemp.union_area, sttemp.intersect_area);
		}

		if (stop.needs_dice) {
			sttemp.dice = compute_dice(sttemp.area1, sttemp.area2, sttemp.intersect_area);
		}

		if (stop.needs_min_distance) {
			if (stop.use_earth_distance
				&& geom1->getPolyhedronTypeId() == geos::geom::GEOS_POINT
				&& geom2->getPolyhedronTypeId() == geos::geom::GEOS_POINT) 				{
				sttemp.distance = get_distance_earth(
						dynamic_cast<const geos::geom::Point*>(geom1),
						dynamic_cast<const geos::geom::Point*>(geom2)); 
			}
			else {
				sttemp.distance = DistanceOp::distance(geom1, geom2);
			}
		}
	}

		*/
	return flag; 
}


/* Update the spatial dimension of the bucket */
/*
void update_bucket_dimension(struct mbb_3d *env) {
	if (sttemp.bucket_min_x >= env->min_x) {
		sttemp.bucket_min_x = env->min_x;
	}
	if (sttemp.bucket_min_y >= env->min_y) {
		sttemp.bucket_min_y = env->min_y;
	}
	if (sttemp.bucket_min_z >= env->min_z) {
		sttemp.bucket_min_z = env->min_z;
	}
	if (sttemp.bucket_max_x <= env->max_x) {
		sttemp.bucket_max_x = env->max_x;
	}
	if (sttemp.bucket_max_y <= env->max_y) {
		sttemp.bucket_max_y = env->max_y;
	}
	if (sttemp.bucket_max_z <= env->max_z) {
		sttemp.bucket_max_z = env->max_z;
	}
}
*/

/* Update the nearest neighbor list and the distances to them */
void update_nn(int object_id, double distance)
{
	list<struct query_nn_dist *>::iterator it;
	bool new_inserted = false;
	struct query_nn_dist * tmp;

	for (it = sttemp.nearest_distances.begin(); 
		it != sttemp.nearest_distances.end(); it++) {
		if ((*it)->distance > distance) {
			/* Insert the new distance in */
			tmp = new struct query_nn_dist();
			tmp->distance = distance;
			tmp->object_id = object_id;
			sttemp.nearest_distances.insert(it, tmp);
			new_inserted = true;
			break;
		}
    	}

	if (sttemp.nearest_distances.size() > stop.k_neighbors) {
		// the last element is the furthest from all tracked nearest neighbors
		tmp = sttemp.nearest_distances.back();
		sttemp.nearest_distances.pop_back(); 
		delete tmp;
	} else if (!new_inserted && 
		sttemp.nearest_distances.size() < stop.k_neighbors) {
		/* Insert at the end */
		tmp = new struct query_nn_dist();
		tmp->distance = distance;
		tmp->object_id = object_id;
		sttemp.nearest_distances.insert(it, tmp);
	}
}

/* Report result separated by separator */
void report_result(int i, int j)
{
	sttemp.stream.str("");
	sttemp.stream.clear();
	/* ID used to access rawdata for the "second" data set */

	if (stop.output_fields.size() == 0) {
		/* No output fields have been set. Print all fields read */
		for (int k = 0; k < sttemp.rawdata[SID_1][i].size(); k++) {
			sttemp.stream << sttemp.rawdata[SID_1][i][k] << SEP;
		}
		for (int k = 0; k < sttemp.rawdata[SID_1][j].size(); k++) {
			sttemp.stream << SEP << sttemp.rawdata[stop.sid_second_set][j][k];
		}
	}
	else {
		/* Output fields are listed */
		int k = 0;
		for (; k < stop.output_fields.size() - 1; k++) {
	//		cerr << "outputting fields " << stop.output_fields[k];
			obtain_field(k, i, j);
			sttemp.stream << SEP;
		}
		obtain_field(k, i, j);

	//		cerr << "outputting fields " << stop.output_fields[k];
	}

	sttemp.stream << endl;
	//cout << "Result pair:" << TAB << i << TAB << j << ".	Intersected volume: "<<sttemp.intersect_volume << endl;
	cout << sttemp.stream.str();

}

/* Output the field at given position  */
void obtain_field(int position, int pos1, int pos2)
{
	//cerr << "Set id" << stop.output_fields_set_id[position] << endl;
	if (stop.output_fields_set_id[position] == SID_1) {
		sttemp.stream << sttemp.rawdata[SID_1][pos1][stop.output_fields[position]];	
	}
	else if (stop.output_fields_set_id[position] == SID_2) {
		sttemp.stream << sttemp.rawdata[stop.sid_second_set][pos2][stop.output_fields[position]];	
	}
	else if (stop.output_fields_set_id[position] == SID_NEUTRAL) {
		switch (stop.output_fields[position]) {
			case STATS_AREA_1:
				sttemp.stream << sttemp.area1;	
				break;
			case STATS_AREA_2:
				sttemp.stream << sttemp.area2;	
				break;
			case STATS_UNION_AREA:
				sttemp.stream << sttemp.union_area;
				break;
			case STATS_INTERSECT_AREA:
				sttemp.stream << sttemp.intersect_area;
				break;
			case STATS_JACCARD_COEF:
				sttemp.stream << sttemp.jaccard;
				break;
			case STATS_DICE_COEF:
				sttemp.stream << sttemp.dice;
				break;
			case STATS_TILE_ID:
				sttemp.stream << sttemp.tile_id;
				break;
			case STATS_MIN_DIST:
				sttemp.stream << sttemp.distance;
				break;
			case STATS_VOLUME_1:
				sttemp.stream << sttemp.volume1;
				break;
			case STATS_VOLUME_2:
				sttemp.stream << sttemp.volume2;
				break;
			case STATS_INTERSECT_VOLUME:
				sttemp.stream << sttemp.intersect_volume;
				break;
			default:
				return;
		}					
	}
}

/* 
 *  Perform spatial computation on a given tile with data 
 *   located in polydata and rawdata
 *   
 */
int join_bucket()
{
	IStorageManager *storage = NULL;
	ISpatialIndex *spidx = NULL;
	bool selfjoin = stop.join_cardinality == 1  ? true : false;

	/* Indicates where original data is mapped to */
	int idx1 = SID_1; 
	int idx2 = selfjoin ? SID_1 : SID_2;
	
	int pairs = 0; // number of satisfied results

	double low[3], high[3];  // Temporary value placeholders for MBB
	double tmp_distance; // Temporary distance for nearest neighbor query
	double def_search_radius = -1; // Default search radius 
					//for nearest neighbor (NN) with unknown bounds
	double max_search_radius; // max_radius to search for NN

	try { 

		vector<Polyhedron*>  & poly_set_one = sttemp.polydata[idx1];
		vector<Polyhedron*>  & poly_set_two = sttemp.polydata[idx2];
		
		int len1 = poly_set_one.size();
		int len2 = poly_set_two.size();

		#ifdef DEBUG
		cerr << "Bucket size: " << len1 << " joining with " << len2 << endl;
		cerr << "mbbdata size: " << sttemp.mbbdata[idx1].size() 
		<< " joining with " << sttemp.mbbdata[idx2].size() << endl;
		#endif

		if (len1 <= 0 || len2 <= 0) {
			return 0;
		}

		/* Build index on the "second data set */
		map<int, Polyhedron*> geom_polygons2;
		geom_polygons2.clear();


		/* Handling for special nearest neighbor query */	

		// build the actual spatial index for input polygons from idx2
		if (! build_index_geoms(idx2, spidx, storage)) {
			#ifdef DEBUG
			cerr << "Building index on geometries from set 2 has failed" << endl;
			#endif
			return -1;
		}
		for (int i = 0; i < len1; i++) {		
			/* Extract minimum bounding box */
			Polyhedron* geom1 = poly_set_one[i];
			struct mbb_3d * env1 = sttemp.mbbdata[SID_1][i];
			
			low[0] = env1->low[0];
			low[1] = env1->low[1];
			low[2] = env1->low[2];
			high[0] = env1->high[0];
			high[1] = env1->high[1];
			high[2] = env1->high[2];
			/* Handle the buffer expansion for R-tree in 
			the case of Dwithin and nearest neighbor predicate */
			if (stop.JOIN_PREDICATE == ST_NEAREST_2) {
				/* Nearest neighbor when max search radius 
					is not determined */
				stop.expansion_distance = def_search_radius;
				// Initial value
			}
			if (stop.JOIN_PREDICATE == ST_DWITHIN ||
				stop.JOIN_PREDICATE == ST_NEAREST) {
				low[0] -= stop.expansion_distance;
				low[1] -= stop.expansion_distance;
				low[2] -= stop.expansion_distance;
				high[0] += stop.expansion_distance;
				high[1] += stop.expansion_distance;
				high[2] += stop.expansion_distance;
			}

			/* Regular handling */
			Region r(low, high, 3);
			MyVisitor vis;
			vis.matches.clear();
			/* R-tree intersection check */
			spidx->intersectsWithQuery(r, vis);

			
			for (uint32_t j = 0; j < vis.matches.size(); j++) 
			{
				/* Skip results that have been seen before (self-join case) */
				if (selfjoin && ((vis.matches[j] == i) ||  // same objects in self-join
				    (!stop.result_pair_duplicate && vis.matches[j] <= i))) { // duplicate pairs
					#ifdef DEBUG
					cerr << "skipping (selfjoin): " << i << " " << vis.matches[j] << endl;
					#endif
					continue;
				}
				//cerr << "Matches: " << vis.matches[j] << endl;
				
				Polyhedron* geom2 = poly_set_two[vis.matches[j]];
				struct mbb_3d * env2 = sttemp.mbbdata[SID_1][vis.matches[j]];
				/*//cerr << "Got here 4" << endl;
 				if(geom1->is_valid()) cerr << "Geom1 is valid!" << endl;
				if(geom2->is_valid()) cerr << "Geom2 is valid!" << endl;
				//cerr << "P1:" << geom1->size_of_vertices() <<"P2: " << geom2->size_of_vertices()<< endl;*/
				if (join_with_predicate(geom1, geom2, env1, env2,
							stop.JOIN_PREDICATE))  {
					report_result(i, vis.matches[j]);
					pairs++;
				}

			}
	
		}
	} // end of try
	catch (Tools::Exception& e) {
	//catch (...) {
		std::cerr << "******ERROR******" << std::endl;
		#ifdef DEBUG
		cerr << e.what() << std::endl;
		#endif
		return -1;
	} // end of catch
	
	delete spidx;
	delete storage;
	return pairs ;
}

/* main body of the engine */
int main(int argc, char** argv)
{
	int c = 0; // Number of results satisfying the predicate
	init(); // setting the query operator and temporary variables to default

	
	if (!extract_params(argc, argv)) {
		#ifdef DEBUG 
		cerr <<"ERROR: query parameter extraction error." << endl 
		     << "Please see documentations, or contact author." << endl;
		#endif
		usage();
		return 1;
	}
	
	switch (stop.join_cardinality) {
		case 1:
		case 2:
			// adjusting set id
			stop.sid_second_set = stop.join_cardinality == 1 ? SID_1 : SID_2;
			c = execute_query();
			break;

		default:
			#ifdef DEBUG 
			std::cerr <<"ERROR: join cardinality does not match engine capacity." << std::endl ;
			#endif
			return 1;
	}
	if (c >= 0 ) {
		#ifdef DEBUG 
		std::cerr <<"Query Load: [" << c << "]" <<std::endl;
		#endif
	} else {
		#ifdef DEBUG 
		std::cerr <<"Error: ill formatted data. Terminating ....... " << std::endl;
		#endif
		return 1;
	}

	#ifdef DEBUGTIME
	cerr << "Total reading time: " 
		<< (double) total_reading / CLOCKS_PER_SEC 
		<< " seconds." << endl;
	cerr << "Total query exec time: " 
		<< (double) total_query_exec / CLOCKS_PER_SEC 
		<< " seconds." << endl;
	#endif

	cout.flush();
	cerr.flush();
	return 0;
}

