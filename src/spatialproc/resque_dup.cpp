#include <iostream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <getopt.h>
#include <time.h>
#include "../common/string_constants.h"
#include "../common/tokenizer.h"
#include "../common/resque_constants.h"
#include "../common/spatialproc_structs.h"
#include "../common/spatialproc_params.h"
#include "../common/rtree_traversal.h"
#include "../extensions/specialmeasures/pathology_metrics.h"
#include "../extensions/specialmeasures/geographical.h"
#include <string>
#include <sstream>
#include <vector>
#define KNN_IDX_FILE "knn_idx_file.txt"

/* 
 * RESQUE processing engine v2.0
 *   1) parseParameters
 *   2) readCacheFile - metadata such as partition schemata
 *   3) for every input line in the current tile
 *         an input line represents an object
 *         save geometry and original data in memory
 *         execute join operation when finish reading a tile
 *   4) Join operation between 2 sets or a single set
 *         build Rtree index on the second data set
 *         for every object in the first data set
 *            using Rtree index of the second data set
 *              check for MBR/envelope intersection
 *              output the pair result or save pair statistics
 *   5) Output final statistics (opt)
 *double get_distance_earth(geos::geom::Point * p1, geos::geom::Point * p2) 
 *   Requirement (input files): see the Wiki
 * */


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
void update_bucket_dimension(const Envelope * env);
double get_distance(const geos::geom::Point* p1, const geos::geom::Point* p2);
double get_distance_earth(const geos::geom::Point* p1,const geos::geom::Point* p2);
bool build_index_geoms(map<int,Geometry*> & geom_polygons, ISpatialIndex* & spidx, IStorageManager* & storage);

/* Externed function from other files */
extern void set_projection_param(char * arg);
extern bool extract_params(int argc, char** argv );
extern int get_join_predicate(char * predicate_str);
extern void usage();

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
	stop.needs_intersect = false;
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



std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

void print_list(std::vector<int> distances)
{

        for(std::vector<int>::iterator it = distances.begin(); it!= distances.end(); ++it)
        {
                        cerr << *it << ", ";
        }

    cerr << endl;
}

map<string, vector<int> > read_knn_indexes() {
	map<string, vector<int> > knn_idx_map;
	ifstream myfile (KNN_IDX_FILE);
	string line;
	
	//cerr << "Printing KNN idx file" << endl;
	if (myfile.is_open())
	  {
	    while ( getline (myfile,line) )
	    {
		  //cerr << line << endl;
		  std::vector<std::string> elems;
		  split(line, ':', elems);

		  std::vector<std::string> distances;
		  split(elems[1], ',', distances);

		  std::vector<int> distance_ints;
          for(std::vector<int>::size_type i = 0; i != distances.size(); i++) {
			 int res = 0;
			 sscanf (distances[i].c_str(), "%d", &res);
			 distance_ints.push_back(res);
          }
		  string object_id = elems[0];
		  knn_idx_map[object_id] = distance_ints;
	    }
	    myfile.close();
	  }
	
	 else cerr << "Unable to open file"; 
	 //print_list(distance_ints);
	 return knn_idx_map;
}

map<string, vector<int> > knn_idx_map;
int execute_query()
{
	// Processing variables
	string input_line; // Temporary line
	vector<string> fields; // Temporary fields
	int sid = 0; // Join index ID for the current object
	int index = -1;  // Geometry field position for the current object
	string tile_id = ""; // The current tile_id
	string previd = ""; // the tile_id of the previously read object
	int tile_counter = 0; // number of processed tiles

	/* GEOS variables for spatial computation */
	PrecisionModel *pm = new PrecisionModel();
	GeometryFactory *gf = new GeometryFactory(pm, OSM_SRID); // default is OSM for spatial application
	WKTReader *wkt_reader = new WKTReader(gf);
	Geometry *poly = NULL;

	/* Define the resource when using cache-file  */
	int maxCardRelease = min(stop.join_cardinality, stop.use_cache_file ? 1 : 2);

    if(strcmp(getenv("USE_PRECOMPUTED"), "true") == 0) {
    	knn_idx_map = read_knn_indexes();
    }

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

	while (cin && getline(cin, input_line) && !cin.eof()) {
		//printf("Input line is ----- %s\n", input_line.c_str());
	//	cerr << input_line << endl;
		tokenize(input_line, fields, TAB, true);
		sid = atoi(fields[1].c_str());
		tile_id = fields[0];

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
		
		/* Handling of objects with missing geometry */
		if (fields[index].size() <= 0) 
			continue ; //skip empty spatial object 
		
		#ifdef DEBUG
		cerr << "geometry: " << fields[stop.shape_idx_1]<< endl;
		#endif  

		/* Parsing polygon input */
		try { 
			poly = wkt_reader->read(fields[index]);
		}
		catch (...) {
			cerr << "******Geometry Parsing Error******" << endl;
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
		sttemp.rawdata[sid].push_back(fields);

		/* Update the field */
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
	
	// clean up newed objects
	delete wkt_reader;
	delete gf;
	delete pm;

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
      			delete sttemp.polydata[delete_index][i];
			sttemp.rawdata[delete_index][i].clear();
		}
    		sttemp.polydata[delete_index].clear();
    		sttemp.rawdata[delete_index].clear();
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

/* Create an R-tree index on a given set of polygons */
bool build_index_geoms(map<int,Geometry*> & geom_polygons, ISpatialIndex* & spidx, IStorageManager* & storage) {
	// build spatial index on tile boundaries 
	id_type  indexIdentifier;
	GEOSDataStream stream(&geom_polygons);
	storage = StorageManager::createNewMemoryStorageManager();
	spidx   = RTree::createAndBulkLoadNewRTree(RTree::BLM_STR, stream, *storage, 
			FillFactor,
			IndexCapacity,
			LeafCapacity,
			2, 
			RTree::RV_RSTAR, indexIdentifier);

	// Error checking 
	return spidx->isIndexValid();
}

/* Perform spatial computation between 2 geometry objects */
bool join_with_predicate(const Geometry * geom1 , const Geometry * geom2, 
		const Envelope * env1, const Envelope * env2,
		const int jp){
	bool flag = false; // flag == true means the predicate is satisfied 
	
	BufferOp * buffer_op1;
	BufferOp * buffer_op2;
	Geometry* geom_buffer1;
	Geometry* geom_buffer2;
	Geometry* geomUni;
	Geometry* geomIntersect; 

	
	#ifdef DEBUG
	cerr << "1: (envelope) " << env1->toString() 
		<< " and actual geom: " << geom1->toString()
		<< " 2: (envelope)" << env2->toString() 
		<< " and actual geom: " << geom2->toString() << endl;
	#endif

	switch (jp){
		case ST_INTERSECTS:
			flag = env1->intersects(env2) && geom1->intersects(geom2);
			break;

		case ST_TOUCHES:
			flag = geom1->touches(geom2);
			break;

		case ST_CROSSES:
			flag = geom1->crosses(geom2);
			break;

		case ST_CONTAINS:
			flag = env1->contains(env2) && geom1->contains(geom2);
			break;

		case ST_ADJACENT:
			flag = !geom1->disjoint(geom2);
			break;

		case ST_DISJOINT:
			flag = geom1->disjoint(geom2);
			break;

		case ST_EQUALS:
			flag = env1->equals(env2) && geom1->equals(geom2);
			break;

		case ST_DWITHIN:
			/* Special spatial handling for the point-point case */
			if (geom1->getGeometryTypeId() == geos::geom::GEOS_POINT 
				&& geom2->getGeometryTypeId() == geos::geom::GEOS_POINT) 				{
				/* Replace with spherical distance computation if points are on eath */
				if (stop.use_earth_distance) {
					flag = get_distance_earth(
						dynamic_cast<const geos::geom::Point*>(geom1),
						dynamic_cast<const geos::geom::Point*>(geom2)) 
						<= stop.expansion_distance;
				} else {
					flag = DistanceOp::distance(geom1, geom2)
						<= stop.expansion_distance;
				}

				/* flag = distance(
					dynamic_cast<const geos::geom::Point*>(geom1), 						dynamic_cast<const geos::geom::Point*>(geom2) ) 
					 <= stop.expansion_distance; */
			}
			else {
				// Regular handling for other object types
				buffer_op1 = new BufferOp(geom1);
				// buffer_op2 = new BufferOp(geom2);
				if (NULL == buffer_op1)
					cerr << "NULL: buffer_op1" <<endl;
				geom_buffer1 = buffer_op1->getResultGeometry(stop.expansion_distance);
				env1 = geom_buffer1->getEnvelopeInternal();
				// geom_buffer2 = buffer_op2->getResultGeometry(expansion_distance);
				//Envelope * env_temp = geom_buffer1->getEnvelopeInternal();
				if (NULL == geom_buffer1) {
					cerr << "NULL: geom_buffer1" << endl;
				}
				flag = join_with_predicate(geom_buffer1, geom2, env1, env2, ST_INTERSECTS);
				delete geom_buffer1;
				delete buffer_op1;
			}
			break;

		case ST_WITHIN:
			flag = geom1->within(geom2);
			break; 

		case ST_OVERLAPS:
			flag = geom1->overlaps(geom2);
			break;

		case ST_NEAREST:
		case ST_NEAREST_2:
			/* Execution only reaches here if this is already the nearest neighbor */
			flag = true;
			break;

		default:
			cerr << "ERROR: unknown spatial predicate " << endl;
			break;
	}
	/* Spatial computation is only performed once for a result pair */
	if (flag) {
		if (stop.needs_area_1) {
			sttemp.area1 = geom1->getArea();
		}
		if (stop.needs_area_2) {
			sttemp.area2 = geom2->getArea();
		}
		if (stop.needs_union) {
			Geometry * geomUni = geom1->Union(geom2);
			sttemp.union_area = geomUni->getArea();
			delete geomUni;
		}
		if (stop.needs_intersect) {
			Geometry * geomIntersect = geom1->intersection(geom2);
			sttemp.intersect_area = geomIntersect->getArea();
			delete geomIntersect;
		}
		/* Statistics dependent on previously computed statistics */
		if (stop.needs_jaccard) {
			sttemp.jaccard = compute_jaccard(sttemp.union_area, sttemp.intersect_area);
		}

		if (stop.needs_dice) {
			sttemp.dice = compute_dice(sttemp.area1, sttemp.area2, sttemp.intersect_area);
		}

		if (stop.needs_min_distance) {
			if (stop.use_earth_distance
				&& geom1->getGeometryTypeId() == geos::geom::GEOS_POINT
				&& geom2->getGeometryTypeId() == geos::geom::GEOS_POINT) 				{
				cerr <<"got here" << endl;
				sttemp.distance = get_distance_earth(
						dynamic_cast<const geos::geom::Point*>(geom1),
						dynamic_cast<const geos::geom::Point*>(geom2)); 
			}
			else {
				sttemp.distance = DistanceOp::distance(geom1, geom2);
			}
		}
	}
	return flag; 
}

/* Compute distance between two points using Euclidian distance */
double get_distance(const geos::geom::Point * p1, const geos::geom::Point * p2) 
{	return sqrt(pow(p1->getX() - p2->getX(), 2) 
			+ pow(p1->getY() - p2->getY(), 2));
}

/* Compute geographical distance between two points on earth */
double get_distance_earth(const geos::geom::Point * p1, const geos::geom::Point * p2) 
{
	return earth_distance(p1->getX(), p1->getY(), p2->getX(), p2->getY());
}

/* Update the spatial dimension of the bucket */
void update_bucket_dimension(const Envelope * env) {
	if (sttemp.bucket_min_x >= env->getMinX()) {
		sttemp.bucket_min_x = env->getMinX();
	}
	if (sttemp.bucket_min_y >= env->getMinY()) {
		sttemp.bucket_min_y = env->getMinY();
	}
	if (sttemp.bucket_max_x <= env->getMaxX()) {
		sttemp.bucket_max_x = env->getMaxX();
	}
	if (sttemp.bucket_max_y <= env->getMaxY()) {
		sttemp.bucket_max_y = env->getMaxY();
	}
}

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
			default:
				return;
		}					
	}
}

	
int get_radius_index(int k) {
	int res = (int) sqrt(k);
	//cerr << " k=" << k << ", res is " << res << endl;
	return res;
}

int get_required_radius(map<string, vector<int> > &map, string obj_id, int idx) {
	if (map.find(obj_id) == map.end()) {
		return -1;
	}
	vector<int> distances = map[obj_id];	
	if(distances.size() < idx+ 1) {
		return -1;
	}
	//cerr << "obj_id" << obj_id << ", idx is " << idx << ", req radius is " << distances[idx] << endl;
	return distances[idx];
}

int num_iterations = 0;
long num_selected = 0;
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

	double low[2], high[2];  // Temporary value placeholders for MBB
	double tmp_distance; // Temporary distance for nearest neighbor query
	double def_search_radius = -1; // Default search radius 
					//for nearest neighbor (NN) with unknown bounds
	double max_search_radius; // max_radius to search for NN

	try { 

		vector<Geometry*>  & poly_set_one = sttemp.polydata[idx1];
		vector<Geometry*>  & poly_set_two = sttemp.polydata[idx2];
		
		int len1 = poly_set_one.size();
		int len2 = poly_set_two.size();

		#ifdef DEBUG
		cerr << "Bucket size: " << len1 << " joining with " << len2 << endl;
		#endif

		if (len1 <= 0 || len2 <= 0) {
			return 0;
		}

		/* Build index on the "second data set */
		map<int, Geometry*> geom_polygons2;
		geom_polygons2.clear();

		// Make a copy of the vector to map to build index (API restriction)
		for (int j = 0; j < len2; j++) {
			geom_polygons2[j] = poly_set_two[j];
		}

		/* Handling for special nearest neighbor query */	
		
		if (stop.JOIN_PREDICATE == ST_NEAREST_2) {
			// Updating bucket information	
			if (len2 > 0) {
				const Envelope * envtmp = poly_set_two[0]->getEnvelopeInternal();
				sttemp.bucket_min_x = envtmp->getMinX();
				sttemp.bucket_min_y = envtmp->getMinY();
				sttemp.bucket_max_x = envtmp->getMaxX();
				sttemp.bucket_max_y = envtmp->getMaxY();
			}
			for (int j = 0; j < len1; j++) {
				update_bucket_dimension(poly_set_one[j]->getEnvelopeInternal());
			}
			if (!selfjoin) {
				for (int j = 0; j < len2; j++) {
					update_bucket_dimension(poly_set_two[j]->getEnvelopeInternal());
				}
			}
			
			max_search_radius = max(sttemp.bucket_max_x - sttemp.bucket_min_x, 
						sttemp.bucket_max_y - sttemp.bucket_min_y);
			def_search_radius = min(sqrt((sttemp.bucket_max_x - sttemp.bucket_min_x)
						* (sttemp.bucket_max_y - sttemp.bucket_min_y) 
						* stop.k_neighbors / len2), max_search_radius);
			if (def_search_radius == 0) {
				def_search_radius = DistanceOp::distance(poly_set_one[0], poly_set_two[0]);
			}
			#ifdef DEBUG
			cerr << "Bucket dimension min-max: " << sttemp.bucket_min_x << TAB 
				<< sttemp.bucket_min_y << TAB << sttemp.bucket_max_x
				<< TAB << sttemp.bucket_max_y << endl;
			cerr << "Width and height (x-y span) " 
			<< sttemp.bucket_max_x - sttemp.bucket_min_x
			<< TAB << sttemp.bucket_max_y - sttemp.bucket_min_y  << endl;
			#endif
		}

		// build the actual spatial index for input polygons from idx2
		if (! build_index_geoms(geom_polygons2, spidx, storage)) {
			#ifdef DEBUG
			cerr << "Building index on geometries from set 2 has failed" << endl;
			#endif
			return -1;
		}

		int radius_index = get_radius_index(stop.k_neighbors + 2);
		for (int i = 0; i < len1; i++) {		
			/* Extract minimum bounding box */
			const Geometry* geom1 = poly_set_one[i];
			const Envelope * env1 = geom1->getEnvelopeInternal();

			low[0] = env1->getMinX();
			low[1] = env1->getMinY();
			high[0] = env1->getMaxX();
			high[1] = env1->getMaxY();
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
				high[0] += stop.expansion_distance;
				high[1] += stop.expansion_distance;
			}

			vector<vector<string> >  fields_poly_one = sttemp.rawdata[idx1];
			string obj_id = fields_poly_one[i][3];	
		    int required_radius = get_required_radius(knn_idx_map, obj_id, radius_index);
			//cerr << "default search radius is " << def_search_radius << endl;
			if(strcmp(getenv("USE_PRECOMPUTED"), "true") == 0) {
			    //cerr << "old default radius: " << def_search_radius << ", new: " << required_radius << endl;
				def_search_radius = required_radius;
			}
			/* Regular handling */
			Region r(low, high, 2);
			MyVisitor vis;
			vis.matches.clear();
			/* R-tree intersection check */
			spidx->intersectsWithQuery(r, vis);
			
			int num_iters_for_obj = 0;
			/* Retrieve enough candidate neighbors */
			if (stop.JOIN_PREDICATE == ST_NEAREST_2) {
				double search_radius = def_search_radius;
				while (vis.matches.size() <= stop.k_neighbors + 1
					&& vis.matches.size() <= len2 // there can't be more neighbors than number of objects in the bucket
					&& search_radius <= sqrt(2) * max_search_radius) {
					// Increase the radius to find more neighbors
					// Stopping criteria when there are not enough neighbors in a tile
					low[0] = env1->getMinX() - search_radius;
					low[1] = env1->getMinY() - search_radius;
					high[0] = env1->getMaxX() + search_radius;
					high[1] = env1->getMaxY() + search_radius;
					
					Region r2(low, high, 2);
					
					vis.matches.clear();
					spidx->intersectsWithQuery(r2, vis);

					#ifdef DEBUG
					cerr << "Search radius:" << search_radius << " hits: " << vis.matches.size() << endl;
					#endif
					search_radius *= sqrt(2);
					num_iterations++;
					num_iters_for_obj++;
				}

				sttemp.nearest_distances.clear();
				/* Handle the special case of rectangular/circle expansion -sqrt(2) expansion */
				vis.matches.clear();
				low[0] = env1->getMinX() - search_radius;
				low[1] = env1->getMinY() - search_radius;
				high[0] = env1->getMaxX() + search_radius;
				high[1] = env1->getMaxY() + search_radius;
				Region r3(low, high, 2);
				spidx->intersectsWithQuery(r3, vis);
				// cerr << "num_iterations:" << num_iters_for_obj << " num objs selected " << vis.matches.size() <<  endl;
				num_selected += vis.matches.size();
			}
			
			for (uint32_t j = 0; j < vis.matches.size(); j++) 
			{
				/* Skip results that have been seen before (self-join case) */
				if (selfjoin && ((vis.matches[j] == i) ||  // same objects in self-join
				    (!stop.result_pair_duplicate && vis.matches[j] <= i))) { // duplicate pairs
					#ifdef DEBUG
					cerr << "skipping (selfjoin): " << j << " " << vis.matches[j] << endl;
					#endif
					continue;
				}
				
				const Geometry* geom2 = poly_set_two[vis.matches[j]];
				const Envelope * env2 = geom2->getEnvelopeInternal();

				if (stop.JOIN_PREDICATE == ST_NEAREST 
					&& (!selfjoin || vis.matches[j] != i)) { // remove selfjoin candidates
					/* Handle nearest neighbor candidate */	
					tmp_distance = DistanceOp::distance(geom1, geom2);
					if (tmp_distance < stop.expansion_distance) {
						update_nn(vis.matches[j], tmp_distance);
					}
					
				}
				else if (stop.JOIN_PREDICATE == ST_NEAREST_2 
					&& (!selfjoin || vis.matches[j] != i)) {
					tmp_distance = DistanceOp::distance(geom1, geom2);
					update_nn(vis.matches[j], tmp_distance);
	//				cerr << "updating: " << vis.matches[j] << " " << tmp_distance << endl;
				}
				else if (stop.JOIN_PREDICATE != ST_NEAREST 
					&& stop.JOIN_PREDICATE != ST_NEAREST_2
					&& join_with_predicate(geom1, geom2, env1, env2,
							stop.JOIN_PREDICATE))  {
					report_result(i, vis.matches[j]);
					pairs++;
				}

			}
			/* Nearest neighbor outputting */
			if (stop.JOIN_PREDICATE == ST_NEAREST 
				|| stop.JOIN_PREDICATE == ST_NEAREST_2) {
				for (std::list<struct query_nn_dist *>::iterator 
					it = sttemp.nearest_distances.begin(); 
					it != sttemp.nearest_distances.end(); it++) 
				{
					sttemp.distance = (*it)->distance;
					report_result(i, (*it)->object_id);	
					pairs++;
					/* Cleaning up memory */
					delete *it;
				}
				sttemp.nearest_distances.clear();
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

	cerr << "num_iterations:" << num_iterations << " num objs selected " << num_selected <<  endl;
	cout.flush();
	cerr.flush();
	return 0;
}

