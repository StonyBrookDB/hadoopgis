#include <iostream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <getopt.h>
#include <time.h>
#include "../common/string_constants.h"
#include "../common/tokenizer.h"
#include "../common/resque_constants.h"
#include "../common/rtree_traversal.h"
#include "../extensions/specialmeasures/pathology_metrics.h"
#include "../extensions/specialmeasures/geographical.h"


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
	std::map<int, std::vector<Geometry*> > polydata;

	/* Nearest neighbor temporary placeholders */
	list<struct query_nn_dist *> nearest_distances;
} sttemp;



/* Query operator */
struct query_op {
	bool use_cache_file;
	string cachefilename;

	int JOIN_PREDICATE; /* Join predicate - see resquecommon.h for the full list*/
	int shape_idx_1; /* geometry field number of the 1st set */
	int shape_idx_2; /* geometry field number of the 2nd set */
	int join_cardinality; /* Join cardinality */
	double expansion_distance; /* Distance used in dwithin and k-nearest query */
	int k_neighbors; /* k - the number of neighbors used in k-nearest neighbor */
	/* the number of additional meta data fields
	 i.e. those first fields not counting towards object data
	*/
	int offset; // offset/adjustment for meta data field

	int sid_second_set; // set id for the "second" dataset.
	// Value == 2 for joins between 2 data sets
	// Value == 1 for selfjoin

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
int get_join_predicate(char * predicate_str);
void release_mem(int maxCard);
void set_projection_param(char * arg);
bool extract_params(int argc, char** argv );
void report_result(int i, int j);
void obtain_field(int position, int pos1, int pos2);
void update_nn(int object_id, double distance);
void update_bucket_dimension(const Envelope * env);
double get_distance(const geos::geom::Point* p1, const geos::geom::Point* p2);
double get_distance_earth(const geos::geom::Point* p1,const geos::geom::Point* p2);

/* Externed function from other files */
extern double compute_jaccard(double union_area, double intersection_area);
extern double compute_dice(double area1, double area2, double intersection_area);
extern double to_radians(double degrees);
extern double earth_distance(double lat1, double lng1, double lat2, double lng2);

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

void print_stop(){
	// initlize query operator 
	std::cerr << "predicate: " << stop.JOIN_PREDICATE << std::endl;
	std::cerr << "distance: " << stop.expansion_distance << std::endl;
	std::cerr << "shape index 1: " << stop.shape_idx_1 << std::endl;
	std::cerr << "shape index 2: " << stop.shape_idx_2 << std::endl;
	std::cerr << "join cardinality: " << stop.join_cardinality << std::endl;
}

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

	/* Define the resource cleaning when using cache-file  */
	int maxCardRelease = min(stop.join_cardinality, stop.use_cache_file ? 1 : 2);


	#ifdef DEBUG
	std::cerr << "Bucket info:[ID] |A|x|B|=|R|" <<std::endl;
	#endif  

	#ifdef DEBUGTIME
	start_reading_data = clock();
	#endif

	/* Handle reading from cache file */
	if (stop.use_cache_file) {
		read_cache_file();
	}


	while(cin && getline(cin, input_line) && !cin.eof()) {
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

	while(cin && getline(cin, input_line) && !cin.eof()) {
		tokenize(input_line, fields, TAB, true);
		sid = atoi(fields[1].c_str());
		tile_id = fields[0];

	}

}

/* Create an R-tree index on a given set of polygons */
bool buildIndex(map<int,Geometry*> & geom_polygons, ISpatialIndex* & spidx, IStorageManager* & storage) {
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
				if (NULL == geom_buffer1)
					cerr << "NULL: geom_buffer1" << endl;

				flag = join_with_predicate(geom_buffer1, geom2, env1, env2, ST_INTERSECTS);
				delete geom_buffer1;
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

/* Set fields to be output */
void set_projection_param(char * arg)
{
	string param(arg);
	vector<string> fields;
	vector<string> selec;
	tokenize(param, fields, STR_OUTPUT_DELIM);
	int set_id = 0;

	for (int i = 0; i < fields.size(); i++) {
		string stat_param = fields[i];
		/* Check if the current field is one of the existing field in the 
		data set and collect its set id */

		tokenize(stat_param, selec, STR_SET_DELIM);
		if (selec.size() == 1) {
			// Special statistics below (no set delimiter/colon was found)	

			// Output cannot be taken directly from the original field
			stop.output_fields_set_id.push_back(SID_NEUTRAL);

			// Updating output_fields
			if (stat_param.compare(PARAM_STATS_TILE_ID) == 0 ) {
				stop.output_fields.push_back(STATS_TILE_ID);
			}
			else if (stat_param.compare(PARAM_STATS_AREA_1) == 0 ) {
				stop.output_fields.push_back(STATS_AREA_1);
				stop.needs_area_1 = true;
			}
			else if (stat_param.compare(PARAM_STATS_AREA_2) == 0) {
				stop.output_fields.push_back(STATS_AREA_2);
				stop.needs_area_2 = true;
			}
			else if (stat_param.compare(PARAM_STATS_UNION_AREA) == 0) {
				stop.output_fields.push_back(STATS_UNION_AREA);
				stop.needs_union = true;
			}
			else if (stat_param.compare(PARAM_STATS_INTERSECT_AREA) == 0) {
				stop.output_fields.push_back(STATS_INTERSECT_AREA);
				stop.needs_intersect = true;
			}
			else if (stat_param.compare(PARAM_STATS_JACCARD_COEF ) == 0) {
				stop.output_fields.push_back(STATS_JACCARD_COEF);
				stop.needs_jaccard = true;
			}
			else if (stat_param.compare(PARAM_STATS_DICE_COEF) == 0) {	
				stop.output_fields.push_back(STATS_DICE_COEF);
				stop.needs_dice = true;
			} 
			else if (stat_param.compare(PARAM_STATS_MIN_DIST) == 0) {	
				stop.output_fields.push_back(STATS_MIN_DIST);
				stop.needs_min_distance = true;
			} 

			else {
				#ifdef DEBUG
				cerr << "Unrecognizeable option for output statistics" << endl;
				#endif
			}
		}
		else {
			// Colon/Set delimiter was found
			// Regular original field
			set_id = atoi(selec[0].c_str());
			switch (set_id) {
				case SID_1:
				case SID_2:
					stop.output_fields_set_id.push_back(set_id);
					stop.output_fields.push_back(atoi(selec[1].c_str()) - 1 + stop.offset);
					break;
				default :
					#ifdef DEBUG
					cerr << "Unrecognizeable set ID for output statistics ID" << endl;
					#endif
					break;
			}
		}
		selec.clear();
	}

	// Dependency adjustment
	if (stop.needs_jaccard) {
		stop.needs_intersect = true;
		stop.needs_union = true;
	}

	if (stop.needs_dice) {
		stop.needs_intersect = true;
		stop.needs_area_1 = true;
		stop.needs_area_2 = true;
	}
}


int get_join_predicate(char * predicate_str)
{
	if (strcmp(predicate_str, PARAM_PREDICATE_INTERSECTS.c_str()) == 0) {
		return ST_INTERSECTS ; 
	} 
	else if (strcmp(predicate_str, PARAM_PREDICATE_TOUCHES.c_str()) == 0) {
		return ST_TOUCHES;
	} 
	else if (strcmp(predicate_str, PARAM_PREDICATE_CROSSES.c_str()) == 0) {
		return ST_CROSSES;
	} 
	else if (strcmp(predicate_str, PARAM_PREDICATE_CONTAINS.c_str()) == 0) {
		return ST_CONTAINS;
	} 
	else if (strcmp(predicate_str, PARAM_PREDICATE_ADJACENT.c_str()) == 0) {
		return ST_ADJACENT;
	} 
	else if (strcmp(predicate_str, PARAM_PREDICATE_DISJOINT.c_str()) == 0) {
		return ST_DISJOINT;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_EQUALS.c_str()) == 0) {
		return ST_EQUALS;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_DWITHIN.c_str()) == 0) {
		return ST_DWITHIN;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_WITHIN.c_str()) == 0) {
		return ST_WITHIN;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_OVERLAPS.c_str()) == 0) {
		return ST_OVERLAPS;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_NEAREST.c_str()) == 0) {
		return ST_NEAREST;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_NEAREST_NO_BOUND.c_str()) == 0) {
		return ST_NEAREST_2;
	}
	else {
		#ifdef DEBUG
		cerr << "unrecognized join predicate " << endl;
		#endif
		return -1;
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
		bool ret = buildIndex(geom_polygons2, spidx, storage);
		if (ret == false) {
			return -1;
		}

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

			/* Regular handling */
			Region r(low, high, 2);
			hits.clear();
			vector<id_type> matches;
			MyVisitor vis(&matches);
			/* R-tree intersection check */
			spidx->intersectsWithQuery(r, vis);

			/* Retrieve enough candidate neighbors */
			if (stop.JOIN_PREDICATE == ST_NEAREST_2) {
				double search_radius = def_search_radius;
				
				hits.clear();
				sttemp.nearest_distances.clear();
				while (hits.size() <= stop.k_neighbors + 1
					&& hits.size() <= len2 // there can't be more neighbors than number of objects in the bucket
					&& search_radius <= sqrt(2) * max_search_radius) {
					// Increase the radius to find more neighbors
					// Stopping criteria when there are not enough neighbors in a tile
					low[0] = env1->getMinX() - search_radius;
					low[1] = env1->getMinY() - search_radius;
					high[0] = env1->getMaxX() + search_radius;
					high[1] = env1->getMaxY() + search_radius;
					hits.clear();
					

					Region r2(low, high, 2);
					MyVisitor vistmp;
					
					spidx->intersectsWithQuery(r2, vistmp);
					#ifdef DEBUG
					cerr << "Search radius:" << search_radius << " hits: " << hits.size() << endl;
					#endif
					search_radius *= sqrt(2);
				}
			}
			
			for (uint32_t j = 0; j < hits.size(); j++) 
			{
				/* Skip results that have been seen before (self-join case) */
				if (selfjoin && ((hits[j] == i) ||  // same objects in self-join
				    (!stop.result_pair_duplicate && hits[j] <= i))) { // duplicate pairs
					#ifdef DEBUG
					cerr << "skipping (selfjoin): " << j << " " << hits[j] << endl;
					#endif
					continue;
				}
				
				const Geometry* geom2 = poly_set_two[hits[j]];
				const Envelope * env2 = geom2->getEnvelopeInternal();

				if (stop.JOIN_PREDICATE == ST_NEAREST 
					&& (!selfjoin || hits[j] != i)) { // remove selfjoin candidates
					/* Handle nearest neighbor candidate */	
					tmp_distance = DistanceOp::distance(geom1, geom2);
					if (tmp_distance < stop.expansion_distance) {
						update_nn(hits[j], tmp_distance);
					}
					
				}
				else if (stop.JOIN_PREDICATE == ST_NEAREST_2 
					&& (!selfjoin || hits[j] != i)) {
					tmp_distance = DistanceOp::distance(geom1, geom2);
					update_nn(hits[j], tmp_distance);
	//				cerr << "updating: " << hits[j] << " " << tmp_distance << endl;
				}
				else if (stop.JOIN_PREDICATE != ST_NEAREST 
					&& stop.JOIN_PREDICATE != ST_NEAREST_2
					&& join_with_predicate(geom1, geom2, env1, env2,
							stop.JOIN_PREDICATE))  {
					report_result(i, hits[j]);
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
				}
				sttemp.nearest_distances.clear();
			}
	
		}
	} // end of try
	//catch (Tools::Exception& e) {
	catch (...) {
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

bool extract_params(int argc, char** argv ){ 
	/* getopt_long stores the option index here. */
	int option_index = 0;
	/* getopt_long uses opterr to report error*/
	opterr = 0 ;
	struct option long_options[] =
	{
		{"distance",   required_argument, 0, 'd'},
		{"shpidx1",  required_argument, 0, 'i'},
		{"shpidx2",  required_argument, 0, 'j'},
		{"predicate",  required_argument, 0, 'p'},
		{"knn",  required_argument, 0, 'k'},
		{"fields",     required_argument, 0, 'f'},
		{"earth",     required_argument, 0, 'e'},
		{"replicate",     required_argument, 0, 'r'},
		{"cachefile",     required_argument, 0, 'c'},
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long (argc, argv, "p:i:j:d:f:k:r:e:c:",long_options, &option_index)) != -1){
		switch (c)
		{
			case 0:
				/* If this option set a flag, do nothing else now. */
				if (long_options[option_index].flag != 0)
					break;
				cout << "option " << long_options[option_index].name ;
				if (optarg)
					cout << "a with arg " << optarg ;
				cout << endl;
				break;

			case 'p':
				stop.JOIN_PREDICATE = get_join_predicate(optarg);
				#ifdef DEBUG
					cerr << "predicate: " 
						<< stop.JOIN_PREDICATE << endl;
                                #endif
				break;

			case 'i':
				// Adjusting the actual geometry field (shift) to account
				//   for tile_id and join_index
				stop.shape_idx_1 = strtol(optarg, NULL, 10) - 1 + stop.offset;
                                stop.join_cardinality++;
                                #ifdef DEBUG
					cerr << "geometry index i (set 1): " 
						<< stop.shape_idx_1 << endl;
				#endif
                                break;

			case 'j':
				stop.shape_idx_2 = strtol(optarg, NULL, 10) - 1 + stop.offset;
                                stop.join_cardinality++;
                                #ifdef DEBUG
					cerr << "geometry index j (set 2): " 
						<< stop.shape_idx_2 << endl;
				#endif
                                break;

			case 'd':
				stop.expansion_distance = atof(optarg);
				#ifdef DEBUG
					cerr << "Search distance/Within distance parameter " 
						<< stop.expansion_distance << endl;
				#endif
				break;

			case 'k':
				stop.k_neighbors = strtol(optarg, NULL, 10);
				#ifdef DEBUG
					cerr << "Number of neighbors: " 
						<< stop.k_neighbors << endl;
				#endif
				break;

			case 'f':
                                set_projection_param(optarg);
                                #ifdef DEBUG
                                        cerr << "Original params: " << optarg << endl;
					cerr << "output field: " << endl;
					for (int m = 0; m < stop.output_fields.size(); m++) {
						cerr << stop.output_fields[m] << SEP;
					}
					cerr << endl << "setid: ";
					for (int m = 0; m < stop.output_fields_set_id.size(); m++) {
						cerr << stop.output_fields_set_id[m] << SEP;
					}
					cerr << endl;
                                #endif
                                break;
			case 'r':
				stop.result_pair_duplicate = strcmp(optarg, "true") == 0;
				#ifdef DEBUG
					cerr << "Allows symmetric result pairs: "
						<< stop.result_pair_duplicate << endl;
				#endif
				break;
			case 'e':
				stop.use_earth_distance = strcmp(optarg, "true") == 0;
				#ifdef DEBUG
					cerr << "Using earth distance: "
						<< stop.use_earth_distance << endl;
				#endif
				break;

			case 'c':
				stop.cachefilename = string(optarg);
				stop.use_cache_file = true;
				#ifdef DEBUG
					cerr << "Cache file name: " << filename << endl;
				#endif
				break;

			case '?':
				return false;
				/* getopt_long already printed an error message. */
				break;

			default:
				return false;
		}
	}

	// query operator validation 
	if (stop.JOIN_PREDICATE <= 0 ) {
		#ifdef DEBUG 
		cerr << "Query predicate is NOT set properly. Please refer to the documentation." << endl ; 
		#endif
		return false;
	}
	// check if distance is set for dwithin predicate
	if (stop.JOIN_PREDICATE == ST_DWITHIN && stop.expansion_distance == 0.0) { 
		#ifdef DEBUG 
		cerr << "Distance parameter is NOT set properly. Please refer to the documentation." << endl ;
		#endif
		return false;
	}
	if (stop.join_cardinality == 0) {
		#ifdef DEBUG 
		cerr << "Join cardinality are NOT set properly. Please refer to the documentation." << endl ;
		#endif
		return false; 
	}
	
	if ((stop.JOIN_PREDICATE == ST_NEAREST || stop.JOIN_PREDICATE == ST_NEAREST_2)
		 && stop.k_neighbors <= 0) {
		#ifdef DEBUG 
		cerr << "K-the number of nearest neighbors is NOT set properly. Please refer to the documentation." << endl ;
		#endif
		return false; 
	}

	#ifdef DEBUG 
	print_stop();
	#endif
	
	return true;
}


/* Display help message to users */
void usage(){
	cerr  << endl << "Usage: resque [OPTIONS]" << endl << "OPTIONS:" << endl;
	cerr << TAB << "-p,  --predicate" << TAB <<  "The spatial join predicate for query processing. \
Acceptable values are [st_intersects, st_disjoint, st_overlaps, st_within, st_equals,\
st_dwithin, st_crosses, st_touches, st_contains, st_nearest, st_nearest2]." << endl;
	cerr << TAB << "-i, --shpidx1"  << TAB << "The index of the geometry field from the \
first dataset. Index value starts from 1." << endl;
	cerr << TAB << "-j, --shpidx2"  << TAB << "The index of the geometry field from the second dataset.\
Index value starts from 1." << endl;
	cerr << TAB << "-d, --distance" << TAB << "Used together with st_dwithin predicate to \
indicate the join distance or used together with st_nearest to indicate the max distance \
to search for nearest neighbor. \
This field has no effect on other join predicates." << endl;	
	cerr << TAB << "-k, --knn" << TAB << "The number of nearest neighbor\
Only used in conjuction with the st_nearest or st_nearest2 predicate";
	cerr << TAB << "-o, --offset"  << TAB << "Optional. \
The offset (number of fields to be adjusted as metadata information.\
Observe that the first field is always the tile id. The join id \
is always the second field (2). Index starts from 1. Default value is 3" << endl;
	cerr << TAB << "-r, --replicate" << TAB <<  "Optional [true | false]. \
Indicates whether result pair are output twice for result pair that logically exists\
when the pair is swapped (position). e.g. intersection between object 1 and 2\
also indicates a logical intersection between object 2 and 1. \
Default value is false" << endl;
	cerr << TAB << "-e, --earth" << TAB << "Optional [true | false]\
Indicate wheather to compute spherical distance for point-point on earth.";
	cerr << TAB << "-f, --fields"   << TAB << "Optional. \
Output field election parameter. Original fields \
should have a format set_id:field_id. Fields are delimited by commas. \
\nSpecial fields are [area1 | area2 | union | intersect |\
dice | jaccard | mindist | tileid] representing the area of objects\
from set 1, set 2, union, intersection,\
dice coefficient, intersection coefficient, dice statitics, jaccard coeeficient,\
minimum distance between the pair of polygons, and tile id correspondingly. \
\nField values and statistics will be output output order specifed by users. \
\nFor example: if we want to only output fields 1, 3, and 5\
from the first dataset (indicated with param -i), \
and output fields 1, 2, and 9 from the second dataset \
(indicated with param -j) followed by the area of object 2 \
and jaccard coefficient, \
then we can provide an argument as: --fields 1:1,1:3,1:5:2:1,2:2,2:9,tileid,\
area2,jacc" << endl;
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

