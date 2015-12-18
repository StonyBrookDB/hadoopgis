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


/* Performance metrics */
clock_t start_reading_data;
clock_t start_query_exec;

clock_t total_reading;
clock_t total_query_exec;

/* Function prototypes */
void init();
void print_stop();
int pair_bucket();
int execute_query();
void release_mem(int maxCard);
double get_distance(const geos::geom::Point* p1, const geos::geom::Point* p2);
double get_distance_earth(const geos::geom::Point* p1,const geos::geom::Point* p2);

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


	#ifdef DEBUG
	std::cerr << "Bucket info:[ID] |A|x|B|=|R|" <<std::endl;
	#endif  

	#ifdef DEBUGTIME
	start_reading_data = clock();
	#endif


	while (cin && getline(cin, input_line) && !cin.eof()) {
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
			int pairs = pair_bucket(); // number of satisfied predicates

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
	int  pairs = pair_bucket();

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

void print_list(std::vector<int> distances) 
{

	for(std::vector<int>::iterator it = distances.begin(); it!= distances.end(); ++it)
	{
			cerr << *it << ", ";
	}

    cerr << endl;
}


void track_radius(string obj_id, std::vector<int> distances, int counts[]) 
{
	cout << obj_id << ":";
	int i = 0;
	for(; counts[i + 1] != -1 && counts[i] <= distances.size(); i++) {
		cout << distances[counts[i] - 1] << ",";
	}
	if(counts[i] <= distances.size()) {
		cout << distances[counts[i] - 1];
	}
//	for(; counts[i + 1] != -1 && counts[i] < distances.size(); i++) {
//		cout << distances[counts[i]] << ",";
//	}
//	if(counts[i] < distances.size()) {
//		cout << distances[counts[i]];
//	}
	cout << endl;
}

/* 
 *  Perform spatial computation on a given tile with data 
 *   located in polydata and rawdata
 *   
 */
int pair_bucket()
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

		//#ifdef DEBUG
		//cerr << "Bucket size: " << len1 << " joining with " << len2 << endl;
		//#endif

		if (len1 <= 0 || len2 <= 0) {
			return 0;
		}

		for (int i = 0; i < len1; i++) {		
			/* Extract minimum bounding box */
			const Geometry* geom1 = poly_set_one[i];
			const Envelope * env1 = geom1->getEnvelopeInternal();

			low[0] = env1->getMinX();
			low[1] = env1->getMinY();
			high[0] = env1->getMaxX();
			high[1] = env1->getMaxY();

			std::vector<int> distances;
			vector<vector<string> >  fields_poly_one = sttemp.rawdata[idx1];
			string obj_id = fields_poly_one[i][3];	
			for (int j = 0; j < len2; j++) {		
					const Geometry* geom2 = poly_set_two[j];
					const Envelope * env2 = geom2->getEnvelopeInternal();

					double low2[2], high2[2];  // Temporary value placeholders for MBB
					low2[0] = env2->getMinX();
					low2[1] = env2->getMinY();
					high2[0] = env2->getMaxX();
					high2[1] = env2->getMaxY();

					int maxX = std::max(abs(high2[0] - low[0]), abs(high[0] - low2[0]));
					int maxY = std::max(abs(high2[1] - low[1]), abs(high[1] - low2[1]));
					int dist = std::max(maxX, maxY);
					distances.push_back(dist);	
			}
			std::sort(distances.begin(), distances.end()) ;		
			//cerr << "Will output for object " << obj_id << endl;
			//print_list(distances);
			int exp[] = {1, 4, 9, 16, -1};
			track_radius(obj_id, distances, exp);
		}
	} // end of try
	catch (Tools::Exception& e) {
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

