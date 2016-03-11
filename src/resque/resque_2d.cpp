
#include <resque/resque_2d.hpp>

/* 
 * RESQUE processing engine v3.0
 *   It supports spatial join and nearest neighbor query with different predicates
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
 *   Requirement (input files): see the Wiki
 * */

using namespace geos;
using namespace geos::io;
using namespace geos::geom;
using namespace geos::operation::buffer;
using namespace geos::operation::distance;
using namespace std;
using namespace SpatialIndex;

// Spatial join operations
#include <resque/spjoin_2d.hpp>

// Nearest neighbor operations
#include <resque/knn_2d.hpp>

/* Performance metrics */
clock_t start_reading_data;
clock_t start_query_exec;

clock_t total_reading;
clock_t total_query_exec;

/* Initialize default values in query structs (operator and temporary placeholders) */
/* To be potentially removed to adjust for initialization already 
 * 	been done in param extraction method */
void init(struct query_op &stop, struct query_temp &sttemp){
	stop.offset = 2; // default format or value for offset
}

/* report result separated by separator */
void report_result(struct query_op &stop, struct query_temp &sttemp, int i, int j)
{
	sttemp.stream.str("");
	sttemp.stream.clear();
	/* ID used to access rawdata for the "second" data set */

	if (stop.output_fields.size() == 0) {
		/* No output fields have been set. Print all fields read */
		for (int k = 0; k < sttemp.rawdata[SID_1][i].size(); k++) {
			sttemp.stream << sttemp.rawdata[SID_1][i][k] << SEP;
		}
		for (int k = 0; k < sttemp.rawdata[stop.sid_second_set][j].size(); k++) {
			sttemp.stream << SEP << sttemp.rawdata[stop.sid_second_set][j][k];
		}
	}
	else {
		/* Output fields are listed */
		int k = 0;
		for (; k < stop.output_fields.size() - 1; k++) {
	//		cerr << "outputting fields " << stop.output_fields[k];
			obtain_field(stop, sttemp, k, i, j);
			sttemp.stream << SEP;
		}
		obtain_field(stop, sttemp, k, i, j);

	//		cerr << "outputting fields " << stop.output_fields[k];
	}

	sttemp.stream << endl;
	cout << sttemp.stream.str();
}

// Performs spatial query on data stored in query_temp using operator query_op
int execute_query(struct query_op &stop, struct query_temp &sttemp)
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

	/*
	if (stop.use_cache_file) {
		read_cache_file();
	}
	*/

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
			int pairs = join_bucket(stop, sttemp); // number of satisfied predicates

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
			release_mem(stop, sttemp, maxCardRelease);
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
	int pairs = join_bucket(stop, sttemp); // number of satisfied predicates

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

	release_mem(stop, sttemp, stop.join_cardinality);
	
	// clean up newed objects
	delete wkt_reader;
	delete gf;
	delete pm;

	return tile_counter;
}


/* Release objects in memory (for the current tile/bucket) */
void release_mem(struct query_op &stop, struct query_temp &sttemp, int maxCard) {
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
void read_cache_file(struct query_op &stop) {
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


/* Output the field at given position  */
void obtain_field(struct query_op &stop, struct query_temp &sttemp, 
	int position, int pos1, int pos2)
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
int join_bucket(struct query_op &stop, struct query_temp &sttemp)
{
	if (stop.join_predicate == ST_NEAREST
		|| stop.join_predicate == ST_NEAREST_2) {
		return join_bucket_knn(stop, sttemp);	
	} else {
		return join_bucket_spjoin(stop, sttemp);
	}
}

/* main body of the engine */
int main(int argc, char** argv)
{
	int c = 0; // Number of results satisfying the predicate

	struct query_op stop;
	struct query_temp sttemp;

	init(stop, sttemp); // setting the query operator and temporary variables to default

	if (!extract_params(argc, argv, stop, sttemp)) { // Function is located in params header file
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
			c = execute_query(stop, sttemp);
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

