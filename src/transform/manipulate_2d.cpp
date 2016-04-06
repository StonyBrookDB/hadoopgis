/* 
 * This program has many functionalities depending on the parameters
 *   set in progparams/resque_params_2d.hpp
 *
 *   Flags used in processing are located in stop (spatial operator struct)
 *
 *   It can map objects to their respective tiles given 
 *   a partition schema which is read from a disk
 * or it can be used to extract MBBs from objects
 *
 *  It can also use sampling rate. See the usage() in progparams/resque_params_2d.hpp
 *        and progparams/resque_datastructs.h for the list functionalities
 * */

#include <transform/manipulate_2d.hpp>

using namespace geos;
using namespace geos::io;
using namespace geos::geom;
using namespace geos::operation::buffer;
using namespace geos::operation::distance;
using namespace std;
using namespace SpatialIndex;

/* Performance metrics */
clock_t start_reading_data;
clock_t start_query_exec;

clock_t total_reading;
clock_t total_query_exec;

// Initialize default values in spatial operator structure
void init(struct query_op &stop, struct query_temp &sttemp) {
	stop.extract_mbb = false;
	stop.collect_mbb_stat = false;
	stop.use_sampling = false;
	stop.sample_rate = 1.0;
	stop.offset = 0;

	stop.prefix_1 = NULL;
	stop.prefix_2 = NULL;
	stop.shape_idx_1 = 0;
	stop.shape_idx_2 = 0;
}

/* Process standard input and emit objects to their respective partitions */
bool process_input(struct query_op &stop, struct query_temp &sttemp,
		const int join_idx, const int geom_idx, 
		IStorageManager * &storage, ISpatialIndex * &spidx,
		std::map<id_type, string> * id_tiles) {
	string input_line;
	vector<string> fields;
	bool firstLineRead = false;

	Geometry* geom; 
	PrecisionModel *pm;
	GeometryFactory *gf;
	WKTReader *wkt_reader;
	
	MyVisitor vis;

	/* Space info */
	double space_low[2];
	double space_high[2];

	pm = new PrecisionModel();
	gf = new GeometryFactory(pm, 0);
	wkt_reader= new WKTReader(gf);


	long count_objects = 0;
	#ifdef DEBUG
	long count_emitted = 0;
	long count_bad = 0;
	#endif


	#ifdef DEBUGTIME
	start_reading_data = clock();
	#endif

	// Variables to track
	int random_id = 0;
	if (stop.extract_mbb) {
		struct timeval time; 
		gettimeofday(&time,NULL);
		srand((time.tv_sec * 1000) + (time.tv_usec / 1000));
		random_id = rand() % 10000; // Can use the number of reducers here for better load balancing/key hashing
	}

	double low[2];
	double high[2];
	// srand (static_cast <unsigned>(time(NULL)));

	/* Handling standard input */
	while(cin && getline(cin, input_line) && !cin.eof()){
		/* Sampling (optional parameter) */
		if (stop.use_sampling &&
			(static_cast<float>(rand()) / static_cast<float>(RAND_MAX)) > stop.sample_rate) {
			continue; // skip the record
		}
		tokenize(input_line, fields, TAB, true);
		count_objects++;
		try {
			if (stop.reading_mbb) {
				low[0] = stod(fields[1]);
				low[1] = stod(fields[2]);
				high[0] = stod(fields[3]);
				high[1] = stod(fields[4]);
			} else {
				if (fields[geom_idx].length() < 2 ) {
					#ifdef DEBUG
					cerr << "skipping record [" << input_line << "]"<< endl;
					#endif
					continue ;  // skip lines which has empty geometry
				}
				geom = wkt_reader->read(fields[geom_idx]);

				#ifdef DEBUGTIME
				total_reading += clock() - start_reading_data;
				start_query_exec = clock();
				#endif
				const Envelope * env = geom->getEnvelopeInternal();

				low[0] = env->getMinX();
				low[1] = env->getMinY();
				high[0] = env->getMaxX();
				high[1] = env->getMaxY();
				delete geom;
			}
			/* For extracting MBBs information */
			if (stop.extract_mbb) {
				// Simple output MBBs with offset 1
				cout << random_id << TAB << low[0] << TAB << low[1]
					<< TAB << high[0] << TAB << high[1] << endl;

				/* Collecting information about the space dimension */
				if (!firstLineRead) {
					space_low[0] = low[0];
					space_low[1] = low[1];
					space_high[0] = high[0];
					space_high[1] = high[1];
					firstLineRead = true;
				} else {
					space_low[0] = low[0] < space_low[0] ? low[0] : space_low[0];
					space_low[1] = low[1] < space_low[1] ? low[1] : space_low[1];
					space_high[0] = high[0] > space_high[0] ? high[0] : space_high[0];
					space_high[1] = high[1] > space_high[1] ? high[1] : space_high[1];
				}
			} else {
				Region r(low, high, 2);
				/* Find objects matching with intersecting tiles*/	
				spidx->intersectsWithQuery(r, vis);

				#ifdef DEBUG
				cerr << "intersecting with: " << vis.matches.size() << endl;
				#endif
				/* Emit objects to intersecting tiles */
				for (uint32_t i = 0 ; i < vis.matches.size(); i++ ) {
					if (stop.reading_mbb) {
						cout << (*id_tiles)[vis.matches[i]]
							<< TAB << low[0] << TAB << low[1]
							<< TAB << high[0] << TAB << high[1] << endl;
							
					} else if (stop.drop_join_idx) {
						// Append tile id only
						cout <<  (*id_tiles)[vis.matches[i]]
							<< TAB << input_line <<  endl ;
					} else {
						// Append tile id and join index
						cout <<  (*id_tiles)[vis.matches[i]]
							<< TAB << join_idx
							<< TAB << input_line <<  endl ;
					}
					#ifdef DEBUG
					count_emitted++;
					#endif
				}
				vis.matches.clear();
			}
		}
		catch (...)
		{
			#ifdef DEBUG
			count_bad++;
			cerr << "WARNING: Record is not well formatted " << input_line << endl;
			#endif
			continue; // ignore bad formatting and continue
		}

		fields.clear();

		#ifdef DEBUGTIME
		total_query_exec += clock() - start_query_exec;
		start_reading_data = clock();
		#endif
	}
	/* Output dimensions of space */
	if (stop.extract_mbb) {
		cout << "-1" << TAB << space_low[0] 
			<< TAB << space_low[1] << TAB << space_high[0]
			<< TAB << space_high[1] << TAB << count_objects << endl;
	}

	/* Cleaning up memory */
	delete wkt_reader;
	delete gf;
	delete pm;
	#ifdef DEBUG
	/* Output useful statistics */
	cerr << "Number of processed objects: " << count_objects << endl;
	cerr << "Number of times objects were emitted: " << count_emitted << endl;
	cerr << "Number of not well formatted objects: " << count_bad << endl;
	#endif
}

/* Build indexing on tile boundaries from cache file */
bool build_index_tiles(struct query_op &stop, struct query_temp &sttemp, 
	IStorageManager* &storage, ISpatialIndex * &spidx, 
	std::map<SpatialIndex::id_type, std::string> *id_tiles) {
	// build spatial index on tile boundaries 
	id_type  indexIdentifier;
	GEOSDataStreamFileTile stream(stop.cachefilename, id_tiles); // input from cache file
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

int main(int argc, char **argv) {
	struct query_op stop;
	struct query_temp sttemp;
	std::map<id_type, string> id_tiles;
	init(stop, sttemp);

	if (!extract_params(argc, argv, stop, sttemp)) {
#ifdef DEBUG 
		cerr <<"ERROR: query parameter extraction error." << endl 
			<< "Please see documentations, or contact author." << endl;
#endif
		usage();
		return -1;
	}

	char* stdin_file_name = NULL; // name of the input file
	int join_idx = -1; // index of the current file (0 or 1) matching to dataset 1 or 2
	int geom_idx = -1; // geometry field index	
	IStorageManager * storage = NULL;
	ISpatialIndex * spidx = NULL;

	if (!stop.reading_mbb && !stop.drop_join_idx) {
		/* Set the current join index */
		stdin_file_name = getenv("mapreduce_map_input_file");
		if (!stdin_file_name) {
			/* This happens if program is not run in mapreduce
			 *  For testing locally, set/export the environment variable above */
			#ifdef DEBUG
			cerr << "Environment variable mapreduce_map_input_file is not set correctly." << endl;
			#endif
			return -1;
		}

		if (strstr(stdin_file_name, stop.prefix_1) != NULL) {
			join_idx = SID_1;
			geom_idx = stop.shape_idx_1;
		} else if (strstr(stdin_file_name, stop.prefix_2) != NULL) {
			join_idx = SID_2;
			geom_idx = stop.shape_idx_2;
		} else {
			cerr << "File name from environment variable \
				does not match any path prefix" << endl;
		}

		if (join_idx < 0) {
			#ifdef DEBUG
			cerr << "Invalid join index" << endl;
			#endif
			return -1;
		}
	}
	if (stop.drop_join_idx) {
		geom_idx = stop.shape_idx_1;
	}
	if (!stop.extract_mbb) {
		if( !build_index_tiles(stop, sttemp, storage, spidx, &id_tiles)) {
#ifdef DEBUG
			cerr << "ERROR: Index building on tile structure has failed ." << std::endl;
#endif
			return 1 ;
		}
		else { 
#ifdef DEBUG  
			cerr << "GRIDIndex Generated successfully." << endl;
#endif
		}
	}

	// Process input line by line
	process_input(stop, sttemp, join_idx, geom_idx, storage, spidx, &id_tiles);

	/* Clean up indices */
	delete spidx;
	delete storage;
	cerr << "Tile contains" << id_tiles.size() << endl;
	id_tiles.clear();

	cout.flush();
	cerr.flush();

	return 0;
}


