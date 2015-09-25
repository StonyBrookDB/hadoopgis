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
#include "../common/spatialproc_structs.h"
#include "../common/spatialproc_params.h"
#include "../common/rtree_traversal.h"
/* 
 * This program maps objects to their respective tiles given 
 *   a partition schema which is read from a disk
 * or it can be used to extract MBBs from objects
 * */


/* Performance metrics */
clock_t start_reading_data;
clock_t start_query_exec;

clock_t total_reading;
clock_t total_query_exec;

/* Function protoypes */
bool build_index_tiles();
bool process_input(const int join_idx, const int geom_idx);

/* Included in header file */
extern void usage();

/* Process standard input and emit objects to their respective partitions */
bool process_input(const int join_idx, const int geom_idx) {
	string input_line;
	vector<string> fields;
	bool firstLineRead = false;

	Geometry* geom; 
	GeometryFactory *gf = NULL;
	WKTReader *wkt_reader = NULL;
	IStorageManager * storage = NULL;
	ISpatialIndex * spidx = NULL;
	MyVisitor vis;

	/* Space info */
	double space_min_x = 0;
	double space_min_y = 0;
	double space_max_x = 0;
	double space_max_y = 0;

  	gf = new GeometryFactory(new PrecisionModel(), 0);
  	wkt_reader= new WKTReader(gf);

	#ifdef DEBUG
	long count_objects = 0;
	long count_emitted = 0;
	long count_bad = 0;
	#endif

	vis.matches.clear();

	#ifdef DEBUGTIME
	start_reading_data = clock();
	#endif

	srand (static_cast <unsigned>(time(NULL)));

	/* Handling standard input */
	while(cin && getline(cin, input_line) && !cin.eof()){

		/* Sampling (optional parameter) */
		if (stop.use_sampling &&
			(static_cast<float>(rand()) / static_cast<float>(RAND_MAX)) > stop.sample_rate) {
			continue; // skip the record
		}


		tokenize(input_line, fields, TAB, true);
    		if (fields[geom_idx].length() < 2 ) {
			#ifdef DEBUG
      			cerr << "skipping record [" << input_line << "]"<< endl;
			#endif
      			continue ;  // skip lines which has empty geometry
    		}
    		try {
    			geom = wkt_reader->read(fields[geom_idx]);

			#ifdef DEBUGTIME
			total_reading += clock() - start_reading_data;
			start_query_exec = clock();
			#endif

			double low[2], high[2];
			const Envelope * env = geom->getEnvelopeInternal();

			low[0] = env->getMinX();
			low[1] = env->getMinY();

			high[0] = env->getMaxX();
			high[1] = env->getMaxY();

			/* For extracting MBBs information */
			if (stop.extract_mbb) {
				// Simple output MBBs with offset 1
				cout << 0 << TAB << low[0] << TAB << low[1]
					<< TAB << high[0] << TAB << high[1] << endl;

				/* Collecting information about the space dimension */
				if (stop.collect_mbb_stat) {
					if (!firstLineRead) {
						space_min_x = low[0];
						space_min_y = low[1];
						space_max_x = high[0];
						space_max_y = high[1];
						firstLineRead = true;
					} else {
						space_min_x = low[0] < space_min_x ? low[0] : space_min_x;
						space_min_y = low[1] < space_min_y ? low[1] : space_min_y;
						space_max_x = high[0] > space_max_x ? high[0] : space_max_x;
						space_max_y = high[0] > space_max_y ? high[1] : space_max_y;
					}
				}

			} else {
				Region r(low, high, 2);
		
				/* Find objects matching with intersecting tiles*/	
				spidx->intersectsWithQuery(r, vis);

				#ifdef DEBUG
				count_objects++;
				#endif
				/* Emit objects to intersecting tiles */
   				for (uint32_t i = 0 ; i < vis.matches.size(); i++ ) {
					// Append tile id and join index
					cout <<  stop.id_tiles[vis.matches[i]]
					<< TAB << join_idx
					<< TAB << join_idx // to-be-replaced by image_id or metadata
					<< TAB << input_line <<  endl ;
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
	if (stop.extract_mbb && stop.collect_mbb_stat) {
		cout << 1 << TAB << space_min_x 
				<< TAB << space_min_y 
				<< TAB << space_max_y 
				<< TAB << space_max_y << endl;
	}

	/* Cleaning up memory */
	delete gf;
	delete wkt_reader;

	#ifdef DEBUG
	/* Output useful statistics */
	cerr << "Number of processed objects: " << count_objects << endl;
	cerr << "Number of times objects were emitted: " << count_emitted << endl;
	cerr << "Number of not well formatted objects: " << count_bad << endl;
	#endif
}

/* Build indexing on tile boundaries from cache file */
bool build_index_tiles(ISpatialIndex* &spidx, IStorageManager* &storage) {
	// build spatial index on tile boundaries 
	id_type  indexIdentifier;
	GEOSDataStreamFileTile stream(stop.cachefilename); // input from cache file
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

	if (!extract_params(argc, argv)) {
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

	/* Set the current join index */
	stdin_file_name = getenv("mapreduce_map_input_file");
	if (!stdin_file_name) {
		/* This happens if program is not run in mapreduce
                 *  For testing locally, set/export the environment variable above */
		#ifdef DEBUG
		cerr << "Environment variable mapreduce_map_input_file \
				is not set correctly." << endl;
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

  	if (!build_index_tiles(spidx, storage)) {
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
	
	process_input(join_idx, geom_idx);

	/* Clean up indices */
	delete spidx;
	delete storage;

  	cout.flush();
 	cerr.flush();

	return 0;
}

