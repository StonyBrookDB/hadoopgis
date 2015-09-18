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
 *
 * */


/* Performance metrics */
clock_t start_reading_data;
clock_t start_query_exec;

clock_t total_reading;
clock_t total_query_exec;

/* Function protoypes */
bool build_index_tiles();
bool process_input(const int join_idx, const int geom_idx);

/* Process standard input and emit objects to their respective partitions */
bool process_input(const int join_idx, const int geom_idx) {
	string input_line;
	vector<string> fields;

	Geometry* geom; 
	GeometryFactory *gf = NULL;
	WKTReader *wkt_reader = NULL;
	IStorageManager * storage = NULL;
	ISpatialIndex * spidx = NULL;
	MyVisitor vis;

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
	while(cin && getline(cin, input_line) && !cin.eof()){
		tokenize(input_line, fields, TAB, true);
    		if (fields[geom_idx].length() < 2 ) {
			#ifdef DEBUG
      			cerr << "skipping record [" << id <<"]"<< endl;
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

			low [0] = env->getMinX();
			low [1] = env->getMinY();

			high [0] = env->getMaxX();
			high [1] = env->getMaxY();

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
	GEOSDataStreamFile stream(stop.cachefilename); // input from cache file
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
	}

	char* stdin_file_name = NULL; // name of the input file
	int join_idx = -1; // index of the current file (0 or 1) matching to dataset 1 or 2
	int geom_idx = -1; // geometry field index	
	IStorageManager * storage = NULL;
	ISpatialIndex * spidx = NULL;

	/* Set the current join index */
	stdin_file_name = getenv("mapreduce_map_input_file");
	if (strstr(stdin_file_name, stop.prefix_1) != NULL) {
		join_idx = SID_1;
		geom_idx = stop.shape_idx_1;
	} else if (strstr(stdin_file_name, stop.prefix_2) != NULL) {
		join_idx = SID_2;
		geom_idx = stop.shape_idx_2;
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

