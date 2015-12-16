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
/* This program collect statistics on number of object counts per tile
 * */


/* Function protoypes */
bool build_index_tiles(IStorageManager* &storage, ISpatialIndex * &spidx);
bool process_input(IStorageManager * &storage, ISpatialIndex * &spidx);

/* Included in header file */
extern void usage();

/* Process standard input and emit objects to their respective partitions */
bool process_input(IStorageManager * &storage, ISpatialIndex * &spidx) {
	string input_line;
	vector<string> fields;
	bool firstLineRead = false;

	Geometry* geom; 
	GeometryFactory *gf = NULL;
	WKTReader *wkt_reader = NULL;
	MyVisitor vis;

	/* Space info */
	double space_low[2];
	double space_high[2];

	gf = new GeometryFactory(new PrecisionModel(), 0);
	wkt_reader= new WKTReader(gf);


	long count_objects = 0;
	#ifdef DEBUG
	long count_emitted = 0;
	long count_bad = 0;
	#endif

	//vis.matches.clear();

	#ifdef DEBUGTIME
	start_reading_data = clock();
	#endif

	double low[2];
	double high[2];

	map<int, long> tile_counts;
	for (std::map<int, string>::iterator it = stop.id_tiles.begin(); it!= stop.id_tiles.end(); it++) {
		tile_counts[it->first] = 0;
	}

	/* Handling standard input */
	while(cin && getline(cin, input_line) && !cin.eof()){
		/* Sampling (optional parameter) */
		/*if (stop.use_sampling &&
		  (static_cast<float>(rand()) / static_cast<float>(RAND_MAX)) > stop.sample_rate) {
		  continue; // skip the record
		  }*/
		tokenize(input_line, fields, TAB, true);
		count_objects++;
		try {
			low[0] = stod(fields[1]);
			low[1] = stod(fields[2]);
			high[0] = stod(fields[3]);
			high[1] = stod(fields[4]);
			Region r(low, high, 2);
			/* Find objects matching with intersecting tiles*/	
			spidx->intersectsWithQuery(r, vis);

			/* Emit objects to intersecting tiles */
			for (uint32_t i = 0 ; i < vis.matches.size(); i++ ) {
				tile_counts[vis.matches[i]]++;
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
	int k = 0;
	/*
	for ( ; k < stop.id_tiles.size(); k++) {
		cout << k << TAB << tile_counts[k] << endl;	
	}
	*/	

	
	ifstream inputfile(stop.cachefilename);
	while(getline(inputfile, input_line)) {
		cout << input_line << TAB << tile_counts[k] << endl;
		k++;
	}


	/*
	for (std::map<int, string>::iterator it = id_tiles.begin(); it!= id_tiles.end(); it++) {
		k = it->first();
		cout << id_tiles[tile_counts[it->first()]] <<
			<< TAB << geom_tiles  endl;
	}*/

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
bool build_index_tiles(IStorageManager* &storage, ISpatialIndex * &spidx) {
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
	IStorageManager * storage = NULL;
	ISpatialIndex * spidx = NULL;

	if( !build_index_tiles(storage, spidx)) {
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

	process_input(storage, spidx);

	/* Clean up indices */
	delete spidx;
	delete storage;

	cout.flush();
	cerr.flush();

	return 0;
}

