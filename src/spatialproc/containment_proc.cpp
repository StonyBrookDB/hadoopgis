#include <iostream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include "../common/string_constants.h"
#include "../common/tokenizer.h"
#include "../common/resque_constants.h"
#include "../common/spatialproc_structs.h"
#include "../common/spatialproc_params.h"
#include "../common/rtree_traversal.h"
/* 2D Mapping object to tile / MBB extractor 
 * This program maps objects to their respective tiles given 
 *   a partition schema which is read from a disk
 * or it can be used to extract MBBs from objects
 * */


/* Performance metrics */
#ifdef DEBUGTIME
clock_t start_reading_data;
clock_t start_query_exec;

clock_t total_reading;
clock_t total_query_exec;
#endif

bool process_input(const int geom_idx);

/* Process standard input and emit objects to their respective partitions */
bool process_input(const int geom_idx) {
	#ifdef DEBUG
	cerr << "Start processing input" << endl;
	#endif

	string input_line;
	vector<string> fields;
	bool firstLineRead = false;

	Geometry* geom; 
	GeometryFactory *gf = NULL;
	WKTReader *wkt_reader = NULL;

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


	#ifdef DEBUGTIME
	start_reading_data = clock();
	#endif


	ifstream instr(stop.cachefilename);
	if (instr.is_open()) {
		getline(instr, input_line);
	} else {
		cerr << "Error reading cache file" << endl;
		exit(1);
	}
	// The window we are checking for
	Geometry *windowobj = wkt_reader->read(input_line);
	const Envelope * env_windowobj = windowobj->getEnvelopeInternal();

	#ifdef DEBUG
	cerr << "Done reading input object." << endl;
	#endif

	/* Handling standard input */
	while(cin && getline(cin, input_line) && !cin.eof()){
		tokenize(input_line, fields, TAB, true);
		count_objects++;
		try {
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

			if (env->intersects(env_windowobj) && geom->intersects(windowobj)) {
				cout << input_line << endl;	
			}
			delete geom;

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
	delete windowobj;
	delete gf;
	delete wkt_reader;

	#ifdef DEBUG
	/* Output useful statistics */
	cerr << "Number of processed objects: " << count_objects << endl;
	cerr << "Number of satisfied objects. " << count_emitted << endl;
	#endif
}

int main(int argc, char **argv) {

	if (!extract_params(argc, argv)) {
		#ifdef DEBUG 
		cerr <<"ERROR: query parameter extraction error." << endl 
			<< "Please see documentations, or contact author." << endl;
		#endif
		return -1;
	}
	// Process input line by line
	process_input(stop.shape_idx_1);

	cout.flush();
	cerr.flush();

	return 0;
}

