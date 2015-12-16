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
#include "../common/spatialproc3d_structs.h"
#include "../common/spatialproc3d_params.h"
#include "../extensions/rtree3d/rtree_traversal3d.h"
/* 
 * This program maps 3D objects to their respective tiles given 
 *   a partition schema which is read from a disk
 * or 
 * it can be used to extract MBBs from objects 
 * */

using namespace std;

/* Performance metrics */
clock_t start_reading_data;
clock_t start_query_exec;

clock_t total_reading;
clock_t total_query_exec;

/* Function protoypes */
bool build_index_tiles(IStorageManager* &storage, ISpatialIndex * &spidx);
/* Included in header file */
extern void usage();

const string STR_3D_HEADER = "OFF";

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
			3, 
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
	bool firstObj = true;

	IStorageManager * storage = NULL;
	ISpatialIndex * spidx = NULL;

	// Set the current join index
	stdin_file_name = getenv("mapreduce_map_input_file");
	if (!stdin_file_name) {
		// This happens if program is not run in mapreduce
		//  For testing locally, set/export the environment variable above
		#ifdef DEBUG
		cerr << "Environment variable mapreduce_map_input_file \
			is not set correctly." << endl;
		#endif
		return -1;
	}

	#ifdef DEBUG
	cerr << "Prefix for path 1: " << stop.prefix_1 << endl;
	if (stop.join_cardinality > 1) {
		cerr << "Prefix for path 2" << stop.prefix_2 << endl;
	}
	#endif

	string object_prefix = "";
	string input_line;
	vector<string> fields2;
	double tmp_x, tmp_y, tmp_z;
	double low[3];
	double high[3];

	MyVisitor vis;

	bool object_beg = true;
	stringstream ss;
	string tmp_line;
	ss.str("");

	if (!stop.extract_mbb) {
		if (strstr(stdin_file_name, stop.prefix_1) != NULL) {
			join_idx = SID_1;
		} else if (strstr(stdin_file_name, stop.prefix_2) != NULL) {

			join_idx = SID_2;
		} else {
			cerr << "File name from environment variable \
			does not match any path prefix" << endl;
		}
		stringstream sstmp;
		char *filename = strrchr(stdin_file_name, '/');
		filename++;
		sstmp << filename << BAR;
		object_prefix = sstmp.str();

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
	}
	#ifdef DEBUG
	int countline = 1;
	#endif
	int strbeg = -1;
	int object_count = 0;
	while(cin && getline(cin, input_line) && !cin.eof()){
		#ifdef DEBUG
		cerr << "Reading line "  << input_line << endl;
		#endif


		// Removal of \r symbol on Windows
		if (input_line.at(input_line.size() - 1) == '\r') {
			input_line = input_line.substr(0, input_line.size() - 1);
		}
		// Removal of offset from MapReduce
		if ((strbeg = input_line.find('\t')) > 0) {
			input_line = input_line.substr(strbeg + 1);
		}
		tokenize(input_line, fields2, " ", true);
		if (fields2.size() == 4 && !stop.extract_mbb) {
			ss << BAR << input_line;
		} else if (strcmp(fields2[0].c_str(), "OFF") == 0) {
			// End of object/beginning of next object
			if (!firstObj) {
				// start of next object
				// output mbb
				if (stop.extract_mbb) {
					
					cout << 0 << TAB << low[0] << TAB << low[1] << TAB << low[2]
						<< TAB << high[0] << TAB << high[1] << TAB << high[2] << endl;
				} else {
					Region r(low, high, 3);
					vis.matches.clear();
					spidx->intersectsWithQuery(r, vis);
					for (uint32_t i = 0 ; i < vis.matches.size(); i++ ) {	
						cout << stop.id_tiles[vis.matches[i]] << TAB << join_idx << TAB
						<< object_prefix << object_count // Object "ID"	
						// MBB Info	
						<< TAB << low[0] << TAB << low[1] << TAB << low[2]
						<< TAB << high[0] << TAB << high[1] << TAB << high[2]
						<< TAB << ss.str() << BAR << endl; // Geometry of the object
					}
					object_count++;
					ss.str(""); // Reset for next object
				}

			} else {
				// First object
				firstObj = false;
			}

			ss.str("");
			getline(cin, tmp_line); // consume the next line (number of vertices, edges)
			if (!stop.extract_mbb) {
				// Removal of \r symbol on Windows
				if (tmp_line.at(tmp_line.size() - 1) == '\r') {
					tmp_line = tmp_line.substr(0, tmp_line.size() - 1);
				}
				// Removal of offset from MapReduce
				if ((strbeg = tmp_line.find('\t')) > 0) {
					tmp_line = tmp_line.substr(strbeg + 1);
				}
				ss << STR_3D_HEADER << BAR << tmp_line; 
			}
			object_beg = true;

		} else if (fields2.size() == 3) {
			//cerr << "Field2: " << fields2[0] << TAB << fields2[1] << TAB << fields2[2] << endl;
			if (object_beg) {
				low[0] = high[0] = stod(fields2[0], NULL);
				low[1] = high[1] = stod(fields2[1], NULL);
				low[2] = high[2] = stod(fields2[2], NULL);
				object_beg = false;
			} else {
				tmp_x = stod(fields2[0], NULL);
				tmp_y = stod(fields2[1], NULL);
				tmp_z = stod(fields2[2], NULL);
				if (tmp_x < low[0]) {
					low[0] = tmp_x;
				}
				if (tmp_y < low[1]) {
					low[1] = tmp_y;
				}
				if (tmp_z < low[2]) {
					low[2] = tmp_z;
				}
				if (tmp_x > high[0]) {
					high[0] = tmp_x;
				}
				if (tmp_x > high[1]) {
					high[1] = tmp_y;
				}
				if (tmp_x > high[2]) { 
					high[2] = tmp_z;
				}
			}
			ss << BAR << input_line;
		}
		fields2.clear();
	}

	// Output the last object
	if (stop.extract_mbb) {
		cout << 0 << TAB << low[0] << TAB << low[1] << TAB << low[2]
			<< TAB << high[0] << TAB << high[1] << TAB << high[2] << endl;
	} else {
		Region r(low, high, 3);
		vis.matches.clear();
		spidx->intersectsWithQuery(r, vis);
		for (uint32_t i = 0 ; i < vis.matches.size(); i++ ) {	
			cout << stop.id_tiles[vis.matches[i]] << TAB << join_idx << TAB
				<< object_prefix << object_count++ // Object "ID"	
						// MBB Info	
				<< TAB << low[0] << TAB << low[1] << TAB << low[2]
				<< TAB << high[0] << TAB << high[1] << TAB << high[2]
				<< TAB << ss.str() << BAR << endl; // Geometry of the object
		}
	}

	return 0;

}

