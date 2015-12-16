#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <getopt.h>
#include <vector>
#include "../common/string_constants.h"
#include "../common/resque_constants.h"
#include "../common/tokenizer.h"
#include "../common/partition3d_structs.h"
#include "../common/partition3d_params.h"

/* 
 * This program extracts, normalizes or denormalizes 3-D MBBs
 *  give the space dimensions
 *  Input: MBBs one per line
 *  Output: MBBs one per line
 * */

extern bool extract_params_partitiong(int argc, char** argv, struct partition_op & partop);

using namespace std;

int main(int argc, char **argv) {
	/* Extracting parameters */
	struct partition_op partop;
	if (!extract_params_partitioning(argc, argv, partop)) {
		#ifdef DEBUG
			cerr << "Fail to extract parameters" << endl;
		#endif
		return -1;
	}

	/* Tile/Image span */
  	double space_x_span;
	double space_y_span; 
	double space_z_span; 
  	
	/* For processing individual line */
	string input_line;
	vector<string> fields;
	double min_x, min_y, min_z, max_x, max_y, max_z;

	/* Parallel partitioning: index file of first layer of partitioning  */
	string tmpTileID;
	struct mbb_info * tmp;
	map<string, struct mbb_info *> global_partition_index;
	int pos = 0;
	#ifdef DEBUG
	cerr << "Starting reading global index file." << endl;
	#endif


	if (partop.parallel_partitioning) {
		// open index file
		ifstream filein(partop.file_name);
		while  (getline(filein, input_line)) {
			istringstream ssline(input_line);
			tmp = new struct mbb_info();
			ssline >> tmpTileID  >> tmp->low[0] 
				>> tmp->low[1] >> tmp->low[2]
				>> tmp->high[0] >> tmp->high[1]
				>> tmp->high[2];
			global_partition_index[tmpTileID] = tmp;
		}
	}

	#ifdef DEBUG
	cerr << "Finished reading global index file." << endl;
	#endif

	space_x_span  = partop.max_x - partop.min_x;
	space_y_span = partop.max_y - partop.min_y;
	space_z_span = partop.max_z - partop.min_z;

	while(cin && getline(cin, input_line) && !cin.eof()) {
		tokenize(input_line, fields, TAB, true);   

		if (partop.to_be_normalized) {
			min_x = (atof(fields[partop.offset].c_str()) - partop.min_x) / space_x_span;
			min_y = (atof(fields[partop.offset + 1].c_str()) - partop.min_y) / space_y_span;
			min_z = (atof(fields[partop.offset + 2].c_str()) - partop.min_z) / space_z_span;
			max_x = (atof(fields[partop.offset + 3].c_str()) - partop.min_x) / space_x_span;
			max_y = (atof(fields[partop.offset + 4].c_str()) - partop.min_y) / space_y_span;
			max_z = (atof(fields[partop.offset + 5].c_str()) - partop.min_z) / space_z_span;
		} else if (partop.to_be_denormalized) {
			/* Parallel partitioning requires 2 denormalizations */
			if (partop.parallel_partitioning) {
				tmpTileID = fields[0].substr(0, fields[0].find(BAR));
				tmp = global_partition_index[tmpTileID];
			
				/* Denormalize to the tile level */
				min_x = atof(fields[partop.offset].c_str()) 
					* (tmp->high[0] - tmp->low[0]) + tmp->low[0];
				min_y = atof(fields[partop.offset + 1].c_str()) 
					* (tmp->high[1] - tmp->low[1]) + tmp->low[1];
				min_z = atof(fields[partop.offset + 2].c_str()) 
					* (tmp->high[2] - tmp->low[2]) + tmp->low[2];
				max_x = atof(fields[partop.offset + 3].c_str()) 
					* (tmp->high[0] - tmp->low[0]) + tmp->low[0];
				max_y = atof(fields[partop.offset + 4].c_str()) 
					* (tmp->high[1] - tmp->low[1]) + tmp->low[1];
				max_z = atof(fields[partop.offset + 5].c_str()) 
					* (tmp->high[2] - tmp->low[2]) + tmp->low[2];
				/* Denormalize to the global level */
				min_x = min_x * space_x_span + partop.min_x;
				min_y = min_y * space_y_span + partop.min_y;
				min_z = min_z * space_z_span + partop.min_z;
				max_x = max_x * space_x_span + partop.min_x;
				max_y = max_y * space_y_span + partop.min_y;
				max_z = max_z * space_z_span + partop.min_z;
			} else {

				/* Regular denormalization for single partitioning  */
				min_x = atof(fields[partop.offset].c_str()) 
						* space_x_span + partop.min_x;
				min_y = atof(fields[partop.offset + 1].c_str()) 
						* space_y_span + partop.min_y;
				min_z = atof(fields[partop.offset + 2].c_str()) 
						* space_z_span + partop.min_z;
				max_x = atof(fields[partop.offset + 3].c_str()) 
						* space_x_span + partop.min_x;
				max_y = atof(fields[partop.offset + 4].c_str()) 
						* space_y_span + partop.min_y;
				max_z = atof(fields[partop.offset + 5].c_str()) 
						* space_z_span + partop.min_z;
			}

		}


		/* Output fields */
     		cout << fields[0] << TAB << min_x << TAB << min_y << TAB << min_z
          		<< TAB << max_x << TAB << max_y << TAB << max_z << endl;
    		fields.clear();
	}



	/* Cleaning up */
	if (partop.parallel_partitioning) {
		for (map<string, struct mbb_info *>::iterator it= global_partition_index.begin(); 
			it != global_partition_index.end(); ++it) {
			delete (it->second);
		}
		global_partition_index.clear();
	}
	cerr.flush();
	cout.flush();
}
