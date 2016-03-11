#include <iostream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <getopt.h>
#include <vector>
#include <progparams/string_constants.h>
#include <utilities/tokenizer.h>

using namespace std;

/* This program retrieves the space dimension and total object counts
 *     from input object MBBs
 * Input should be tab separated
 * Offset refers to the start of the min_x, min_y, max_x, max_y sequence
 * The offset is always 1
 *  If the first field is a positive integer, this is the reducer outputting only MBBs
 *  If the first field is -1, this is the reducer handling space dimensions
 * */

int main(int argc, char **argv) {
	double low[2];
	double high[2];
	string input_line;

	/* Count of objects */
	int count = 0;
	int offset = 1;
	int prev_id = 0;

	#ifdef DEBUG
	cerr << "Offset/The first MBB field (adjusted) is : " << offset << endl;
	#endif

	vector<string> fields;
	fields.clear();
	bool cat_mode = false; // whether this is the reducer outputting only mbbs

	int pos = -1;
	int tile_id;

	while(cin && getline(cin, input_line) && !cin.eof()) {
		pos = input_line.find('\t');
		tile_id = stoi(input_line.substr(0, pos));
		if (tile_id >= 0 && prev_id >= 0) {
			cout << 0 << TAB << 0 << input_line.substr(pos) << endl;
		} else if (tile_id >= 0 && prev_id == -1) {
			// Reaching the last of space info records
			cout << "SPACE" << TAB << "T"
				<< TAB << low[0] << TAB << low[1] 
				<< TAB << high[0] << TAB << high[1] 
				<< TAB << count << endl;

			// Output the current line as well
			cout << 0 << TAB << 0 << input_line.substr(pos) << endl;
		} else if (tile_id == -1) {
			// Start reading space dimension information
			tokenize(input_line, fields, TAB, true);
			if (prev_id == -1) {
				low[0] = min(low[0], atof(fields[offset].c_str()));
				low[1] = min(low[1], atof(fields[offset + 1].c_str()));
				high[0] = max(high[0], atof(fields[offset + 2].c_str()));
				high[1] = max(high[1], atof(fields[offset + 3].c_str()));
				count += atol(fields[offset + 4].c_str());
			} else {
				low[0] = atof(fields[offset].c_str());
				low[1] = atof(fields[offset + 1].c_str());
				high[0] = atof(fields[offset + 2].c_str());
				high[1] = atof(fields[offset + 3].c_str());
				count = atol(fields[offset + 4].c_str());
			}	
			fields.clear();
		}
		prev_id = tile_id;
	}
	if (prev_id == -1) {
		// Reaching the last of space info records
		cout << "SPACE" << TAB << "T"
			<< TAB << low[0] << TAB << low[1] 
			<< TAB << high[0] << TAB << high[1] 
			<< TAB << count << endl;
	}
	cout.flush();
	cerr.flush();
}
