#include <iostream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <getopt.h>
#include <vector>
#include "../common/string_constants.h"
#include "../common/resque_constants.h"
#include "../common/tokenizer.h"

using namespace std;

/* This program retrieves the space dimension and total object counts
 *     from input object MBBs
 * Input should be tab separated
 * Offset refers to the start of the min_x, min_y, max_x, max_y sequence
 * The offset is 1 for object MBB (accounting for object ID)
 * The offset is 0 for input coming from a previous get_space_dimension program 
 * Only 1 parameter is the offset (counting from 1)
 *
 * */

int main(int argc, char **argv) {
	double min_x;
	double max_x;
	double min_y;
	double max_y;
	string input_line;

	/* Count of objects */
	int count = 0;

	if (argc < 2) {
		cerr << "Missing offset field. Usage: " << argv[0] << " [offset]" << endl;
		return -1;
	}


	int offset = strtol(argv[1], NULL, 10) - 1;

	#ifdef DEBUG
	cerr << "Offset/The first MBB field (adjusted) is : " << offset << endl;
	#endif

	vector<string> fields;
	fields.clear();

	/* Reading the first line */
	if(cin && getline(cin, input_line) && !cin.eof()) {
		tokenize(input_line, fields, TAB, true);   

		min_x = atof(fields[offset].c_str());
		min_y = atof(fields[offset + 1].c_str());
		max_x = atof(fields[offset + 2].c_str());
		max_y = atof(fields[offset + 3].c_str());
		if (offset == 0) {
			count = atol(fields[offset + 4].c_str());
		} else {
			count = 1;
		}

		fields.clear();
	}

	/* Processing remaining lines */
	while(cin && getline(cin, input_line) && !cin.eof()) {
		tokenize(input_line, fields, TAB, true);   

		min_x = min(min_x, atof(fields[offset].c_str()));
		min_y = min(min_y, atof(fields[offset + 1].c_str()));
		max_x = max(max_x, atof(fields[offset + 2].c_str()));
		max_y = max(max_y, atof(fields[offset + 3].c_str()));

		if (offset == 0) {
			count += atol(fields[offset + 4].c_str());
		} else {
			count++;
		}

		fields.clear();
	}

	/* Output the statistics */
	if (count > 0) {
		cout << min_x << TAB << min_y << TAB 
			<< max_x << TAB << max_y 
			<< TAB << count << endl;
	}

	cout.flush();
	cerr.flush();
}
