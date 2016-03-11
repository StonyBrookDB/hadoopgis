#include <iostream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <getopt.h>
#include <cstdlib>
#include <vector>
#include <progparams/string_constants.h>

using namespace std;

/* This program performs duplicate removal.
 * There are different modes cat, sort, uniq and uniqpart
 *
 * */


/* Same as system cat program */
void cat() {
	string input_line = "";
	while(cin && getline(cin, input_line) && !cin.eof()) {
		cout << input_line << endl;
	}
}

/* Same as system uniq program  */
void uniq() {
	string input_line = "";
	string prev_line = "";
	while(cin && getline(cin, input_line) && !cin.eof()) {
		if (input_line.compare(prev_line) != 0) {
			cout << input_line << endl;
			prev_line = input_line;
		}	

	}
	/* Output the last line if the line has not been output */
	if (input_line.compare(prev_line) == 0) {
		cout << input_line << endl;
	}
}

/* Same as uniq but it discards the last field */
void uniqpart() {
	string input_line = "";
	string prev_part = "";
	string current_part = "";
	size_t pos = -1;
	while(cin && getline(cin, input_line) && !cin.eof()) {
		pos = input_line.rfind(TAB);
		if (pos == string::npos) {
			cout << input_line << endl;
			continue; // skip the line	
		}
		current_part = input_line.substr(pos + 1);
		if (current_part.compare(prev_part) != 0) {
			cout << input_line << endl;
			prev_part = current_part;
		}	
	}

}


int main(int argc, char **argv) {

	if (argc < 2) {
		cerr << "Missing usage mode. Usage: " << argv[0] << " [ cat | uniq | uniqpart ]" << endl;
		return -1;
	}


	char * usage = argv[1];

	#ifdef DEBUG
	cerr << "Usage mode: " << argv[1] << endl;
	#endif

	if (strcmp(usage, "cat") == 0) {
		cat();
	} else if (strcmp(usage, "uniq") == 0) {
		uniq();
	} else if (strcmp(usage, "uniqpart") == 0) {
		uniqpart();
	} else {
		cerr << "Usage mode not recognized" << endl;
	}


	cout.flush();
	cerr.flush();
}
