#include <iostream>
#include <cstring>
#include <cmath>
#include <cstdlib> 
#include <limits>
#include <ctime>
#include <utilities/tokenizer.h>

using namespace std;

/* This program performs random coin flipping with a fixed sampling rate
 * */
#define TAB "\t"

int main(int argc, char **argv) {
	vector<string> fields;
	string input_line;
	string tileid = "Empty";
	string previd = "";

	long obj_count;
	long running_total = 0;
	
	while(cin && getline(cin, input_line) && !cin.eof()){
		try {
			fields.clear();
			tokenize(input_line, fields, TAB, true);
			tileid = fields[0];
			obj_count = strtol(fields[5].c_str(), NULL, 10); // 1st field is tile id, next 4 fields are MBB information
			cerr << "Reading" << tileid << endl;	
			if (tileid.compare(previd) == 0) {
				running_total += obj_count;
			} else {
				if (previd.size() > 0) {
					cout << previd << TAB << fields[1] << TAB 
						<< fields[2] << TAB 
						<< fields[3] << TAB 
						<< fields[4] << TAB 
						<< running_total << endl;
				} else {
					cerr << "Been here" << endl;
				}
				previd = tileid;
				running_total = obj_count;
			}
			
		} catch (...) {
			cerr << "Bad format" << endl;
		}
	}
	// Last word
	if (tileid.compare(previd) == 0 && fields.size() >= 6) {
		cout << tileid << TAB << fields[1] << TAB 
			<< fields[2] << TAB 
			<< fields[3] << TAB 
			<< fields[4] << TAB 
			<< running_total << endl;
		cerr << "outputting lastword" << endl;
	}
	cout.flush();
}
