#include <iostream>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include "../../common/string_constants.h"
#include "../../common/Timer.hpp"
#include <boost/program_options.hpp>

using namespace std;
// using namespace boost;
namespace po = boost::program_options;

//function defs
bool readInput();
void generatePartitions();

// global vars 
int bucket_size;
string prefix_tile_id = "FG";

/* Dimensions are set to invalid configs to check for parameter parsing */
double space_left = 0;
double space_right = -1;
double space_top = 0;
double space_bottom = -1;

int num_x_splits = -1;
int num_y_splits = -1;
long objectCount = 0;

// main method
int main(int ac, char** av) {
	cout.precision(15);
	int numxsplits;

	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "this help message")
			("bucket,b", po::value<int>(&bucket_size), "Expected bucket size")
			("min_x,k", po::value<double>(&space_left), "(Optional) Spatial min x")
			("min_y,l", po::value<double>(&space_bottom), "(Optional) Spatial min y")
			("max_x,m", po::value<double>(&space_right), "(Optional) Spatial max x")
			("max_y,n", po::value<double>(&space_top), "(Optional) Spatial max y")
			("num_x,x", po::value<int>(&(num_x_splits)), "(Optional) Number of splits along x")
			("num_y,y", po::value<int>(&(num_y_splits)), "(Optional) Number of splits along y");

		po::variables_map vm;        
		po::store(po::parse_command_line(ac, av, desc), vm);
		po::notify(vm);    

		if ( vm.count("help") || (! vm.count("bucket"))) {
			cerr << desc << endl;
			return 0; 
		}

		// Adjust bucket size
		#ifdef DEBUG
		cerr << "Bucket size: " << bucket_size <<endl;
		#endif
	}
	catch(exception& e) {
		cerr << "error: " << e.what() << "\n";
		return 1;
	}
	catch(...) {
		cerr << "Exception of unknown type!\n";
		return 1;
	}
	// argument parsing is done here.


	if (!readInput()) {
		cerr << "Error reading input in" << endl;
		return -1;
	}

	#ifdef DEBUGTIME
	Timer t; 
	#endif

	generatePartitions();	

	return 0;
}

void generatePartitions() {
	double span_x, span_y;

	int tid = 1; //tile id
	if (space_left > space_right || space_bottom > space_top) {
		span_x = span_y = 1.0;
	} else {
		span_x = space_right - space_left;
		span_y = space_top - space_bottom;
	}

	#ifdef DEBUG
	cerr << "Number of x splits: " << num_x_splits << endl;
	cerr << "Number of y splits: " << num_y_splits << endl;
	#endif	

	if (num_x_splits < 0 || num_y_splits < 0) {
		/* Number of splits are not set */
		if (span_y > span_x) {
			// We prefer to split into more-square regions than long rectangles
			num_y_splits = max(ceil(sqrt(static_cast<double>(objectCount) / bucket_size 
				* span_y / span_x)), 1.0);	
			num_x_splits = max(ceil(static_cast<double>(objectCount) / bucket_size 
				/ num_y_splits), 1.0);
		} else {
			num_x_splits = max(ceil(sqrt(static_cast<double>(objectCount) / bucket_size 
				* span_x / span_y)), 1.0);	
			num_y_splits = max(ceil(static_cast<double>(objectCount) / bucket_size 
				/ num_x_splits), 1.0);
		}
	}	

	#ifdef DEBUG
	cerr << "Number of x splits: " << num_x_splits << endl;
	cerr << "Number of y splits: " << num_y_splits << endl;
	#endif	

	/* num_x_splits and num_y_splits should have been set by here */
	double width_x = 1.0 / num_x_splits;
	double width_y = 1.0 / num_y_splits;
	for (int i = 0; i < num_x_splits; i++) {
		for (int j = 0; j < num_y_splits; j++) {
			cout << prefix_tile_id << BAR << tid 
				<< TAB << i * width_x  << TAB << j * width_y << TAB
				<< (i + 1) * width_x << TAB << (j + 1) * width_y << endl;
			tid++;
		}
	} 


}

bool readInput() {
	string input_line;

	while (cin && getline(cin, input_line)) {
		objectCount++;
	}

	return true;
}

	
