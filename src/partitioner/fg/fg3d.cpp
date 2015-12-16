#include <iostream>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <algorithm>   
#include <vector>
#include <map>
#include <cstdlib> 
#include "common/string_constants.h"
#include "common/Timer.hpp"
#include <boost/program_options.hpp>

using namespace std;

// global vars 
namespace po = boost::program_options;

string prefix_tile_id = "FG";

int num_x_splits = -1;
int num_y_splits = -1;
int num_z_splits = -1;
int bucket_size = 1;
int object_count = 0;

double global_minX, global_minY, global_minZ, global_maxX, global_maxY, global_maxZ;
double global_xSpan, global_ySpan, global_zSpan;

bool readInput() {
	string input_line;
	while (cin && getline(cin, input_line)) {
		object_count++;
	}
	return true;
}

// main method
int main(int ac, char** av) {
	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "this help message")
			("offset,f", "Field offset")
			("bucket,b", po::value<int>(&bucket_size), "Expected bucket size")
			("min_x,k", po::value<double>(&global_minX), "(Optional) Spatial min x")
			("min_y,l", po::value<double>(&global_minY), "(Optional) Spatial min y")
			("min_z,m", po::value<double>(&global_minZ), "(Optional) Spatial min z")
			("max_x,n", po::value<double>(&global_maxX), "(Optional) Spatial max x")
			("max_y,o", po::value<double>(&global_maxY), "(Optional) Spatial max y")
			("max_z,p", po::value<double>(&global_maxZ), "(Optional) Spatial max z")
			("num_x,x", po::value<int>(&(num_x_splits)), "(Optional) Number of splits along x")
			("num_y,y", po::value<int>(&(num_y_splits)), "(Optional) Number of splits along y")
			("num_z,z", po::value<int>(&(num_z_splits)), "(Optional) Number of splits along z");

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

	cout.precision(15);
	readInput();

	int partition_num = max(static_cast<int>(ceil(object_count / bucket_size)), 1) ;

	double minX, minY, minZ, maxX, maxY, maxZ;


	#ifdef DEBUG
	cerr << global_minX << TAB << global_minY << TAB << global_minZ 
		<< TAB << global_maxX << TAB << global_maxY << TAB << global_maxZ << endl;
	#endif

	global_xSpan = global_maxX - global_minX;
	global_ySpan = global_maxY - global_minY;
	global_zSpan = global_maxZ - global_minZ;
	
	if (num_x_splits < 0 || num_y_splits < 0 || num_z_splits < 0) {
		/* Number of splits are not set */
		if (global_zSpan > global_ySpan && global_ySpan> global_xSpan) {
			// We prefer to split into more-cube 
			num_z_splits = max(ceil(cbrt(static_cast<double> (partition_num) * global_zSpan*global_zSpan / (global_xSpan*global_ySpan))), 1.0);
			num_y_splits = max(ceil(cbrt(static_cast<double>(partition_num) * global_ySpan*global_ySpan / (global_xSpan*global_zSpan))), 1.0);
			num_x_splits = max(ceil(static_cast<double>(partition_num) / (num_y_splits*num_z_splits)), 1.0);
		}
		else if (global_zSpan > global_xSpan && global_xSpan> global_ySpan) {
			// We prefer to split into more-cube 
			num_z_splits = max(ceil(cbrt(static_cast<double> (partition_num)* global_zSpan*global_zSpan / (global_xSpan*global_ySpan))), 1.0);
			num_x_splits = max(ceil(cbrt(static_cast<double>(partition_num)* global_xSpan*global_xSpan / (global_ySpan*global_zSpan))), 1.0);
			num_y_splits = max(ceil(static_cast<double>(partition_num) / (num_x_splits*num_z_splits)), 1.0);
		}else if (global_ySpan > global_xSpan && global_xSpan > global_zSpan) {
			// We prefer to split into more-cube 
			num_y_splits = max(ceil(cbrt(static_cast<double> (partition_num)* global_ySpan*global_ySpan / (global_xSpan*global_zSpan))), 1.0);
			num_x_splits = max(ceil(cbrt(static_cast<double>(partition_num)* global_xSpan*global_xSpan / (global_ySpan*global_zSpan))), 1.0);
			num_z_splits = max(ceil(static_cast<double>(partition_num) / (num_x_splits*num_y_splits)), 1.0);
		}else if (global_ySpan > global_zSpan && global_zSpan > global_xSpan) {
			// We prefer to split into more-cube 
			num_y_splits = max(ceil(cbrt(static_cast<double> (partition_num)* global_ySpan*global_ySpan / (global_xSpan*global_zSpan))), 1.0);
			num_z_splits = max(ceil(cbrt(static_cast<double>(partition_num)* global_zSpan*global_zSpan / (global_ySpan*global_xSpan))), 1.0);
			num_x_splits = max(ceil(static_cast<double>(partition_num) / (num_y_splits*num_z_splits)), 1.0);
		}	else if (global_xSpan> global_ySpan && global_ySpan > global_zSpan) {
			// We prefer to split into more-cube 
			num_x_splits = max(ceil(cbrt(static_cast<double> (partition_num)* global_xSpan*global_xSpan / (global_ySpan*global_zSpan))), 1.0);
			num_y_splits = max(ceil(cbrt(static_cast<double>(partition_num)* global_ySpan*global_ySpan / (global_xSpan*global_zSpan))), 1.0);
			num_z_splits = max(ceil(static_cast<double>(partition_num) / (num_x_splits*num_y_splits)), 1.0);
		} else if (global_xSpan> global_zSpan && global_zSpan > global_ySpan) {
			// We prefer to split into more-cube 
			num_x_splits = max(ceil(cbrt(static_cast<double> (partition_num)* global_xSpan*global_xSpan / (global_ySpan*global_zSpan))), 1.0);
			num_z_splits = max(ceil(cbrt(static_cast<double>(partition_num)* global_zSpan*global_zSpan / (global_ySpan*global_xSpan))), 1.0);
			num_y_splits = max(ceil(static_cast<double>(partition_num) / (num_x_splits*num_z_splits)), 1.0);
		}else{
			// We prefer to split into more-cube 
			num_x_splits = max(ceil(cbrt(static_cast<double> (partition_num))), 1.0);
			num_y_splits = max(ceil(cbrt(static_cast<double>(partition_num))), 1.0);
			num_z_splits = max(ceil(static_cast<double>(partition_num) / (num_x_splits * num_y_splits)), 1.0);
		}
	}

	/* num_x_splits , num_y_splits and num_z_splits should have been set by here */
	double width_x = global_xSpan / num_x_splits;
	double width_y = global_ySpan / num_y_splits;
	double width_z = global_zSpan / num_z_splits;

	int tid = 1;
	double node_minX, node_minY, node_minZ, node_maxX, node_maxY, node_maxZ;

	for (int i = 0; i < num_x_splits; i++) {
		for (int j = 0; j < num_y_splits; j++) {
			for (int k = 0; k < num_z_splits; k++) {

				node_minX = global_minX + i * width_x;
				node_minY = global_minY + j * width_y;
				node_minZ = global_minZ + k * width_z;
				node_maxX = node_minX + width_x;
				node_maxY = node_minY + width_y;
				node_maxZ = node_minZ + width_z;

				cout << prefix_tile_id << tid
					<< TAB << node_minX << TAB << node_minY << TAB << node_minZ << TAB
					<< node_maxX << TAB << node_maxY << TAB << node_maxZ << endl;
				tid++;
			}
		}
	}

	// Memeory cleanup here. 
	// delete the stuff inside your vector

	return 0;	
}


	
