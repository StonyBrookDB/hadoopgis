
/* Containing methods to extract parameters and store them in query operator */
#include <iostream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <iterator>
#include <cstdlib> 
#include <vector>
#include <boost/program_options.hpp>
#include <progparams/partition_datastructs.hpp>

#define NUMBER_DIMENSIONS 2

namespace po = boost::program_options;

void init_params_partitioning(struct partition_op & partop) {
	partop.to_be_normalized = false;
	partop.to_be_denormalized = false;
	partop.offset = 1; // Default offset
}

// Read the cache file containing MBB of regions
bool read_partition_file(struct partition_op &partop) {
	string inputline;
	string tile_id;
	ifstream infile(partop.file_name);

	while (getline(infile, inputline)) {
		std::istringstream ss(inputline);
		struct mbb_info *tmp = new struct mbb_info();
		if (!(ss >> tile_id >> tmp->low[0] >> tmp->low[1] >> tmp->high[0] >> tmp->high[1])) { 
			// Incorrect parsing
			return false;
		}

		partop.region_mbbs[tile_id] = tmp;
	}
	// Update the global information
	if (partop.region_mbbs.size() == 1) {
		struct mbb_info *tmp = partop.region_mbbs.begin()->second;
		partop.low[0] = tmp->low[0];
		partop.low[1] = tmp->low[1];
		partop.high[0] = tmp->high[0];
		partop.high[1] = tmp->high[1];
	}

	return true;
}


void update_partop_info(struct partition_op & partop, string uppertileid, string newprefix) {
	partop.prefix_tile_id = newprefix;
	if (partop.region_mbbs.size() > 1) {
		struct mbb_info *tmp = partop.region_mbbs[uppertileid]; 
		// cerr << "Updating info :" << tmp->low[0] << TAB << tmp->low[1] << TAB 
		// << tmp->high[0] << TAB << tmp->high[1] << endl;
		partop.low[0] = tmp->low[0];
		partop.low[1] = tmp->low[1];
		partop.high[0] = tmp->high[0];
		partop.high[1] = tmp->high[1];
	}
}

/* Extract parameters for partitioning */
bool extract_params_partitioning(int ac, char** av, struct partition_op & partop){ 
	init_params_partitioning(partop);
	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "this help message")
			("norm", "Normalize the data")
			("denorm", "Denormalize the data")
			("bucket,b", po::value<long>(&partop.bucket_size), "Expected bucket size")
			("offset,f", po::value<int>(&partop.offset), "Offset from where MBB starts")      
			("cachefilename,c", po::value<string>(&(partop.file_name)), "(Optional) Name of cache file")
			("min_x,k", po::value<double>(&(partop.low[0])), "(Optional) Spatial min x")
			("min_y,l", po::value<double>(&(partop.low[1])), "(Optional) Spatial min y")
			("max_x,m", po::value<double>(&(partop.high[0])), "(Optional) Spatial max x")
			("max_y,n", po::value<double>(&(partop.high[1])), "(Optional) Spatial max y");
		po::variables_map vm;        
		po::store(po::parse_command_line(ac, av, desc), vm);
		po::notify(vm);   

		if (vm.count("help")) {
			cerr << desc <<	endl;
			return 0;
		}

		if (vm.count("norm")) {
			partop.to_be_normalized = true;
		}
		if (vm.count("denorm")) {
			partop.to_be_denormalized = true;
		}

		if (vm.count("cachefilename")) {
			read_partition_file(partop);
		}

	} catch(exception& e) {
		cerr << "error: " << e.what() << "\n";
		return false;
	}
	catch(...) {
		cerr << "Exception of unknown type!\n";
		return false;
	}

	if (partop.to_be_denormalized && partop.to_be_normalized) {
		/* Mutually exclusive options */
		#ifdef DEBUG
		cerr << "Mutually exclusive functions -n and -o" << endl;
		#endif
		return false;
	}

	return true;
}

void cleanup(struct partition_op & partop){
	for (std::map<string,struct mbb_info *>::iterator it= partop.region_mbbs.begin();
			it!= partop.region_mbbs.end(); ++it) {
		delete it->second;
	}
	partop.region_mbbs.clear();
}
