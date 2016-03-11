#include <iostream>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib>

#include <boost/program_options.hpp>
#include <utilities/Timer.hpp>

#include <progparams/string_constants.h>
#include <utilities/tokenizer.h>

#include <progparams/partition_params.hpp>

// global vars
string prefix_tile_id = "FG";
string cache_file_name;


/* Dimensions are set to invalid configs to check for parameter parsing */
double space_left = 0;
double space_right = -1;
double space_top = 0;
double space_bottom = -1;

int num_x_splits = -1;
int num_y_splits = -1;
long objectCount = 0;

//function defs
bool read_input(struct partition_op & partop);
void generate_partitions(struct partition_op & partop);
void update_partop_info(struct partition_op & partop, string uppertileid, string newprefix);
void cleanup(struct partition_op & partop);
bool extract_params_partitioning(int argc, char** argv, 
	struct partition_op & partop);

