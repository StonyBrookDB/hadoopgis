

#include <iostream>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <vector>
#include <cstdlib> 
#include <getopt.h>
#include <boost/program_options.hpp>
#include <utilities/Timer.hpp>

#include <progparams/string_constants.h>
#include <utilities/tokenizer.h>
#include <spatialindex/SpatialIndex.h>

#include <progparams/partition_params.hpp>

#ifdef DEBUGAREA
#include <utilities/tile_rectangle.h>
#endif

#define NUMBER_DIMENSIONS 2
/* 
 * Hilbert curve partitioning
 * */

using namespace std;
using namespace SpatialIndex;
using namespace SpatialIndex::RTree;

namespace po = boost::program_options;

int FILLING_CURVE_PRECISION = 0; // precision 

bool read_input(struct partition_op & partop);
void generatePartitions(struct partition_op & partop);
void process_input(struct partition_op &partop);
long hilbert2d (int x, int y);
long hilbert2d (int x, int y, unsigned int mask);
long getHilbertValue(double obj[]);

extern void update_partop_info(struct partition_op & partop, 
		string uppertileid, string newprefix);
extern void cleanup(struct partition_op & partop);

struct HilberCurveSorter {
	bool operator() (double i[], double j[]) { 
		return (getHilbertValue(i)< getHilbertValue(j));
	}
} hcsorter;


// Global variables
vector<double*> obj_list;  // spatial object collection 
string prefix_tile_id = "HC";
