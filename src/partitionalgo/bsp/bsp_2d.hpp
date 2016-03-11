
#include <iostream>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <vector>
#include <cstdlib> 
#include <boost/program_options.hpp>
#include <utilities/Timer.hpp>

#include <progparams/string_constants.h>
#include <utilities/tokenizer.h>

#include <progparams/partition_params.hpp>


static int FACTOR = 4;
static int GLOBAL_MAX_LEVEL = 10000000;

class SpatialObject {
	public:
		double low[2];
		double high[2];
		SpatialObject(double min_x, double min_y, double max_x, double max_y);
};

class BinarySplitNode {
	public:
		double low[2];
		double high[2];
		int level;
		bool isLeaf;
		bool canBeSplit;
		int size;
		BinarySplitNode* first;
		BinarySplitNode* second;
		std::vector<SpatialObject*> objectList;

		BinarySplitNode(double min_x, double min_y, double max_x, 
				double max_y, int level);

		~BinarySplitNode();

		bool addObject(SpatialObject *object);
		bool intersects(SpatialObject *object);
		bool addObjectIgnore(SpatialObject *object);    
};

// Global variables (used by BinarySplitNode)
std::vector<BinarySplitNode*> leafNodeList;
std::vector<SpatialObject*> listAllObjects;
int bucket_size;
long total_count = 0;

string prefix_tile_id = "BSP";

//function defs

void process_input();

bool read_input(struct partition_op & partop);
void update_partop_info(struct partition_op & partop, 
	string uppertileid, string newprefix);

 #include <partitionalgo/bsp/BinarySplitNode.hpp>

