#include <iostream>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <vector>
#include <cstdlib> 

#include <progparams/string_constants.h>
#include <utilities/tokenizer.h>

#include <progparams/partition_params.hpp>

#include <boost/program_options.hpp>

static int GLOBAL_MAX_LEVEL = 10000000;

class SpatialObject {
	public:
		double low[2];
		double high[2];
		SpatialObject(double min_x, double min_y, double max_x, double max_y);
};

class QuadtreeNode {
	public:
		double low[2];
		double high[2];
		int level;
		bool isLeaf;
		bool canBeSplit;
		int size;
		QuadtreeNode* children[4];
		std::vector<SpatialObject*> objectList;

		QuadtreeNode(double min_x, double min_y, double max_x, 
				double max_y, int level);

		~QuadtreeNode();

		bool addObject(SpatialObject *object);
		bool intersects(SpatialObject *object);
		bool addObjectIgnore(SpatialObject *object);    
};

#include <partitionalgo/qt/QuadtreeNode.hpp>
