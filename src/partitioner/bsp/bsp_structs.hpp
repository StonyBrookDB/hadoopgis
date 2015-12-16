using namespace std;

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
		vector<SpatialObject*> objectList;

		BinarySplitNode(double min_x, double min_y, double max_x, 
				double max_y, int level);

		~BinarySplitNode();

		bool addObject(SpatialObject *object);
		bool intersects(SpatialObject *object);
		bool addObjectIgnore(SpatialObject *object);    
};

