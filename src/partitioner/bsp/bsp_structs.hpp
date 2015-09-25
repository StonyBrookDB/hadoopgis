using namespace std;

static int FACTOR = 4;
static int GLOBAL_MAX_LEVEL = 10000000;

class SpatialObject {
public:
    double left, right, top, bottom;
    SpatialObject(double left, double right, double top, double bottom);
};

class BinarySplitNode {
public:
    double left;
    double right;
    double top;
    double bottom;
    int level;
    bool isLeaf;
    bool canBeSplit;
    int size;
    BinarySplitNode* first;
    BinarySplitNode* second;
    vector<SpatialObject*> objectList;
    
    BinarySplitNode(double left, double right, double top, 
        double bottom, int level);
    
    ~BinarySplitNode();
    
    bool addObject(SpatialObject *object);
    bool intersects(SpatialObject *object);
    bool addObjectIgnore(SpatialObject *object);    
};

