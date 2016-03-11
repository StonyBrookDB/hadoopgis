#include <partitionalgo/qt/qt_2d.hpp>

using namespace std;
namespace po = boost::program_options;


// Global variables (used by QuadtreeNode)
vector<QuadtreeNode*> leafNodeList;
vector<SpatialObject*> listAllObjects;
int bucket_size;
long total_count = 0;

string prefix_tile_id = "QT";

//function defs
void process_input();
bool read_input(struct partition_op & partop);
extern void update_partop_info(struct partition_op & partop, 
	string uppertileid, string newprefix);
extern void cleanup(struct partition_op & partop);
extern bool extract_params_partitioning(int argc, char** argv, 
	struct partition_op & partop);

// main method
int main(int argc, char** argv) {
	cout.precision(15);
	struct partition_op partop;
	if (!extract_params_partitioning(argc, argv, partop)) {
		#ifdef DEBUG
		cerr << "Fail to extract parameters" << endl;
		#endif
		return -1;
	}
	bucket_size = partop.bucket_size;   
	if (!read_input(partop)) {
		cerr << "Error reading input in" << endl;
		return -1;
	}
	//cerr << total_count << endl;
	return 0;
}

void process_input(struct partition_op &partop) {
	QuadtreeNode *tree = new QuadtreeNode(partop.low[0], 
		partop.low[1], partop.high[0], partop.high[1], 0);
	leafNodeList.push_back(tree);
	for (vector<SpatialObject*>::iterator it = listAllObjects.begin(); 
		it != listAllObjects.end(); it++) {
		tree->addObject(*it);
	}

	int tid = 0; //bucket id
	//int countLeaf = 0;
	for(vector<QuadtreeNode*>::iterator it = leafNodeList.begin(); it != leafNodeList.end(); it++ ) {
		QuadtreeNode *tmp = *it;
		if (tmp->isLeaf) {
			/* Composite key */
			cout << partop.prefix_tile_id << tid 
				<< TAB << tmp->low[0] << TAB << tmp->low[1] << TAB
				<< tmp->high[0] << TAB << tmp->high[1]
				#ifdef DEBUG
				<<  TAB << tmp->size 
				#endif
				<< endl;
			tid++;
		}
	}

	// Memory cleanup here. 

	for (vector<SpatialObject*>::iterator it = listAllObjects.begin(); it != listAllObjects.end(); it++) { 
		delete *it;
	}

	for(vector<QuadtreeNode*>::iterator it = leafNodeList.begin(); it != leafNodeList.end(); it++ ) {
	//	(*it)->objectList.clear();
		delete *it;

	}
	listAllObjects.clear(); 
	leafNodeList.clear();
	//delete tree;
}

bool read_input(struct partition_op &partop) {
	string input_line;
	string prevtileid = "";
	string tile_id;
	vector<string> fields;
	double low[2];
	double high[2];

	partop.object_count = 0;
	while (cin && getline(cin, input_line) && !cin.eof()) {
		try {
			istringstream ss(input_line);
			ss >> tile_id >> low[0] >> low[1] >> high[0] >> high[1];

			if (prevtileid.compare(tile_id) != 0 && prevtileid.size() > 0) {
				update_partop_info(partop, prevtileid, prevtileid  + prefix_tile_id);
				process_input(partop);
				// total_count += partop.object_count;
				partop.object_count = 0;
			}
			prevtileid = tile_id;
			// Create objects
			SpatialObject *obj = new SpatialObject(low[0], low[1], high[0], high[1]);

			listAllObjects.push_back(obj);

			fields.clear();
			partop.object_count++;
		} catch (...) {

		}
	}
	if (partop.object_count > 0) {
		// Process last tile
		if (partop.region_mbbs.size() == 1) {
			update_partop_info(partop, prevtileid, prefix_tile_id);
		} else {
			// First level of partitioning
			update_partop_info(partop, prevtileid, prevtileid + prefix_tile_id);
		}
		process_input(partop);
	}
	//total_count += partop.object_count;
	cleanup(partop);
	return true;
}

