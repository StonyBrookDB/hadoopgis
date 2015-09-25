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
#include "bsp_structs.hpp"
#include "BinarySplitNode.hpp"

using namespace std;
namespace po = boost::program_options;

//function defs
void processInput();
bool readInput();

// global vars 
int bucket_size ;
vector<BinarySplitNode*> leafNodeList;
vector<SpatialObject*> listAllObjects;
BinarySplitNode *tree;
stringstream mystr;
string prefix_tile_id;
double left, right, top, bottom;

// main method
int main(int ac, char** av) {
	cout.precision(15);

	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "this help message")
			("bucket,b", po::value<int>(&bucket_size), "Expected bucket size");

		po::variables_map vm;        
		po::store(po::parse_command_line(ac, av, desc), vm);
		po::notify(vm);    

		if ( vm.count("help") || ! vm.count("bucket") ) {
			cerr << desc << endl;
			return 0; 
		}

		// Adjust bucket size
		#ifdef DEBUG
		cerr << "Bucket size: "<<bucket_size <<endl;
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
	processInput();
	

	#ifdef DEBUGTIME
	double elapsed_time = t.elapsed();
	cerr << "stat:ptime," << bucket_size << COMMA << elapsed_time << endl;
	#endif

	// Memeory cleanup here. 
	// delete the stuff inside your vector
	//
	for (vector<SpatialObject*>::iterator it = listAllObjects.begin(); it != listAllObjects.end(); it++) 
		delete *it;
	
	
	for(vector<BinarySplitNode*>::iterator it = leafNodeList.begin(); it != leafNodeList.end(); it++ ) {
		(*it)->objectList.clear();
		delete *it;

	}

	delete tree;

	listAllObjects.clear(); 
	leafNodeList.clear();

	return 0;
}

void processInput() {
	BinarySplitNode *tree = new BinarySplitNode(0.0, 1.0, 1.0, 0.0, 0);
	leafNodeList.push_back(tree);
	for (vector<SpatialObject*>::iterator it = listAllObjects.begin(); it != listAllObjects.end(); it++) {
		tree->addObject(*it);
  	}

	int tid = 1; //bucket id
	//int countLeaf = 0;
	for(vector<BinarySplitNode*>::iterator it = leafNodeList.begin(); it != leafNodeList.end(); it++ ) {
		BinarySplitNode *tmp = *it;
		if (tmp->isLeaf) {
			// cout << ++countLeaf << SPACE << tmp->left << SPACE  << tmp->bottom 
			//    << SPACE << tmp->right << SPACE << tmp->top << SPACE << tmp->size << endl ;

			/* Composite key */
			cout << prefix_tile_id << BAR << tid 
				<< TAB << tmp->left << TAB << tmp->bottom << TAB
				<< tmp->right << TAB << tmp->top << endl;
			tid++;
		}
	}
}


bool readInput() {
	string input_line;

	while (cin && getline(cin, input_line)) {
		mystr.str(input_line);
		mystr.clear();

		SpatialObject *obj = new SpatialObject(0, 0, 0, 0);
		mystr >> prefix_tile_id >> obj->left >> obj->bottom >> obj->right >> obj->top;
		listAllObjects.push_back(obj);
		//    cerr << obj->left << TAB << obj->right << TAB << obj->top << TAB << obj->bottom << endl;

	}


	return true;
}


