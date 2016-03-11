#include <cmath>
#include <algorithm>
#include <iostream>
#include <sstream>

// Extern variables
extern int bucket_size;
extern vector<QuadtreeNode*> leafNodeList;

SpatialObject::SpatialObject(double min_x, double min_y, 
	double max_x, double max_y) {
	low[0] = min_x;
	low[1] = min_y;
	high[0] = max_x;
	high[1] = max_y;
}

QuadtreeNode::QuadtreeNode(double min_x, double min_y,
		double max_x, double max_y, int _level) {
	low[0] = min_x;
	low[1] = min_y;
	high[0] = max_x;
	high[1] = max_y,
	level = _level;
	isLeaf = true;
	canBeSplit = true;
	size = 0;
		//    leafNodeList.push_back(this);
}

QuadtreeNode::~QuadtreeNode() {
	//objectList.clear();
}

bool QuadtreeNode::addObjectIgnore(SpatialObject *object) {
	size++;
	objectList.push_back(object);
	/*
	if (size > bucket_size) {
	   canBeSplit = false;
	}
	*/
	return true;
}

bool QuadtreeNode::addObject(SpatialObject *object) {
	size++;
	if (isLeaf) {
		/* Update the center */
		objectList.push_back(object);
		/*
		   if (size > bucket_size + 1) {
		   cerr << "WHY: Level " << level << endl;
		   }
		   */

		/* Temporary variables */

		if (size > bucket_size && canBeSplit) {
			//if (level >= GLOBAL_MAX_LEVEL) {
			//canBeSplit = false;
			//return false;
			//}
			#ifdef DEBUG
			cerr << "Splitting" << endl;
			#endif
			double mid[2];
			for (int i = 0; i < 2; i++) {
				mid[i] = (low[i] + high[i]) / 2;
			}
			children[0] = new QuadtreeNode(low[0], low[1], mid[0], mid[1], level + 1);
			children[1] = new QuadtreeNode(mid[0], low[1], high[0], mid[1], level + 1);
			children[2] = new QuadtreeNode(mid[0], mid[1], high[0], high[1], level + 1);
			children[3] = new QuadtreeNode(low[0], mid[1], mid[0], high[1], level + 1);
			
			isLeaf = false;
			/* Split the node */

			for(vector<SpatialObject*>::iterator it = objectList.begin(); 
					it != objectList.end(); it++ ) {
				/* Overflow cases -> reached max level of recursion */
				for (int i = 0; i < 4; i++) {
					if (children[i]->intersects(*it)) {
						children[i]->addObjectIgnore(*it);
					}
				}
			}

	//		bool cannotBeSplit = false;
			int totalChildrenCount = 0;
			for (int i = 0; i < 4; i++) {
				#ifdef DEBUG
				cerr << "Sizes of child " << i << " " << children[i]->size << endl;
				#endif
				//cannotBeSplit = cannotBeSplit || !children[i]->canBeSplit;
				totalChildrenCount += children[i]->size;
			}

			if (totalChildrenCount >= 4 * (size - 1)) {
				isLeaf = true;
				canBeSplit = false;
				#ifdef DEBUG
				cerr << "Fail in finding good split point" << endl;
				#endif
				for (int i = 0; i < 4; i++) {
					delete children[i];
				}
			} else {
				#ifdef DEBUG
				cerr << "Succeed in splitting";
				#endif
				for (int i = 0; i < 4; i++) {
					leafNodeList.push_back(children[i]);
				}
				objectList.clear();
			}
		} else if (size > 1.5 * bucket_size) {
			canBeSplit = true;
		}
		/*
		   if (!isLeaf) {
		   objectList.clear();
		   }*/

	} else {
		for (int i = 0; i < 4; i++) {
			if (children[i]->intersects(object)) {
				children[i]->addObject(object);
			}
		}
	}
	return true;
}


/* Check if the node MBB intersects with the object MBB */
bool QuadtreeNode::intersects(SpatialObject *object) {
	return !(object->low[0] > high[0] || object->high[0] < low[0]
		|| object->low[1] > high[1] || object->high[1] < low[1] );
}
