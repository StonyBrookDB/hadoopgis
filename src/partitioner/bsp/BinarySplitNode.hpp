#include <cmath>
#include <algorithm>
#include <iostream>
#include <sstream>

// Extern variables
extern int bucket_size;
extern vector<BinarySplitNode*> leafNodeList;

/* Temporary variables */
int tempCount;
double averageX;
double averageY;
double xCenter;
double yCenter;
double stdevX;
double stdevY;


SpatialObject::SpatialObject(double _left, double _right, double _top, double _bottom) :
	left(_left),
	right(_right),
	top(_top),
	bottom(_bottom) {

	}

BinarySplitNode::BinarySplitNode(double _left, double _right,
		double _top, double _bottom, int _level) :
	left(_left),
	right(_right),
	top(_top),
	bottom(_bottom),
	level(_level) {
		isLeaf = true;
		canBeSplit = true;
		size = 0;
		//    leafNodeList.push_back(this);
	}


BinarySplitNode::~BinarySplitNode() {
}

bool BinarySplitNode::addObjectIgnore(SpatialObject *object) {
	size++;
	objectList.push_back(object);

	if (size > bucket_size) {
	   canBeSplit = false;
	}
	return true;
}

bool BinarySplitNode::addObject(SpatialObject *object) {
	size++;

	if (isLeaf) {
		/* Update the center */
		objectList.push_back(object);
		/*
		   if (size > bucket_size + 1) {
		   cerr << "WHY: Level " << level << endl;
		   }
		   */

		if (size > bucket_size && canBeSplit) {
			//if (level >= GLOBAL_MAX_LEVEL) {
				//canBeSplit = false;
				//return false;
			//}
 //                       cerr << "Splitting" << endl;
			double *xList = new double[size];
			double *yList = new double[size];

			tempCount = 0;
			for(vector<SpatialObject *>::iterator iter = objectList.begin(); 
					iter != objectList.end(); iter++ ) {
				xList[tempCount] = ((*iter)->left + (*iter)->right) / 2;

				yList[tempCount] = ((*iter)->top + (*iter)->bottom) / 2;
				tempCount++;
			}

			averageX = 0;
			averageY = 0;

			for (int i = 0; i < size; i++) {
				averageX += xList[i];
				averageY += yList[i];
			}
			averageX /= (double) size;
			averageY /= (double) size;

			stdevX = 0;
			stdevY = 0;
			for (int i = 0; i < size; i++) {
				stdevX += pow(xList[i] - averageX, 2);
				stdevY += pow(yList[i] - averageY, 2);
			}

			sort(xList, xList + size);
			sort(yList, yList + size);

			xCenter = size % 2 == 0 ? (xList[size / 2] + xList[size / 2 - 1]) / 2
				: xList[size / 2];

			yCenter = size % 2 == 0 ? (yList[size / 2] + yList[size / 2 - 1]) / 2
				: yList[size / 2];




			/* Choose which direction to split */
			if (xCenter <= left) {
				xCenter = (FACTOR * left + right) / (FACTOR + 1);
			}
			if (xCenter >= right) {
				xCenter = (left + FACTOR * right) / (FACTOR + 1);
			}

			if (yCenter <= bottom) {
				yCenter = (FACTOR * bottom + top) / (FACTOR + 1);
			}

			if (yCenter >= top) {
				yCenter = (bottom + FACTOR * top) / (FACTOR + 1);
			}

			//xCenter = (left +right ) /2;
			//yCenter = (top + bottom) / 2;

			delete [] xList;
			delete [] yList;
			if (stdevX > stdevY) {
				/* Split according to y */
				first = new BinarySplitNode(left, xCenter, top, bottom, level + 1);
				second = new BinarySplitNode(xCenter, right, top, bottom, level + 1);
			} else {
				/* Split according to x */
				first = new BinarySplitNode(left, right, yCenter, bottom, level + 1);
				second = new BinarySplitNode(left, right, top, yCenter, level + 1);
			}

			isLeaf = false;
			/* Split the node */

			for(vector<SpatialObject*>::iterator it = objectList.begin(); it != objectList.end(); it++ ) {
				/* Overflow cases -> reached max level of recursion */
				if (first->intersects(*it)) {
					//first->addObject(*it);
					first->addObjectIgnore(*it);	
					/* if (!first->addObject(*it)) {
					   canBeSplit = false;
					   isLeaf = true;
					   return false;
					   }*/
				}
				if (second->intersects(*it)) {
					//second->addObject(*it);
					second->addObjectIgnore(*it);
					/*if (!second->addObject(*it)) {
					  canBeSplit = false;
					  isLeaf = true;
					  return false;
					  }*/
				}

			}

//			cerr << "Sizes: " << first->size << " " << second->size << endl;


			if (!first->canBeSplit && !second->canBeSplit) {
				isLeaf = true;
				canBeSplit = false;
//				cerr << "Fail in finding good split point" << endl;
				delete first;
				delete second;
			} else {
//				cerr << "Succeed in splitting";
				leafNodeList.push_back(first);
				leafNodeList.push_back(second);
				objectList.clear();
			}	  
		}
		/*
		   if (!isLeaf) {
		   objectList.clear();
		   }*/

	} else {

		if (first->intersects(object)) {
			first->addObject(object);
		}
		if (second->intersects(object)) {
			second->addObject(object);
		}
	}
	return true;
}


/* Check if the node MBB intersects with the object MBB */
bool BinarySplitNode::intersects(SpatialObject *object) {
    return !(object->left > right || object->right < left
            || object->top < bottom || object->bottom > top);
}
