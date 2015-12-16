#include <iostream>
#include <vector>

using namespace std;


class Rectangle {
	public:
		double low[2];
		double high[2];
		Rectangle(double x1, double y1, double x2, double y2) {
			low[0] = x1;
			low[1] = y1;
			high[0] = x2;
			high[1] = y2; 
		};
		void print(void) {
			cout << "Rect: " << low[0] << " " << low[1] << " " << high[0] << " " << high[1] << " " <<endl;
		};
};

// return the iterator of rec in list
vector<Rectangle *>::iterator bin_search(vector<Rectangle *> &list, int begin, int end, Rectangle *rec) {
	int mid = (begin + end)/2;
	if (list[mid]->low[1] == rec->low[1]) {
		if (list[mid]->high[1] == rec->high[1])
			return list.begin() + mid;
		else if (list[mid]->high[1] < rec->high[1]) {
			if (mid == end)
				return list.begin() + mid+1;
			return bin_search(list,mid+1,mid,rec);
		}
		else {
			if (mid == begin)
				return list.begin()+mid;
			return bin_search(list,begin,mid-1,rec);
		}
	}
	else if (list[mid]->low[1] < rec->low[1]) {
		if (mid == end) {
			return list.begin() + mid+1;
		}
		return bin_search(list, mid+1, end, rec);
	}
	else {
		if (mid == begin) {
			return list.begin() + mid;
		}
		return bin_search(list, begin, mid-1, rec);
	}
}

// add rect to rects
void add_rec(Rectangle *rect, vector<Rectangle *> &rects) {
	if (rects.size() == 0) {
		rects.push_back(rect);
	}
	else {
		vector<Rectangle *>::iterator it = bin_search(rects, 0, rects.size()-1, rect);
		rects.insert(it, rect);
	}
}

// remove rec from rets
void remove_rec(Rectangle *rect, vector<Rectangle *> &rects) {
	vector<Rectangle *>::iterator it = bin_search(rects, 0, rects.size()-1, rect);
	rects.erase(it);
}

// calculate the total vertical length covered by rectangles in the active set
double vert_dist(vector<Rectangle *> as) {
	int n = as.size();
	double totallength = 0;
	double start, end;

	int i = 0;
	while (i < n) {
		start = as[i]->low[1];
		end = as[i]->high[1];
		while (i < n && as[i]->low[1] <= end) {
			if (as[i]->high[1] > end) {
				end = as[i]->high[1];
			}
			i++;
		}
		totallength += end - start;
	}
	return totallength;
}

bool mycomp1(Rectangle* a, Rectangle* b) {
	return (a->low[0] < b->low[0]);
}

bool mycomp2(Rectangle* a, Rectangle* b) {
	return (a->high[0] < b->high[0]);
}

double findarea(vector<Rectangle *> &rects) {
	vector<Rectangle *> start = rects;
	vector<Rectangle *> end = rects;
	sort(start.begin(), start.end(), mycomp1);
	sort(end.begin(), end.end(), mycomp2);

	// active set
	vector<Rectangle *> as;

	int n = rects.size();

	double totalarea = 0;
	double current = start[0]->low[0];
	double next;
	int i = 0, j = 0;
	// big loop
	while (j < n) {
	//	cout << "loop---------------"<<endl;
		// add all recs that start at current
		while (i < n && start[i]->low[0] == current) {
	//		cout << "add" <<endl;
			// add start[i] to AS
			add_rec(start[i], as);
	//		cout << "after" <<endl;
			i++;
		}
		// remove all recs that end at current
		while (j < n && end[j]->high[0] == current) {
	//		cout << "remove" <<endl;
			// remove end[j] from AS
			remove_rec(end[j], as);
	//		cout << "after" <<endl;
			j++;
		}

		// find next event x
		if (i < n && j < n) {
			if (start[i]->low[0] <= end[j]->high[0]) {
				next = start[i]->low[0];
			}
			else {
				next = end[j]->high[0];
			}
		}
		else if (j < n) {
			next = end[j]->high[0];
		}

		// distance to next event
		double horiz = next - current;

		// figure out vertical dist
		double vert = vert_dist(as);

		totalarea += vert * horiz;

		current = next;
	}
	return totalarea;
}
