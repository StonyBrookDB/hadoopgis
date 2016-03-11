#include <cstdlib>

using namespace std;
struct Time_interval {
	Time_interval(long _start, long _end) : start(_start), end(_end) {} 
	Time_interval(long _value) : start(_value), end(_value) {} 
	long start;
	long end;
};

class TemporalObject {
	public:
		vector<struct Time_interval> elements;
		TemporalObject();
		TemporalObject(string date, int supportedformat);
		~TemporalObject();
		int getNumIntervals();
		long getStart();
		long getEnd();
		bool intersects(TemporalObject *obj);
		bool contains(TemporalObject *obj);
		long mindist(TemporalObject *obj);
		
};

