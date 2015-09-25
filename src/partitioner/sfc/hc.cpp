#include <iostream>
#include <sstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <getopt.h>
#include <time.h>
#include <string>
#include <cmath>
#include "../../common/string_constants.h"
#include "../../common/Timer.hpp"
#include <boost/program_options.hpp>
#include <spatialindex/SpatialIndex.h>

/* 
 * Hilbert curve partitioning
 * */

using namespace std;
using namespace SpatialIndex;
using namespace SpatialIndex::RTree;
namespace po = boost::program_options;

int FILLING_CURVE_PRECISION = 0; // precision 

/**
 * Computes the hilbert value for two given integers.
 * The number of bits of x and y to be considered can be determined by setting the parameter mask appropriately.
 * @param x the value of the first dimension
 * @param y the value of the second dimension
 * @param mask the bitmask containing exactly the highest bit to be considered.
 *  	If all bits of x and y should be taken into account, choose the value 1<<31.
 * @return the hilbert value for x and y
 */
long hilbert2d (int x, int y, unsigned int mask) {
	long hilbert = 0;
	int not_y = ~(y ^= x);

	do
		if ((y&mask)!=0)
			if ((x&mask)==0)
				hilbert = (hilbert<<2)|1;
			else {
				x ^= not_y;
				hilbert = (hilbert<<2)|3;
			}
		else
			if ((x&mask)==0) {
				x ^= y;
				hilbert <<= 2;
			}
			else
				hilbert = (hilbert<<2)|2;
	while ((mask >>= 1)!=0);
	return hilbert;
}

/**
 * Computes the hilbert value for two given integers.
 * @param x the value of the first dimension
 * @param y the value of the second dimension
 * @return the hilbert value for x and y
 */
long hilbert2d (int x, int y) {
	return hilbert2d(x, y, 1<<31);
}

long getHilbertValue(double obj []) {
	double x = (obj[0] + obj[2])/2;
	double y = (obj[1] + obj[3])/2;
	return hilbert2d((int) (x*FILLING_CURVE_PRECISION),(int) (y*FILLING_CURVE_PRECISION));
}

struct HilberCurveSorter {
	bool operator() (double i [] , double j [] ) { 

		return (getHilbertValue(i)< getHilbertValue(j));
	}
} hcsorter;

void printShape(double obj []){
	string SPACE = " "; 
	long hcval = getHilbertValue(obj);
	cout << obj[0] << SPACE << obj[1] << SPACE << obj[2] << SPACE << obj[3] << SPACE << hcval <<endl;
}

int main(int ac, char* av[]) {
	cout.precision(15);

	uint32_t bucket_size ;
	int prec;

	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "this help message")
			("prec,p", po::value<int>(&prec)->default_value(20), "Hilbert Curve Precision")
			("bucket,b", po::value<uint32_t>(&bucket_size), "Expected bucket size");

		po::variables_map vm;        
		po::store(po::parse_command_line(ac, av, desc), vm);
		po::notify(vm);    

		if ( vm.count("help") || (! vm.count("bucket"))  || (!vm.count("prec")) ) {
			cerr << desc << endl;
			return 0; 
		}

		#ifdef DEBUG
		cerr << "Precision: "<< prec <<endl;
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

	// assign precision 
	FILLING_CURVE_PRECISION = 1 << prec;

	// read objects into memory 
	string spaceid = ""; // Prefix for the partition ID
	vector<double*> socoll;  // spatial object collection 
	string input_line;

	while (cin && getline(cin, input_line))
	{
		istringstream ss(input_line);
		double * obj = new double [4]; 
		ss >> spaceid >> obj[0] >> obj[1] >> obj[2] >> obj[3] ;
			socoll.push_back(obj);
		#ifdef DEBUG
		printShape(obj);
		#endif
	}

	#ifdef DEBUG
	cerr << "start sorting." << endl;
	#endif
	
	#ifdef DEBUGTIME
	Timer t;        
	#endif                 

	// sort object based on Hilbert Curve value
	std::sort (socoll.begin(), socoll.end(), hcsorter); 

	#ifdef DEBUG
	// cerr << "sort ------ done. " << endl;
	#endif

	// partition based on hilbert curve
	double low[2], high[2];
	//vector<RTree::Data*> tiles;
	id_type tid = 1;
	vector<double*>::size_type len = socoll.size();
	for (vector<double*>::size_type i = 0; i < len; i+=bucket_size)
	{
		low[0] = std::numeric_limits<double>::max();
		low[1] = std::numeric_limits<double>::max();
		high[0] = 0.0 ;
		high[1] = 0.0 ;
		for (int j = 0; j < bucket_size-1 && i+j < len; j++)
		{
			//cerr << "i+j = " << i+j << endl;
			double *obj=  socoll[i+j] ;
			if (obj[0] < low[0])  low[0] = obj[0];
			if (obj[1] < low[1])  low[1] = obj[1];
			if (obj[2] > high[0]) high[0] = obj[2];
			if (obj[3] > high[1]) high[1] = obj[3];
		}

		// Actual tile id is "spaceid|tid"
		cout << spaceid  << BAR << tid << TAB << low[0] << TAB << low[1] << TAB 
			<< high[0] << TAB << high[1] << endl;
		//cerr << "tid: " << tid << "; " << "i: " << i<< endl;
		tid++;
	}
	
	// cleanup allocated memory 
	for (vector<double*>::iterator it = socoll.begin() ; it != socoll.end(); ++it) {
		delete [] *it;
	}
	cout.flush();

	return 0;

}

