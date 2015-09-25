#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <unordered_map>
#include <vector>
#include <spatialindex/SpatialIndex.h>
#include "../../common/string_constants.h"
#include "../../common/Timer.hpp"
#include <boost/program_options.hpp>

/* 
 * Boundary-Optimized-Strip Partitioner (BOS)
 *
 * */

using namespace std;
using namespace SpatialIndex;
using namespace SpatialIndex::RTree;
namespace po = boost::program_options;

const uint32_t DIM_X = 0;
const uint32_t DIM_Y = 1;
uint64_t TotalEntries = 0;

Region universe;
std::unordered_map<uint64_t,Region*> buffer;

struct SortRegionAscendingX : public std::binary_function<const uint64_t, const uint64_t, bool>
{
	bool operator()(const uint64_t &idx1, const uint64_t &idx2) const {
		if (buffer[idx1]->m_pHigh[0] + buffer[idx1]->m_pLow[0] < buffer[idx2]->m_pHigh[0] + buffer[idx2]->m_pLow[0])
			return true;
		else if (buffer[idx1]->m_pHigh[0] + buffer[idx1]->m_pLow[0] > buffer[idx2]->m_pHigh[0] + buffer[idx2]->m_pLow[0])
			return false;
		else 
			return idx1 < idx2;
	}
};

struct SortRegionAscendingY : public std::binary_function<const uint64_t, const uint64_t, bool>
{
	bool operator()(const uint64_t &idx1, const uint64_t &idx2) const 
	{
		if (buffer[idx1]->m_pHigh[1] + buffer[idx1]->m_pLow[1] < buffer[idx2]->m_pHigh[1] + buffer[idx2]->m_pLow[1])
			return true;
		else if (buffer[idx1]->m_pHigh[1] + buffer[idx1]->m_pLow[1] > buffer[idx2]->m_pHigh[1] + buffer[idx2]->m_pLow[1])
			return false;
		else 
			return idx1 < idx2;
	}
};

std::set<uint64_t, SortRegionAscendingX> xsorted_buffer;
std::set<uint64_t, SortRegionAscendingY> ysorted_buffer;


// functions 
void insert(uint64_t id, Region* r);
void calculateSpatialUniverse();
float getCostX(uint32_t K);
float getCostY(uint32_t K);
Region split_x(uint32_t K, uint32_t & size );
Region split_y(uint32_t K, uint32_t & size );
void cleanup(std::vector<uint64_t> & spatial_objects);

// main method

int main(int ac, char** av)
{
	cout.precision(15);

	uint32_t bucket_size ;
	string inputPath;

	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "this help message")
			("bucket,b", po::value<uint32_t>(&bucket_size), "Expected bucket size");

		po::variables_map vm;        
		po::store(po::parse_command_line(ac, av, desc), vm);
		po::notify(vm);    

		if ( vm.count("help") || (! vm.count("bucket")) ) {
			cerr << desc << endl;
			return 0; 
		}
		#ifdef DEBUG
		cerr << "Bucket size: "<<bucket_size << endl;
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

	uint64_t cc = 0; 
	vector<Data*> inMemData;
	string input_line;
	double low[2], high[2];
	string str_id;
	id_type id = 0;

	while (cin && getline(cin, input_line))
	{
		istringstream ss(input_line);
		ss >> str_id >> low[0] >> low[1] >> high[0] >> high[1] ;

		Region r(low, high, 2);
		Data* d = new RTree::Data(0, 0 , r, id);// store a zero size null poiter.
		id++;
		if (d == 0)
			throw Tools::IllegalArgumentException(
					"bulkLoadUsingRPLUS: RTree bulk load expects SpatialIndex::RTree::Data entries."
					);
		inMemData.push_back(d);
		cc++; 
		#ifdef DEBUG
		if (cc % 100000 == 0)
			std::cerr << "number of records sorted: " << cc << std::endl;
		#endif
	}

	#ifdef DEBUGTIME
	Timer t;
	#endif 

	uint64_t countid = 1;
	for (vector<Data*>::iterator it = inMemData.begin(); it != inMemData.end(); it++)
	{
		Region *obj = (*it)->m_region.clone();
		insert(countid++,obj);
	}
	#ifdef DEBUGTIME
	double elapsed_time = t.elapsed();
	#endif 

	// initilization 
	TotalEntries = buffer.size();
	universe =  *(buffer.begin()->second);
	calculateSpatialUniverse();

	// summary info
	#ifdef DEBUG 
	std::cerr << "Spatial Universe: " << universe<< std::endl;
	std::cerr << "|spatial objects| = " << TotalEntries
		<< ", |X| = " << xsorted_buffer.size() 
		<< ", |Y| = " << ysorted_buffer.size() << std::endl;
	std::cerr << "RTree::BulkLoader::R+ packing objects .." << std::endl;
	#endif


	#ifdef DEBUGTIME
	t.restart(); // reset timer 
	#endif

	// loop vars 
	double cost [] = {0.0, 0.0};
	uint64_t iteration = 0; 
	Region pr(universe);
	uint32_t size = 0;
	id_type pid = 1;
	vector<RTree::Data*> tiles;

	while (true)
	{

		cost [0] = 0.0;
		cost [1] = 0.0;
		size = 0 ;
		iteration++;

		if (TotalEntries <= bucket_size) {

			pr = split_x(bucket_size,size);

			// last partition
			/*
			   std::cerr << "Iteration: " << iteration << 
			   "\tx-cost = " << cost [DIM_X] << 
			   "\ty-cost = " << cost [DIM_Y] << 
			   "\tRegion = " << pr << std::endl;
			   std::cout << ++pid << " " << pr << endl;
			   std::cerr << "|objetcs| = " << TotalEntries<< " , |partition| = " << size << std::endl;
			   */
			cout << pid << TAB << pr.m_pLow[0] << TAB << pr.m_pLow[1]
				<< TAB << pr.m_pHigh[0] << TAB << pr.m_pHigh[1] << endl;
			tiles.push_back(new RTree::Data(0, 0 , pr, pid++));
			break; 
		}

		cost[DIM_X] = getCostX(bucket_size);
		cost[DIM_Y] = getCostY(bucket_size);

		pr = (cost[DIM_X] < cost[DIM_Y] )?  split_x(bucket_size,size) : split_y(bucket_size,size);

		// program state 
		/* std::cerr << "Iteration: " << iteration << 
		   "\tx-cost = " << cost [DIM_X] << 
		   "\ty-cost = " << cost [DIM_Y] << 
		   "\tRegion = " << pr << std::endl;
		   std::cout << ++pid << " " << pr << endl;
		   */
		cout << pid << TAB << pr.m_pLow[0] << TAB << pr.m_pLow[1]
			<< TAB << pr.m_pHigh[0] << TAB << pr.m_pHigh[1] << endl;
		tiles.push_back(new RTree::Data(0, 0 , pr, pid++));
		//std::cerr << "|objetcs| = " << TotalEntries<< " , |partition| = " << size << std::endl;
	} // end while

	#ifdef DEBUGTIME
	elapsed_time += t.elapsed();

	cerr << "stat:ptime," << bucket_size << "," << tiles.size() <<"," << elapsed_time << endl;

	#endif
	//cleanup memory 
	for (vector<RTree::Data*>::iterator it = tiles.begin() ; it != tiles.end(); ++it) 
		delete *it;

	for (vector<Data*>::iterator it = inMemData.begin(); it != inMemData.end(); it++)
		delete *it; 

	cout.flush();
	cerr.flush();
	return 0 ;
}

// function implementations 

void calculateSpatialUniverse(){
	for ( std::unordered_map<uint64_t,Region*>::iterator iter = buffer.begin(); iter != buffer.end(); ++iter )
		universe.combineRegion(*(iter->second));
}


void insert(uint64_t id, Region* r)
{
	// check if id is in the collection and report error if duplicates
	std::pair<std::unordered_map<uint64_t, Region*>::iterator, bool> res = buffer.insert(std::make_pair(id,r));
	if ( ! res.second ) {
		std::cerr<< "ERROR: item: " <<  id << " already exists in the map." << std::endl;// " with value " << (res.first)->second << endl;
	}

	// sorted by X Axsis
	std::pair<std::set<uint64_t>::iterator, bool> res_set = xsorted_buffer.insert(id);
	if ( ! res_set.second ) {
		std::cerr<< "ERROR: item: " <<  id << " already exists in the horizontal axis set." << std::endl;// " with value " << (res.first)->second << endl;
	}

	// sorted by Y Axsis
	res_set = ysorted_buffer.insert(id);
	if ( ! res_set.second ) {
		std::cerr<< "ERROR: item: " <<  id << " already exists in the vertical axis set." << std::endl;// " with value " << (res.first)->second << endl;
	}
	TotalEntries++;
}


float getCostX(uint32_t K){
	float cost = 0.0; 

	std::set<uint64_t,SortRegionAscendingX>::iterator iter = xsorted_buffer.begin();
	std::advance(iter,K-1);

	Region * region_k = buffer[*iter];
	double c1 [] = {0.0, 0.0};
	double c2 [] = {0.0, 0.0};

	c1[DIM_X] = region_k->getHigh(DIM_X);
	c1[DIM_Y] = universe.getLow(DIM_Y);
	c2[DIM_X] = c1[DIM_X];
	c2[DIM_Y] = universe.getHigh(DIM_Y);
	const LineSegment lseg(c1,c2,universe.getDimension());

	// iterate on smaller elements
	for ( ; iter != xsorted_buffer.begin(); --iter)
	{
		if (buffer[*iter]->intersectsLineSegment(lseg))
			cost += 1.0 ;
		else 
			break;
	}
	if (iter == xsorted_buffer.begin()) cost += 1.0; 

	// iterate right side 
	iter = xsorted_buffer.begin();
	for (std::advance(iter,K); iter != xsorted_buffer.end(); ++iter)
	{
		if (buffer[*iter]->intersectsLineSegment(lseg))
			cost += 1.0 ;
		else 
			break;
	}

	return cost;
}

float getCostY(uint32_t K){
	float cost = 0.0; 

	std::set<uint64_t,SortRegionAscendingY>::iterator iter = ysorted_buffer.begin();
	std::advance(iter,K-1);

	Region * region_k = buffer[*iter];
	double c1 [] = {0.0, 0.0};
	double c2 [] = {0.0, 0.0};

	c1[DIM_Y] = region_k->getHigh(DIM_Y);
	c1[DIM_X] = universe.getLow(DIM_X);
	c2[DIM_Y] = c1[DIM_Y];
	c2[DIM_X] = universe.getHigh(DIM_X);
	const LineSegment lseg(c1,c2,universe.getDimension());

	// iterate on smaller elements
	for ( ; iter != ysorted_buffer.begin(); --iter)
	{
		if (buffer[*iter]->intersectsLineSegment(lseg))
			cost += 1.0 ;
		else 
			break;
	}
	if (iter == ysorted_buffer.begin()) cost += 1.0; 

	// iterate right side 
	iter = ysorted_buffer.begin();
	for (std::advance(iter,K); iter != ysorted_buffer.end(); ++iter)
	{
		if (buffer[*iter]->intersectsLineSegment(lseg))
			cost += 1.0 ;
		else 
			break;
	}

	return cost;
}

Region split_x(uint32_t K, uint32_t & size )
{
	double c1 [] = {0.0, 0.0};
	double c2 [] = {0.0, 0.0};
	uint32_t dim = DIM_X ; 
	uint32_t adim = DIM_Y ; // another dimension 
	Region p;
	p.makeDimension(2);

	vector<uint64_t> spatial_objects; // object ids which forms a partition

	std::set<uint64_t,SortRegionAscendingX>::iterator iter = xsorted_buffer.begin();

	if ( TotalEntries > K )
	{
		std::advance(iter,K-1);
		Region * region_k = buffer[*iter];
		c1[dim] = region_k->getHigh(dim);
		c1[adim] = universe.getLow(adim);
		c2[dim] = c1[dim];
		c2[adim] = universe.getHigh(adim);

		memcpy(p.m_pLow, universe.m_pLow,   2 * sizeof(double));
		memcpy(p.m_pHigh, c2, 2 * sizeof(double));

		memcpy(universe.m_pLow, c1, 2 * sizeof(double));

	}
	else // if (getTotalEntries() <= K)
	{
		p = universe; 
	}

	// create list of items to clean
	uint32_t count = 0 ; 
	for (iter = xsorted_buffer.begin(); iter != xsorted_buffer.end() && count++ < K  ; ++iter)
		spatial_objects.push_back(*iter);

	// update sorted vectors & free processed objects
	size = spatial_objects.size();
	cleanup(spatial_objects);
	// return the partition
	return p;
}
Region split_y(uint32_t K, uint32_t & size )
{
	double c1 [] = {0.0, 0.0};
	double c2 [] = {0.0, 0.0};
	uint32_t dim = DIM_Y ; 
	uint32_t adim = DIM_X ; // another dimension 
	Region p;
	p.makeDimension(2);

	vector<uint64_t> spatial_objects; // object ids which forms a partition

	std::set<uint64_t,SortRegionAscendingY>::iterator iter = ysorted_buffer.begin();

	if ( TotalEntries > K )
	{
		std::advance(iter,K-1);
		Region * region_k = buffer[*iter];
		c1[dim] = region_k->getHigh(dim);
		c1[adim] = universe.getLow(adim);
		c2[dim] = c1[dim];
		c2[adim] = universe.getHigh(adim);

		memcpy(p.m_pLow, universe.m_pLow, 2 * sizeof(double));
		memcpy(p.m_pHigh, c2, 2 * sizeof(double));

		memcpy(universe.m_pLow, c1, 2 * sizeof(double));

	}
	else // if (getTotalEntries() <= K)
	{
		p = universe; 
	}

	// create list of items to clean
	uint32_t count = 0 ; 
	for (iter = ysorted_buffer.begin(); iter != ysorted_buffer.end() && count++ < K  ; ++iter)
		spatial_objects.push_back(*iter);

	// update sorted vectors & free processed objects
	size = spatial_objects.size();
	cleanup(spatial_objects);

	// return the partition
	return p;
}

void cleanup(std::vector<uint64_t> & spatial_objects)
{
	for (std::vector<uint64_t>::iterator it = spatial_objects.begin(); it != spatial_objects.end(); ++it)
	{
		xsorted_buffer.erase(*it);
		ysorted_buffer.erase(*it);
		delete buffer[*it] ;
		buffer.erase(*it);
	}
	TotalEntries = buffer.size();

}


