#include <iostream>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <vector>
#include <cstdlib> 
#include <unordered_map>
#include <boost/program_options.hpp>
#include <spatialindex/SpatialIndex.h>
#include <boost/program_options.hpp>
#include <utilities/Timer.hpp>

#include <progparams/string_constants.h>
#include <utilities/tokenizer.h>

#include <progparams/partition_params.hpp>

#ifdef DEBUGAREA
#include <utilities/tile_rectangle.h>
#endif

const uint32_t DIM_X = 0;
const uint32_t DIM_Y = 1;
uint64_t TotalEntries = 0;

vector<SpatialIndex::RTree::Data*> tiles;

SpatialIndex::Region universe;

unordered_map<uint64_t,SpatialIndex::Region*> buffer;

struct SortRegionAscendingX : public std::binary_function<const uint64_t, const uint64_t, bool>
{
	bool operator()(const uint64_t &idx1, const uint64_t &idx2) const 
	{
		if (buffer[idx1]->m_pHigh[0] + buffer[idx1]->m_pLow[0] 
			< buffer[idx2]->m_pHigh[0] + buffer[idx2]->m_pLow[0])
			return true;
		else if (buffer[idx1]->m_pHigh[0] + buffer[idx1]->m_pLow[0] 
			> buffer[idx2]->m_pHigh[0] + buffer[idx2]->m_pLow[0])
			return false;
		else 
			return idx1 < idx2;
	}
};

struct SortRegionAscendingY : public std::binary_function<const uint64_t, const uint64_t, bool>
{
	bool operator()(const uint64_t &idx1, const uint64_t &idx2) const 
	{
		if (buffer[idx1]->m_pHigh[1] + buffer[idx1]->m_pLow[1] < 
			buffer[idx2]->m_pHigh[1] + buffer[idx2]->m_pLow[1])
			return true;
		else if (buffer[idx1]->m_pHigh[1] + buffer[idx1]->m_pLow[1] 
			> buffer[idx2]->m_pHigh[1] + buffer[idx2]->m_pLow[1])
			return false;
		else 
			return idx1 < idx2;
	}
};

std::set<uint64_t, SortRegionAscendingX> xsorted_buffer;
std::set<uint64_t, SortRegionAscendingY> ysorted_buffer;

// functions 
void insert(uint64_t id, SpatialIndex::Region* r);
void calculateSpatialUniverse();
float getCostX(uint32_t K);
float getCostY(uint32_t K);
SpatialIndex::Region split_x(uint32_t K, uint32_t & size );
SpatialIndex::Region split_y(uint32_t K, uint32_t & size );
void cleanup(std::vector<uint64_t> & spatial_objects);
void freeMemory();
bool read_input(struct partition_op & partop);
void generatePartitions(struct partition_op & partop);
void process_input(struct partition_op &partop);

string prefix_tile_id = "SLC";

// Hidden parameter
int orientation = DIM_X; // partition orientation [0 for x axis, 1 for y axis]

vector<SpatialIndex::Region*> inMemData;

