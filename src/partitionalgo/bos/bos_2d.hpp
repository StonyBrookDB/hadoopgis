
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
#include <progparams/string_constants.h>
#include <utilities/Timer.hpp>
#include <utilities/tokenizer.h>
#include <boost/program_options.hpp>
#include <progparams/partition_params.hpp>

#ifdef DEBUGAREA
#include <utilities/tile_rectangle.h>
#endif

/* 
 * Boundary-Optimized-Strip Partitioner (BOS)
 * */

using namespace std;
using namespace SpatialIndex;
using namespace SpatialIndex::RTree;
namespace po = boost::program_options;


// function prototypes
bool read_input(struct partition_op & partop);
void insert(uint64_t id, Region* r);
void calculateSpatialUniverse();
float getCostX(uint32_t K);
float getCostY(uint32_t K);
Region split_x(uint32_t K, uint32_t & size );
Region split_y(uint32_t K, uint32_t & size );
void cleanup(std::vector<uint64_t> & spatial_objects);

extern void update_partop_info(struct partition_op & partop, string uppertileid, string newprefix);
extern void cleanup(struct partition_op & partop);
extern bool extract_params_partitioning(int argc, char** argv,
        struct partition_op & partop);

// Global variables
Region universe;
std::unordered_map<uint64_t,Region*> buffer;
string prefix_tile_id = "BOS";
vector<Data*> inMemData;
const uint32_t DIM_X = 0;
const uint32_t DIM_Y = 1;
uint64_t TotalEntries = 0;


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

