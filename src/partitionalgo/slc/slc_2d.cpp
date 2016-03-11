#include <partitionalgo/slc/slc_2d.hpp>
/*
 * Strip-based partitioning
 */


using namespace SpatialIndex;
using namespace SpatialIndex::RTree;
namespace po = boost::program_options;

// Process data in memory
void process_input(struct partition_op &partop) {
	#ifdef DEBUGTIME
	Timer t;       
	#endif
        uint64_t k = 0;
	for (vector<Region*>::iterator it = inMemData.begin(); it != inMemData.end(); it++)
	{
		//Region *obj = (*it)->clone();
		//insert((uint64_t)(*it)->m_id, *it);
		Region *obj = (*it)->clone();
		insert(k++, obj);
	}
	#ifdef DEBUGTIME
	double elapsed_time = t.elapsed();
	#endif

	// initilization 
	TotalEntries = buffer.size();
	universe =  *(buffer.begin()->second);
	calculateSpatialUniverse();
	// summary info 
	//
	#ifdef DEBUG
	cerr << "Spatial Universe: " << universe<< std::endl;
	cerr << "|spatial objects| = " << TotalEntries
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
	uint32_t pid = 1;

	while (true)
	{

		cost [0] = 0.0;
		cost [1] = 0.0;
		size = 0 ;

		if (TotalEntries <= partop.bucket_size) {

			pr = (orientation == DIM_X ) ?  split_x(partop.bucket_size, size)
							 : split_y(partop.bucket_size, size);

			// last partition
			tiles.push_back(new RTree::Data(0, 0 , pr, pid));
			pid++;
			break; 
		}

		pr = (orientation == DIM_X ) ?  split_x(partop.bucket_size,size) 
			: split_y(partop.bucket_size,size);

		tiles.push_back(new RTree::Data(0, 0 , pr, pid));
		pid++;
	} // end while
	#ifdef DEBUGTIME
	elapsed_time += t.elapsed();
	#endif

	k = 0;
	for (vector<RTree::Data*>::iterator it = tiles.begin() ; it != tiles.end(); ++it) {
		Region reg = (*it)->m_region;
		cout << partop.prefix_tile_id << k
			<< TAB << reg.m_pLow[0]
			<< TAB << reg.m_pLow[1]
			<< TAB << reg.m_pHigh[0]
			<< TAB << reg.m_pHigh[1] << endl;

		/*
		cerr << (*it)->m_region.m_pLow[0] 
			<< TAB << (*it)->m_region.m_pLow[1]
			<< TAB << (*it)->m_region.m_pHigh[0]
			<< TAB << (*it)->m_region.m_pHigh[1] << endl;
		*/
		k++;
	}


	//cleanup memory 
	
	for (vector<RTree::Data*>::iterator it = tiles.begin() ; it != tiles.end(); ++it) {
		delete *it;
	}

	for (vector<Region*>::iterator it = inMemData.begin(); it != inMemData.end(); it++) {
		delete *it; 
	}
	inMemData.clear();
	freeMemory();
	tiles.clear();
	xsorted_buffer.clear();
	ysorted_buffer.clear();
	buffer.clear();
		
}

// Customized reader for reading from a vector
class VecStreamReader : public IDataStream {
        public:
                VecStreamReader(vector<RTree::Data*> * coll) : m_pNext(0)
        {
                if (coll->size()==0)
                        throw Tools::IllegalArgumentException("Input vector is empty");

                sobjects = coll;
                index = 0 ;
                readNextEntry();
        }

                virtual ~VecStreamReader()
                {
                        if (m_pNext != 0) delete m_pNext;
                }

                virtual IData* getNext()
                {
                        if (m_pNext == 0) return 0;

                        RTree::Data* ret = m_pNext;
                        m_pNext = 0;
                        readNextEntry();
                        return ret;
                }

                virtual bool hasNext()
                {
                        return (m_pNext != 0);
                }

                virtual uint32_t size()
                {
                        throw Tools::NotSupportedException("Operation not supported.");
                }

                virtual void rewind()
                {
                        if (m_pNext != 0)
                        {
                                delete m_pNext;
                                m_pNext = 0;
                        }
                        index = 0;
                        readNextEntry();
                }

                void readNextEntry()
                {
                        if (index < sobjects->size())
                        {
                                m_pNext = (*sobjects)[index];
                                index++;
                        }
                }

                vector<RTree::Data*> * sobjects;
                vector<RTree::Data*>::size_type index ;
                RTree::Data* m_pNext;
};

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
		std::cerr<< "ERROR: item: " <<  id << " already exists in the map." 
			<< std::endl;// " with value " << (res.first)->second << endl;
	}

	// sorted by X Axsis
	std::pair<std::set<uint64_t>::iterator, bool> res_set = xsorted_buffer.insert(id);
	if ( ! res_set.second ) {
		std::cerr<< "ERROR: item: " <<  id 
		<< " already exists in the horizontal axis set." << std::endl;// " with value " << (res.first)->second << endl;
	}

	// sorted by Y Axsis
	res_set = ysorted_buffer.insert(id);
	if ( ! res_set.second ) {
		std::cerr<< "ERROR: item: " <<  id 
		<< " already exists in the vertical axis set." << std::endl;// " with value " << (res.first)->second << endl;
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

void freeMemory()
{
	// free obbjects 
	std::unordered_map<uint64_t, Region*>::iterator it ; 
	for (it = buffer.begin(); it != buffer.end(); ++it)
	{
		delete it->second;
	}

}

bool read_input(struct partition_op &partop) {
        string input_line;
        string prevtileid = "";
        string tile_id;
        vector<string> fields;
        partop.object_count = 0;

	RTree::Data *obj;
	id_type id = 0;
	double low[2];
	double high[2];

        while (cin && getline(cin, input_line) && !cin.eof()) {
                try {
                        tokenize(input_line, fields, TAB, true);
			istringstream ss(input_line);
			ss >> tile_id >> low[0] >> low[1] >> high[0] >> high[1] ;

                        if (prevtileid.compare(tile_id) != 0 && prevtileid.size() > 0) {
                                update_partop_info(partop, prevtileid, prevtileid  + prefix_tile_id);
                                process_input(partop);
                                partop.object_count = 0;
                        }
                        prevtileid = tile_id;
                        
			// Create objects
			Region *reg = new Region(low, high, 2);
			inMemData.push_back(reg);

			fields.clear();
			partop.object_count++;
		} catch (...) {
			#ifdef DEBUG
			cerr << "Error in parsing objects" << endl;
			#endif
			return false;
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
	cleanup(partop);
	return true;
}

int main(int argc, char** argv) {
	cout.precision(15);
	struct partition_op partop;
	if (!extract_params_partitioning(argc, argv, partop)) {
	#ifdef DEBUG
		cerr << "Fail to extract parameters" << endl;
	#endif
		return -1;
	}
	if (!read_input(partop)) {
		cerr << "Error reading input in" << endl;
		return -1;
	}
	cout.flush();
	return 0;
}

