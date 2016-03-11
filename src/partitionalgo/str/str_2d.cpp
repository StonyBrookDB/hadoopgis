#include <partitionalgo/str/str_2d.hpp>

/* Sort-tile recursive (STR) partitioning method */

// using namespace boost;
namespace po = boost::program_options;

using namespace std;
using namespace SpatialIndex;


class MyQueryStrategy : public SpatialIndex::IQueryStrategy
{
	private:
		queue<id_type> ids;
		struct partition_op *partop_ptr;
		id_type bid;
	public:
		MyQueryStrategy(struct partition_op *ptr) {
			partop_ptr = ptr;
			bid = 0;
		}

		void getNextEntry(const IEntry& entry, id_type& nextEntry, bool& hasNext)
		{
			const INode* n = dynamic_cast<const INode*>(&entry);

			// traverse only index nodes at levels 0 and higher.
			if (n != NULL) {
				if (n->getLevel() > 0)
				{
					for (uint32_t cChild = 0; cChild < n->getChildrenCount(); cChild++)
					{
						ids.push(n->getChildIdentifier(cChild));
					}
				}
				else if (n->getLevel() == 0)
				{
					IShape* ps;
					entry.getShape(&ps);
					Region* pr = dynamic_cast<Region*>(ps);
					
					#ifdef DEBUGAREA
					list_rec.push_back(new Rectangle(pr->m_pLow[0], pr->m_pLow[1], 
						pr->m_pHigh[0], pr->m_pHigh[1]));
					#endif					

					 // cout << n->getIdentifier() << TAB
					cout << partop_ptr->prefix_tile_id << bid<< TAB
					<< pr->m_pLow[0] << TAB << pr->m_pLow[1] << TAB
					<< pr->m_pHigh[0] << TAB << pr->m_pHigh[1] 
					<< endl;
					bid++;
					/*
					 cerr<< "set object rect from " 
					 << pr->m_pLow[0] << "," << pr->m_pLow[1] << " to "<< pr->m_pHigh[0] << "," << pr->m_pHigh[1] << endl;
					*/
					delete ps;
				}
			}

			if (!ids.empty()) {
				nextEntry = ids.front();
				ids.pop();
				hasNext = true;
			}
			else {
				hasNext = false;
			}
		}

};

// Customized reader for reading from a vector
class VecStreamReader : public IDataStream
{
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


void process_input(struct partition_op &partop) {
	// build in memory Tree
	VecStreamReader vecstream(&vec);
	IStorageManager* memoryFile = StorageManager::createNewMemoryStorageManager();
	id_type indexIdentifier = 0;

	ISpatialIndex* tree = RTree::createAndBulkLoadNewRTree(RTree::BLM_STR, 
		vecstream, *memoryFile, fillFactor, indexCapacity, partop.bucket_size, 
		2, SpatialIndex::RTree::RV_RSTAR, indexIdentifier);

	// Traverse through each leaf node which is a partition
	MyQueryStrategy qs(&partop);
	tree->queryStrategy(qs);

	#ifdef DEBUGAREA
	Rectangle *tmp;
	double span[2];
	double area_total;

	for (int k = 0; k < 2; k++) {
		span[k] = partop.high[k] - partop.low[k];
	}

	// Normalize the data
	for (vector<Rectangle*>::iterator it = list_rec.begin() ; it != list_rec.end(); ++it) {
		tmp = *it;
		tmp->low[0] = (tmp->low[0] - partop.low[0]) / span[0];
        	tmp->low[1] = (tmp->low[1] - partop.low[1]) / span[1];
        	tmp->high[0] = (tmp->high[0] - partop.low[0]) / span[0];
        	tmp->high[1] = (tmp->high[1] - partop.low[1]) / span[1];
        	area_total += (tmp->high[0] - tmp->low[0]) * (tmp->high[1] - tmp->low[1]);
	}

	cerr << "Area total: " << area_total << endl;
        if (list_rec.size() > 0) {
                cerr << "Area covered: " << findarea(list_rec) << endl;
        }

	for (vector<Rectangle*>::iterator it = list_rec.begin() ; it != list_rec.end(); ++it) {
		delete *it;
	}
	list_rec.clear();
	#endif 
	/* This will fail the code the Data is already removed by VecStreamReader
	for (vector<RTree::Data*>::iterator it = vec.begin() ; it != vec.end(); ++it) {
		delete *it;
	}*/
	vec.clear();
	delete tree;
	delete memoryFile;
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
			Region r(low, high, 2);
			obj = new RTree::Data(0, 0 , r, id);
			id++;
			vec.push_back(obj);

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

int main(int argc, char* argv[]) {
        cout.precision(15);
        struct partition_op partop;
        if (!extract_params_partitioning(argc, argv, partop)) {
                #ifdef DEBUG
                cerr << "Fail to extract parameters" << endl;
                #endif
                return -1;
        }

        if (!read_input(partop)) {
		cerr << "Error reading input in." << endl;
		return -1;
        }
	cerr.flush();
        cout.flush();
}

