
// geos
#include <spatialindex/SpatialIndex.h>

using namespace std;
using namespace SpatialIndex;


/* Result container for R-tree traversal */


/* Obtain R-tree MBR of a given geometry (for usage in R-tree indexing)  */
/*
RTree::Data* parseInputPolygon(, id_type m_id) {
	double low[2], high[2];
	const Envelope * env = p->getEnvelopeInternal();
	low[0] = env->getMinX();
	low[1] = env->getMinY();
	high[0] = env->getMaxX();
	high[1] = env->getMaxY();

	Region r(low, high, 2);
	return new RTree::Data(0, 0 , r, m_id);// store a zero size null poiter.
};
*/
class MyVisitor : public IVisitor
{
	public:
		vector<SpatialIndex::id_type> matches; // contains ids of matching objects

	public:	
		MyVisitor() {}

		~MyVisitor() {
			matches.clear();
		}

		void visitNode(const INode& n) {}
		void visitData(std::string &s) {}

		void visitData(const IData& d)
		{
			matches.push_back(d.getIdentifier());
		}

		void visitData(std::vector<const IData*>& v) {}
		void visitData(std::vector<uint32_t>& v){}
};

/* Customized data stream to read from a mapping of int to geomery */
class CustomDataStream : public IDataStream
{
	public:
		CustomDataStream(vector<struct mbb_3d *> *inputdata) : m_pNext(0), len(0), m_id(0)
	{
		if (inputdata->empty())
			throw Tools::IllegalArgumentException("Input size is ZERO.");
		shapes = inputdata;
		len = inputdata->size();
		iter = shapes->begin();
		readNextEntry();
	}

		virtual ~CustomDataStream()
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
			return len;
			//throw Tools::NotSupportedException("Operation not supported.");
		}

		virtual void rewind()
		{
			if (m_pNext != 0)
			{
				delete m_pNext;
				m_pNext = 0;
			}

			m_id  = 0;
			iter = shapes->begin();
			readNextEntry();
		}

		void readNextEntry()
		{
			if (iter != shapes->end())
			{
				//std::cerr<< "readNextEntry m_id == " << m_id << std::endl;
				Region r((*iter)->low, (*iter)->high, 3);	
				m_pNext = new RTree::Data(sizeof(double), reinterpret_cast<byte*>((*iter)->low), r, m_id);
				iter++;
				m_id++;
			}
		}

		RTree::Data* m_pNext;
		vector<struct mbb_3d*> * shapes; 
		vector<struct mbb_3d*>::iterator iter; 

		int len;
		id_type m_id;
};

class GEOSDataStreamFileTile : public IDataStream
{
	public:
		GEOSDataStreamFileTile(char *input_file) : m_pNext(0)
	{
		m_fin.open(input_file);

		if (! m_fin)
			throw Tools::IllegalArgumentException("Input file not found.");

		readNextEntry();
	}

		virtual ~GEOSDataStreamFileTile()
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

			m_fin.seekg(0, std::ios::beg);
			readNextEntry();
			m_id = 0;
		}

		void readNextEntry()
		{
			std::string tile_id;
			double low[3], high[3];


			m_fin >> tile_id >> low[0] >> low[1] >> low[2]  >>  high[0] >> high[1] >> high[2];
			/* store tile_id */

			if (m_fin.good())
			{
				Region r(low, high, 3);
				m_pNext = new RTree::Data(sizeof(double), reinterpret_cast<byte*>(low), r, m_id);
				/* Use spatialproc struct */
				stop.id_tiles[m_id] = tile_id;
			}
			m_id++;
		}

		id_type m_id;
		std::ifstream m_fin;
		RTree::Data* m_pNext;
};

/* Create an R-tree index on a given set of polygons */
bool build_index_geoms(int sid, ISpatialIndex* & spidx, IStorageManager* & storage) {
	// build spatial index on tile boundaries 
	id_type  indexIdentifier;
	CustomDataStream stream(&(sttemp.mbbdata[sid]));

	storage = StorageManager::createNewMemoryStorageManager();
	spidx   = RTree::createAndBulkLoadNewRTree(RTree::BLM_STR, stream, *storage, 
			FillFactor,
			IndexCapacity,
			LeafCapacity,
			3, 
			RTree::RV_RSTAR, indexIdentifier);
	// Error checking 
	return spidx->isIndexValid();
}

