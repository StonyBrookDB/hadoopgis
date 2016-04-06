#include <iostream>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Geometry.h>
#include <geos/operation/distance/DistanceOp.h>
#include <geos/geom/Point.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include <geos/opBuffer.h>
#include <spatialindex/SpatialIndex.h>


/* Result container for R-tree traversal 
 * used for spatial processing (spatialproc) */


/* Obtain R-tree MBR of a given geometry (for usage in R-tree indexing)  */
SpatialIndex::RTree::Data* parseInputPolygon(geos::geom::Geometry *p, SpatialIndex::id_type m_id) {
	double low[2], high[2];
	const geos::geom::Envelope * env = p->getEnvelopeInternal();
	low[0] = env->getMinX();
	low[1] = env->getMinY();
	high[0] = env->getMaxX();
	high[1] = env->getMaxY();

	SpatialIndex::Region r(low, high, 2);
	return new SpatialIndex::RTree::Data(0, 0 , r, m_id);// store a zero size null poiter.
};


/* Class used for R-tree traversal */
class MyVisitor : public SpatialIndex::IVisitor
{
	public:
		vector<SpatialIndex::id_type> matches; // contains ids of matching objects

	public:	
		MyVisitor() {}

		~MyVisitor() {
			matches.clear();
		}

		void visitNode(const SpatialIndex::INode& n) {}
		void visitData(std::string &s) {}

		void visitData(const SpatialIndex::IData& d)
		{
			matches.push_back(d.getIdentifier());
		}

		void visitData(std::vector<const SpatialIndex::IData*>& v) {}
		void visitData(std::vector<uint32_t>& v){}
};

/* Customized data stream to read from a mapping of int to geomery */
class GEOSDataStream : public SpatialIndex::IDataStream
{
	public:
		GEOSDataStream(map<int,geos::geom::Geometry*> * inputColl ) : m_pNext(0), len(0), m_id(0)
	{
		if (inputColl->empty())
			throw Tools::IllegalArgumentException("Input size is ZERO.");
		shapes = inputColl;
		m_id = 0;
		len = inputColl->size();
		iter = shapes->begin();
		readNextEntry();
	}

		virtual ~GEOSDataStream()
		{
			if (m_pNext != 0) delete m_pNext;
		}

		virtual SpatialIndex::IData* getNext()
		{
			if (m_pNext == 0) return 0;

			SpatialIndex::RTree::Data* ret = m_pNext;
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
				m_id = iter->first;
				m_pNext = parseInputPolygon(iter->second, m_id);
				iter++;
			}
		}

		SpatialIndex::RTree::Data* m_pNext;
		map<int,geos::geom::Geometry*> * shapes; 
		map<int,geos::geom::Geometry*>::iterator iter; 

		int len;
		SpatialIndex::id_type m_id;
};

/* Customized data stream to read from a file to build tile indexing, first field has tile id name  */
class GEOSDataStreamFileTile : public SpatialIndex::IDataStream
{
	public:
		std::map<SpatialIndex::id_type, std::string> *id_tiles;
	public:
		GEOSDataStreamFileTile(char *input_file, 
			std::map<SpatialIndex::id_type, std::string> *id_tiles_ptr) : m_pNext(0)
	{
		id_tiles = id_tiles_ptr;
		m_fin.open(input_file);
		m_id = 0;
		if (! m_fin)
			throw Tools::IllegalArgumentException("Input file not found.");

		readNextEntry();
	}

		virtual ~GEOSDataStreamFileTile()
		{
			if (m_pNext != 0) delete m_pNext;
		}

		virtual SpatialIndex::IData* getNext()
		{
			if (m_pNext == 0) return 0;

			SpatialIndex::RTree::Data* ret = m_pNext;
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
			double low[2], high[2];


			m_fin >> tile_id >> low[0] >> low[1] >> high[0] >> high[1];
			/* store tile_id */


			if (m_fin.good())
			{
				SpatialIndex::Region r(low, high, 2);
				m_pNext = new SpatialIndex::RTree::Data(sizeof(double), reinterpret_cast<byte*>(low), r, m_id);
				/* Use spatialproc struct */
				(*id_tiles)[m_id] = tile_id;
			//" " << low[0] << " "
			//	 	<< low[1] << " " < high[0] << " " << high[1] << endl;
			}
			m_id++;
		}

		SpatialIndex::id_type m_id;
		std::ifstream m_fin;
		SpatialIndex::RTree::Data* m_pNext;
};

/* Customized file stream of MBB to read from file; used by partitioners */
class SpaceStreamReader : public SpatialIndex::IDataStream
{
	public:
		SpaceStreamReader(std::string inputFile) : m_pNext(0)
	{
		m_fin.open(inputFile.c_str());

		if (! m_fin)
			throw Tools::IllegalArgumentException("Input file not found.");

		readNextEntry();
	}

		virtual ~SpaceStreamReader()
		{
			if (m_pNext != 0) delete m_pNext;
			m_fin.close();
		}

		virtual SpatialIndex::IData* getNext()
		{
			if (m_pNext == 0) return 0;

			SpatialIndex::RTree::Data* ret = m_pNext;
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
		}

		void readNextEntry()
		{
			double low[2], high[2];
			SpatialIndex::id_type id;


			m_fin >> id >> low[0] >> low[1] >> high[0] >> high[1] ;

			if (m_fin.good())
			{
				SpatialIndex::Region r(low, high, 2);
				m_pNext = new SpatialIndex::RTree::Data(0, 0 , r, id);// store a zero size null poiter.
			}
		}

		std::ifstream m_fin;
		SpatialIndex::RTree::Data* m_pNext;
};
