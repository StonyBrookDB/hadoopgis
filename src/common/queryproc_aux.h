#include <cstdlib>
#include <sstream>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Geometry.h>
#include <geos/operation/distance/DistanceOp.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include <geos/opBuffer.h>

using namespace geos;
using namespace geos::io;
using namespace geos::geom;
using namespace geos::operation::buffer; 
using namespace geos::operation::distance;
using namespace std;


// Save the mbb of object represented in tmp_line into low and high
void get_mbb(string tmp_line, double *low, double *high) {
	// Attempt to read WKT
	PrecisionModel *md = new PrecisionModel();
	GeometryFactory *gf = new GeometryFactory(md, 0);
        WKTReader *wkt_reader= new WKTReader(gf);

	try {
		Geometry *geom = wkt_reader->read(tmp_line);
		const Envelope * env = geom->getEnvelopeInternal();
		low[0] = env->getMinX();
		low[1] = env->getMinY();
		high[0] = env->getMaxX();
		high[1] = env->getMaxY();
		delete geom;
	} catch (...) {
		#ifdef DEBUG
		cerr << "Cannot convert object to WKT format. Extracting objects as if it's MBB" << endl;
		#endif
		istringstream ss(tmp_line);
		ss >> low[0] >> low[1] >> high[0] >> high[1];
	}
	delete md;
	delete gf;
	delete wkt_reader;
}

string get_wkt_from_mbb(double *low, double *high) {
	stringstream tmpss;
	tmpss << "POLYGON((" << low[0] << SPACE << high[0] 
		<< COMMA << low[1] << SPACE << high[0]
		<< COMMA << low[1] << SPACE << high[1]
		<< COMMA << low[0] << SPACE << high[1]
		<< COMMA << low[0] << SPACE << high[0] << "))";
	return tmpss.str();
}

/* Check if the node MBB intersects with the object MBB */
bool intersects(double *low, double *high, double *wlow, double *whigh) {
	return !(wlow[0] > high[0] || whigh[0] < low[0]
		|| whigh[1] < low[1] || wlow[1] > high[1]);
}
