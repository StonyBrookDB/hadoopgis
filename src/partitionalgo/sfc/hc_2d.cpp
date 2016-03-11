
#include <partitionalgo/sfc/hc_2d.hpp>

/**
 * Computes the hilbert value for two given integers.
 * @param x the value of the first dimension
 * @param y the value of the second dimension
 * @return the hilbert value for x and y
 */
long hilbert2d (int x, int y) {
	return hilbert2d(x, y, 1<<31);
}

long getHilbertValue(double obj[]) {
	double x = (obj[0] + obj[2]) / 2;
	double y = (obj[1] + obj[3]) / 2;
	return hilbert2d((int) (x*FILLING_CURVE_PRECISION),(int) (y*FILLING_CURVE_PRECISION));
}


void printShape(double obj[]){
	string SPACE = " "; 
	long hcval = getHilbertValue(obj);
	cout << obj[0] << SPACE << obj[1] << SPACE << obj[2] << SPACE << obj[3] << SPACE << hcval <<endl;
}

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


bool read_input(struct partition_op &partop) {
	string input_line;
	string prevtileid = "";
	string tile_id;
	vector<string> fields;
	partop.object_count = 0;

	double *obj;

	while (cin && getline(cin, input_line) && !cin.eof()) {
		try {
			istringstream ss(input_line);
			obj = new double[4]; 
			ss >> tile_id >> obj[0] >> obj[1] >> obj[2] >> obj[3];

			if (prevtileid.compare(tile_id) != 0 && prevtileid.size() > 0) {
				update_partop_info(partop, prevtileid, prevtileid  + prefix_tile_id);
				process_input(partop);
				// total_count += partop.object_count;
				partop.object_count = 0;
			}
			prevtileid = tile_id;

			// Add the object to new tile
			obj_list.push_back(obj);

			#ifdef DEBUG
			printShape(obj);
			#endif

			fields.clear();
			partop.object_count++;
		} catch (...) {

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
	//total_count += partop.object_count;
	cleanup(partop);
	return true;
}

void process_input(struct partition_op &partop) {
	// partition based on hilbert curve
	double low[NUMBER_DIMENSIONS];
	double high[NUMBER_DIMENSIONS];
	id_type tid = 0;
	double *obj;

	// Normalize the data
	double span[NUMBER_DIMENSIONS]; // Space span
	for (int k = 0; k < 2; k++) {
		span[k] = partop.high[k] - partop.low[k];
	}

	vector<double*>::size_type len = obj_list.size();
	// sort object based on Hilbert Curve value
	for (vector<double*>::iterator it = obj_list.begin(); 
		it != obj_list.end(); it++)
	{
		obj = *it;
		obj[0] = (obj[0] - partop.low[0]) / span[0];
		obj[1] = (obj[1] - partop.low[1]) / span[1];
		obj[2] = (obj[2] - partop.low[0]) / span[0];
		obj[3] = (obj[3] - partop.low[1]) / span[1];
	}

	#ifdef DEBUG
	cerr << "Begin sorting tile objects " << endl;
	#endif
	std::sort (obj_list.begin(), obj_list.end(), hcsorter); 
	#ifdef DEBUG
	cerr << "Done sorting tile " << endl;

	for (vector<double*>::iterator it = obj_list.begin(); 
		it != obj_list.end(); it++)
	{
		obj = *it;
		printShape(obj);
	}
	#endif


	#ifdef DEBUGAREA
	double area_total = 0;
	vector<Rectangle*> list_rec;
	#endif

	// Split the HC range into equal sized bucket
	for (vector<double*>::size_type i = 0; i < len; i += partop.bucket_size)
	{
		obj = obj_list[i];
		low[0] = obj[0];
		low[1] = obj[1];
		high[0] = obj[2];
		high[1] = obj[3];

		for (int j = 1; j < partop.bucket_size - 1 && i+j < len; j++)
		{
			obj = obj_list[i+j] ;
			if (obj[0] < low[0])  low[0] = obj[0];
			if (obj[1] < low[1])  low[1] = obj[1];
			if (obj[2] > high[0]) high[0] = obj[2];
			if (obj[3] > high[1]) high[1] = obj[3];
		}
		/*
		low[0] = std::numeric_limits<double>::max();
		low[1] = std::numeric_limits<double>::max();
		high[0] = 0.0 ;
		high[1] = 0.0 ;
		for (int j = 0; j < partop.bucket_size-1 && i+j < len; j++)
		{
			cout << "i+j = " << i+j << endl;
			obj =  obj_list[i+j] ;
			if (obj[0] < low[0])  low[0] = obj[0];
			if (obj[1] < low[1])  low[1] = obj[1];
			if (obj[2] > high[0]) high[0] = obj[2];
			if (obj[3] > high[1]) high[1] = obj[3];
		//	cout << "i=" << i << " j=" <<  j << endl;
		}
		*/
		#ifdef DEBUGAREA
		area_total += (high[0] - low[0]) * (high[1] - low[1]);
		list_rec.push_back(new Rectangle(low[0], low[1], high[0], high[1]));
		#endif
		// Denormalize the tile boundary and output
		/*
		low[0] = low[0] * span[0] + partop.low[0];
		low[1] = low[0] * span[0] + partop.low[0];
		high[0] = low[0] * span[0] + partop.low[0];
		high[0] = low[0] * span[0] + partop.low[0];
		*/
		cout << partop.prefix_tile_id << tid 
			<< TAB << low[0] * span[0] + partop.low[0]
			<< TAB << low[1] * span[1] + partop.low[1]
			<< TAB << high[0] * span[0] + partop.low[0]
			<< TAB << high[1] * span[1] + partop.low[1]
			<< endl;
		tid++;
	}
	#ifdef DEBUGAREA
	cerr << "Area total " << area_total << endl;
	if (list_rec.size() > 0) {
		cerr << "Area covered: " << findarea(list_rec) << endl;
	}

	for (vector<Rectangle*>::iterator it = list_rec.begin() ; it != list_rec.end(); ++it) {
		delete *it;
	}
	#endif

	// cleanup allocated memory 
	for (vector<double*>::iterator it = obj_list.begin() ; it != obj_list.end(); ++it) {
		delete [] *it;
	}
	obj_list.clear();
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

	int prec = 20; // Precision of Hilbert curve
	uint32_t bucket_size = partop.bucket_size;   
	// assign precision 
	FILLING_CURVE_PRECISION = 1 << prec;

	if (!read_input(partop)) {
		cerr << "Error reading input in" << endl;
		return -1;
	}
	cout.flush();
}

