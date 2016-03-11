
#include <partitionalgo/fg/fg_2d.hpp>

using namespace std;
// using namespace boost;
namespace po = boost::program_options;
// main method
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
#ifdef DEBUGTIME
	Timer t; 
#endif

	return 0;
}

void generate_partitions(struct partition_op & partop) {
	if (partop.object_count == 0) {
		return;
	}

	int bucket_size = partop.bucket_size;
	double span[NUMBER_DIMENSIONS];
	double region_width[NUMBER_DIMENSIONS];
	int num_splits[NUMBER_DIMENSIONS];
	int k;
	int tid = 0;

	for (k = 0; k < NUMBER_DIMENSIONS; k++) {
		span[k] = partop.high[k] - partop.low[k];
	}

#ifdef DEBUG
	cerr << "Spans " << span[1] << TAB << span[0] << endl; 
	cerr << "Object count: " << partop.object_count;
#endif


	/* Number of splits are not set */
	if (span[1] > span[0]) {
		// We prefer to split into more-square regions than long rectangles
		num_splits[1] = max(ceil(sqrt(static_cast<double>(partop.object_count) / bucket_size 
						* span[1] / span[0])), 1.0);	
		num_splits[0] = max(ceil(static_cast<double>(partop.object_count) / bucket_size 
					/ num_splits[1]), 1.0);
	} else {
		num_splits[0] = max(ceil(sqrt(static_cast<double>(partop.object_count) / bucket_size 
						* span[0] / span[1])), 1.0);	
		num_splits[1] = max(ceil(static_cast<double>(partop.object_count) / bucket_size 
					/ num_splits[0]), 1.0);
	}
#ifdef DEBUG
	cerr << "Number splits: " << TAB << num_splits[0] << TAB << num_splits[1] << endl;
#endif

	for (k = 0; k < NUMBER_DIMENSIONS; k++) {
		region_width[k] = span[k] / num_splits[k];
	}

	for (int i = 0; i < num_splits[0]; i++) {
		for (int j = 0; j < num_splits[1]; j++) {
			cout << partop.prefix_tile_id << tid
				<< TAB << (i * region_width[0] + partop.low[0])
				<< TAB << (j * region_width[1] + partop.low[1])
				<< TAB << ((i + 1) * region_width[0] + partop.low[0])
				<< TAB << ((j + 1) * region_width[1] + partop.low[1])
				<< endl;
			tid++;
		}
	}

}

bool read_input(struct partition_op &partop) {
	string input_line;
	string prevtileid = "";
	string tile_id;
	vector<string> fields;

	partop.object_count = 0;
	while (cin && getline(cin, input_line) && !cin.eof()) {
		try {
		tokenize(input_line, fields, TAB, true);
		if (fields.size() < 5) {
			continue;
		}
		tile_id = fields[0];

		//cerr << "Field id: " << fields[0] << endl;
		if (prevtileid.compare(tile_id) != 0 && prevtileid.size() > 0) {
		//	cerr << "Processing old field" << prevtileid << TAB << fields[0] <<  endl;
			update_partop_info(partop, prevtileid, prevtileid  + prefix_tile_id);
			generate_partitions(partop);
			partop.object_count = 0;
		}
		prevtileid = tile_id;

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
		generate_partitions(partop);
	}
	cleanup(partop);
	return true;
}


