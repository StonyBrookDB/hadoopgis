#include <progparams/resque_datastructs_2d.hpp>

/* Containing methods to extract parameters and store them in query operator */

using namespace std;

/* Display help message to users */
void usage(){
	cerr  << endl << "Usage: program_name [OPTIONS]" << endl << "OPTIONS:" << endl;
	cerr << TAB << "-p,  --predicate" << TAB <<  "The spatial join predicate for query processing. \
Acceptable values are [st_intersects, st_disjoint, st_overlaps, st_within, st_equals,\
st_dwithin, st_crosses, st_touches, st_contains, st_nearest, st_nearest2]." << endl;
	cerr << TAB << "-i, --shpidx1"  << TAB << "The index of the geometry field from the \
first dataset. Index value starts from 1." << endl;
	cerr << TAB << "-j, --shpidx2"  << TAB << "The index of the geometry field from the second dataset.\
Index value starts from 1." << endl;
	cerr << TAB << "-a, --prefix1"  << TAB << "The path prefix of data set 1 (should be different from \
the one of data set 2" << endl;
	cerr << TAB << "-b, --prefix2"  << TAB << "The path prefix of data set 2 (should be different from \
the one of data set 1." << endl;
	cerr << TAB << "-d, --distance" << TAB << "Used together with st_dwithin predicate to \
indicate the join distance or used together with st_nearest to indicate the max distance \
to search for nearest neighbor. \
This field has no effect on other join predicates." << endl;	
	cerr << TAB << "-k, --knn" << TAB << "The number of nearest neighbor\
Only used in conjuction with the st_nearest or st_nearest2 predicate" << endl;
	cerr << TAB << "-c, --cachefile" << TAB << "The name of cache file. \
Dependent on operation/task" << endl;
	cerr << TAB << "-o, --offset"  << TAB << "The offset \
(number of fields to be adjusted as metadata information.\
Observe that for spatial processing reducer (resque): the first field \
is always the tile id. The join id is always the second field (2). \
Index starts from 1. Default value for reducer is 3" << endl;
	cerr << TAB << "-r, --replicate" << TAB <<  "Optional [true | false]. \
Indicates whether result pair are output twice for result pair that logically exists\
when the pair is swapped (position). e.g. intersection between object 1 and 2\
also indicates a logical intersection between object 2 and 1. \
Default value is false" << endl;
	cerr << TAB << "-e, --earth" << TAB << "Optional [true | false]\
Indicate wheather to compute spherical distance for point-point on earth." << endl;
	cerr << TAB << "-q, --samplerate" << TAB << "Optional. Sample rate \
used in partitioning (extracting MBB step)." << endl;
	cerr << TAB << "-x, --extract" << TAB << "[Mapper only] \
Mapper is used to extract MBBs only" << endl;
	cerr << TAB << "-m, --mbbread" << TAB << "[Mapper only] \
Mapper is reading MBBs" << endl;
	cerr << TAB << "-h, --dropjoinidx" << TAB << "[Mapper only] \
Mapper does not insert the join index after the tile ID field." << endl;
	cerr << TAB << "-s, --collectstat" << TAB << "[Mapper only] \
Mapper is used to collect statistics from MBB to determine space dimension" << endl;
	cerr << TAB << "-f, --fields"   << TAB << "Optional. \
Output field election parameter. Original fields \
should have a format set_id:field_id. Fields are delimited by commas. \
\nSpecial fields are [area1 | area2 | union | intersect |\
dice | jaccard | mindist | tileid] representing the area of objects\
from set 1, set 2, union, intersection,\
dice coefficient, intersection coefficient, dice statitics, jaccard coeeficient,\
minimum distance between the pair of polygons, and tile id correspondingly. \
\nField values and statistics will be output output order specifed by users. \
\nFor example: if we want to only output fields 1, 3, and 5\
from the first dataset (indicated with param -i), \
and output fields 1, 2, and 9 from the second dataset \
(indicated with param -j) followed by the area of object 2 \
and jaccard coefficient, \
then we can provide an argument as: --fields 1:1,1:3,1:5:2:1,2:2,2:9,tileid,\
area2,jacc" << endl;
}

/* Set fields to be output */
void set_projection_param(struct query_op &stop, char * arg)
{
	string param(arg);
	vector<string> fields;
	vector<string> selec;
	tokenize(param, fields, STR_OUTPUT_DELIM);
	int set_id = 0;

	for (int i = 0; i < fields.size(); i++) {
		string stat_param = fields[i];
		/* Check if the current field is one of the existing field in the 
		data set and collect its set id */

		tokenize(stat_param, selec, STR_SET_DELIM);
		if (selec.size() == 1) {
			// Special statistics below (no set delimiter/colon was found)	

			// Output cannot be taken directly from the original field
			stop.output_fields_set_id.push_back(SID_NEUTRAL);

			// Updating output_fields
			if (stat_param.compare(PARAM_STATS_TILE_ID) == 0 ) {
				stop.output_fields.push_back(STATS_TILE_ID);
			}
			else if (stat_param.compare(PARAM_STATS_AREA_1) == 0 ) {
				stop.output_fields.push_back(STATS_AREA_1);
				stop.needs_area_1 = true;
			}
			else if (stat_param.compare(PARAM_STATS_AREA_2) == 0) {
				stop.output_fields.push_back(STATS_AREA_2);
				stop.needs_area_2 = true;
			}
			else if (stat_param.compare(PARAM_STATS_UNION_AREA) == 0) {
				stop.output_fields.push_back(STATS_UNION_AREA);
				stop.needs_union = true;
			}
			else if (stat_param.compare(PARAM_STATS_INTERSECT_AREA) == 0) {
				stop.output_fields.push_back(STATS_INTERSECT_AREA);
				stop.needs_intersect = true;
			}
			else if (stat_param.compare(PARAM_STATS_JACCARD_COEF ) == 0) {
				stop.output_fields.push_back(STATS_JACCARD_COEF);
				stop.needs_jaccard = true;
			}
			else if (stat_param.compare(PARAM_STATS_DICE_COEF) == 0) {	
				stop.output_fields.push_back(STATS_DICE_COEF);
				stop.needs_dice = true;
			} 
			else if (stat_param.compare(PARAM_STATS_MIN_DIST) == 0) {	
				stop.output_fields.push_back(STATS_MIN_DIST);
				stop.needs_min_distance = true;
			} 

			else {
				#ifdef DEBUG
				cerr << "Unrecognizeable option for output statistics" << endl;
				#endif
			}
		}
		else {
			// Colon/Set delimiter was found
			// Regular original field
			set_id = atoi(selec[0].c_str());
			switch (set_id) {
				case SID_1:
				case SID_2:
					stop.output_fields_set_id.push_back(set_id);
					stop.output_fields.push_back(atoi(selec[1].c_str()) - 1 + stop.offset);
					break;
				default :
					#ifdef DEBUG
					cerr << "Unrecognizeable set ID for output statistics ID" << endl;
					#endif
					break;
			}
		}
		selec.clear();
	}

	// Dependency adjustment
	if (stop.needs_jaccard) {
		stop.needs_intersect = true;
		stop.needs_union = true;
	}

	if (stop.needs_dice) {
		stop.needs_intersect = true;
		stop.needs_area_1 = true;
		stop.needs_area_2 = true;
	}
}

/* This function converts expensive char comparison into fast int comparison 
 * during spatial processing to determine the spatial predicate being used*/
int get_join_predicate(char * predicate_str)
{
	if (strcmp(predicate_str, PARAM_PREDICATE_INTERSECTS.c_str()) == 0) {
		return ST_INTERSECTS ; 
	} 
	else if (strcmp(predicate_str, PARAM_PREDICATE_TOUCHES.c_str()) == 0) {
		return ST_TOUCHES;
	} 
	else if (strcmp(predicate_str, PARAM_PREDICATE_CROSSES.c_str()) == 0) {
		return ST_CROSSES;
	} 
	else if (strcmp(predicate_str, PARAM_PREDICATE_CONTAINS.c_str()) == 0) {
		return ST_CONTAINS;
	} 
	else if (strcmp(predicate_str, PARAM_PREDICATE_ADJACENT.c_str()) == 0) {
		return ST_ADJACENT;
	} 
	else if (strcmp(predicate_str, PARAM_PREDICATE_DISJOINT.c_str()) == 0) {
		return ST_DISJOINT;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_EQUALS.c_str()) == 0) {
		return ST_EQUALS;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_DWITHIN.c_str()) == 0) {
		return ST_DWITHIN;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_WITHIN.c_str()) == 0) {
		return ST_WITHIN;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_OVERLAPS.c_str()) == 0) {
		return ST_OVERLAPS;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_NEAREST.c_str()) == 0) {
		return ST_NEAREST;
	}
	else if (strcmp(predicate_str, PARAM_PREDICATE_NEAREST_NO_BOUND.c_str()) == 0) {
		return ST_NEAREST_2;
	}
	else {
		#ifdef DEBUG
		cerr << "unrecognized join predicate " << endl;
		#endif
		return -1;
	}
}


// This function  extracts command line arguments
bool extract_params(int argc, char** argv, struct query_op &stop, struct query_temp &sttemp){ 
	stop.use_cache_file = false;
	// initlize query operator 
	stop.expansion_distance = 0.0;
	stop.k_neighbors = 0;
	stop.join_predicate = 0;
	stop.shape_idx_1 = 0;
	stop.shape_idx_2 = 0 ;
	stop.join_cardinality = 0;

	stop.drop_join_idx = false;

	stop.prefix_1 = NULL;
	stop.prefix_2 = NULL;

	stop.needs_area_1 = false;
	stop.needs_area_2 = false;
	stop.needs_union = false;
	stop.needs_intersect = false;
	stop.needs_dice = false;
	stop.needs_jaccard = false;
	stop.needs_min_distance = false;

	stop.result_pair_duplicate = true;
	stop.reading_mbb = false;

	stop.use_earth_distance = false;

	stop.output_fields.clear();
	stop.output_fields_set_id.clear();

	stop.use_cache_file = false;
	stop.extract_mbb = false;
	stop.collect_mbb_stat = false;
	//stop.use_sampling = false;
	//stop.sample_rate = 1.0;

	sttemp.nearest_distances.clear();	
	sttemp.area1 = -1;
	sttemp.area2 = -1;
	sttemp.union_area = -1;
	sttemp.intersect_area = -1;
	sttemp.dice = -1;
	sttemp.jaccard = -1;
	sttemp.distance = -1;


	int option_index = 0;
	/* getopt_long uses opterr to report error*/
	opterr = 0 ;
	struct option long_options[] =
	{
		{"distance",   required_argument, 0, 'd'},
		{"shpidx1",  required_argument, 0, 'i'},
		{"shpidx2",  required_argument, 0, 'j'},
		{"predicate",  required_argument, 0, 'p'},
		{"knn",  required_argument, 0, 'k'},
		{"fields",     required_argument, 0, 'f'},
		{"earth",     required_argument, 0, 'e'},
		{"replicate",     required_argument, 0, 'r'},
		{"cachefile",     required_argument, 0, 'c'},
		{"prefix1",     required_argument, 0, 'a'},
		{"prefix2",     required_argument, 0, 'b'},
		{"extract",     no_argument, 0, 'x'},
		{"mbbread",     no_argument, 0, 'm'},
		{"dropjoinidx",     no_argument, 0, 'h'},
		{"offset",     required_argument, 0, 'o'},
		//{"samplerate",     required_argument, 0, 'q'},

		// Specific to controller only
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long (argc, argv, "o:p:i:j:d:f:k:r:e:c:a:b:xmhs", long_options, &option_index)) != -1){
		switch (c)
		{
			case 0:
				/* If this option set a flag, do nothing else now. */
				if (long_options[option_index].flag != 0)
					break;
				cout << "option " << long_options[option_index].name ;
				if (optarg)
					cout << "a with arg " << optarg ;
				cout << endl;
				break;

			case 'o':
				stop.offset = strtol(optarg, NULL, 10);
				#ifdef DEBUG
					cerr << "Offset: " << stop.offset << endl;
				#endif
				break;

			case 'p':
				stop.join_predicate = get_join_predicate(optarg);
				#ifdef DEBUG
					cerr << "predicate: " << stop.join_predicate << endl;
       	                        #endif
				break;

			case 'i':
				stop.shape_idx_1 = strtol(optarg, NULL, 10) - 1;
                                stop.join_cardinality++;
                                #ifdef DEBUG
					cerr << "geometry index i (set 1) before offsetting: " 
						<< stop.shape_idx_1 << endl;
				#endif
                                break;

			case 'j':
				stop.shape_idx_2 = strtol(optarg, NULL, 10) - 1;
                                stop.join_cardinality++;
                                #ifdef DEBUG
					cerr << "geometry index j (set 2) before offsetting: " 
						<< stop.shape_idx_2 << endl;
				#endif
                                break;

			case 'd':
				stop.expansion_distance = atof(optarg);
				#ifdef DEBUG
					cerr << "Search distance/Within distance parameter " 
						<< stop.expansion_distance << endl;
				#endif
				break;

			case 'k':
				stop.k_neighbors = strtol(optarg, NULL, 10);
				#ifdef DEBUG
					cerr << "Number of neighbors: " 
						<< stop.k_neighbors << endl;
				#endif
				break;

			case 'f':
                                set_projection_param(stop, optarg);
                                #ifdef DEBUG
                                        cerr << "Original params: " << optarg << endl;
					cerr << "output field: " << endl;
					for (int m = 0; m < stop.output_fields.size(); m++) {
						cerr << stop.output_fields[m] << SEP;
					}
					cerr << endl << "setid: ";
					for (int m = 0; m < stop.output_fields_set_id.size(); m++) {
						cerr << stop.output_fields_set_id[m] << SEP;
					}
					cerr << endl;
                                #endif
                                break;

			case 'r':
				stop.result_pair_duplicate = strcmp(optarg, "true") == 0;
				#ifdef DEBUG
					cerr << "Allows symmetric result pairs: "
						<< stop.result_pair_duplicate << endl;
				#endif
				break;

			case 'e':
				stop.use_earth_distance = strcmp(optarg, "true") == 0;
				#ifdef DEBUG
					cerr << "Using earth distance: "
						<< stop.use_earth_distance << endl;
				#endif
				break;

			case 'c':
				stop.cachefilename = optarg;
				stop.use_cache_file = true;
				#ifdef DEBUG
					cerr << "Cache file name: " << stop.cachefilename << endl;
				#endif
				break;

			case 'a':
				stop.prefix_1 = optarg;
				#ifdef DEBUG
					cerr << "Prefix for data set 1 path: " << stop.prefix_1 << endl;
				#endif
				break;

			case 'b':
				stop.prefix_2 = optarg;
				#ifdef DEBUG
					cerr << "Prefix for data set 2 path: " << stop.prefix_2 << endl;
				#endif
				break;

			case 'x':
				stop.extract_mbb = true;
				#ifdef DEBUG
					cerr << "Set to extract MBBs: " << stop.extract_mbb << endl;
				#endif
				break;
			
			case 'm':
				stop.reading_mbb = true;
				#ifdef DEBUG
					cerr << "Set to extract MBBs: " << stop.extract_mbb << endl;
				#endif
				break;

			case 'h':
				stop.drop_join_idx = true;
				#ifdef DEBUG
					cerr << "Dropping join index " << stop.extract_mbb << endl;
				#endif
				break;
				
			case 's':
				stop.collect_mbb_stat = true;
				#ifdef DEBUG
					cerr << "Mapper in collecting mbb statistics mode: " << stop.collect_mbb_stat << endl;
				#endif
				break;
			/*
			case 'q':
				stop.sample_rate = atof(optarg);
				stop.use_sampling = true;
				#ifdef DEBUG
					cerr << "Sample rate: " << stop.sample_rate << endl;
				#endif
				break;
			*/
			case '?':
				return false;
				/* getopt_long already printed an error message. */
				break;

			default:
				return false;
		}
	}

	// Adjusting the actual geometry field (shift) to account
	//   for tile_id and join_index
	stop.shape_idx_1 += stop.offset;
	stop.shape_idx_2 += stop.offset;
	#ifdef DEBUG
	cerr << "Offset:  " << stop.offset << endl;
	cerr << "geometry index i (set 1) after offsetting: " << stop.shape_idx_1 << endl;
	cerr << "geometry index j (set 2) after offsetting: " << stop.shape_idx_2 << endl;
        cerr << "join cardinality: " << stop.join_cardinality << endl;
	#endif

	// query operator validation 
	/*
	if (!stop.drop_join_idx && stop.join_predicate <= 0 ) {
		#ifdef DEBUG 
		cerr << "Query predicate is NOT set properly. Please refer to the documentation." << endl ; 
		#endif
		return false;
	}*/
	// check if distance is set for dwithin predicate
	if (stop.join_predicate == ST_DWITHIN && stop.expansion_distance == 0.0) { 
		#ifdef DEBUG 
		cerr << "Distance parameter is NOT set properly. Please refer to the documentation." << endl ;
		#endif
		return false;
	}
	if (stop.join_cardinality == 0) {
		#ifdef DEBUG 
		cerr << "Join cardinality are NOT set properly. Please refer to the documentation." << endl ;
		#endif
		return false; 
	}
	
	if ((stop.join_predicate == ST_NEAREST || stop.join_predicate == ST_NEAREST_2)
		 && stop.k_neighbors <= 0) {
		#ifdef DEBUG 
		cerr << "K-the number of nearest neighbors is NOT set properly. Please refer to the documentation." << endl ;
		#endif
		return false; 
	}

	return true;
}
