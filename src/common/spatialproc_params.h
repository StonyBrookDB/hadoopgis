
/* Containing methods to extract parameters and store them in query operator */
#include <iostream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <vector>

using namespace std;

/* Set fields to be output */
void set_projection_param(char * arg)
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

bool extract_params(int argc, char** argv ){ 
	/* getopt_long stores the option index here. */
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
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long (argc, argv, "p:i:j:d:f:k:r:e:c:a:b:", long_options, &option_index)) != -1){
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

			case 'p':
				stop.JOIN_PREDICATE = get_join_predicate(optarg);
				#ifdef DEBUG
					cerr << "predicate: " 
						<< stop.JOIN_PREDICATE << endl;
                                #endif
				break;

			case 'i':
				// Adjusting the actual geometry field (shift) to account
				//   for tile_id and join_index
				stop.shape_idx_1 = strtol(optarg, NULL, 10) - 1 + stop.offset;
                                stop.join_cardinality++;
                                #ifdef DEBUG
					cerr << "geometry index i (set 1): " 
						<< stop.shape_idx_1 << endl;
				#endif
                                break;

			case 'j':
				stop.shape_idx_2 = strtol(optarg, NULL, 10) - 1 + stop.offset;
                                stop.join_cardinality++;
                                #ifdef DEBUG
					cerr << "geometry index j (set 2): " 
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
                                set_projection_param(optarg);
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
					cerr << "Prefix for data set 2 path:  " << stop.prefix_2 << endl;
				#endif
				break;

			case '?':
				return false;
				/* getopt_long already printed an error message. */
				break;

			default:
				return false;
		}
	}

	// query operator validation 
	if (stop.JOIN_PREDICATE <= 0 ) {
		#ifdef DEBUG 
		cerr << "Query predicate is NOT set properly. Please refer to the documentation." << endl ; 
		#endif
		return false;
	}
	// check if distance is set for dwithin predicate
	if (stop.JOIN_PREDICATE == ST_DWITHIN && stop.expansion_distance == 0.0) { 
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
	
	if ((stop.JOIN_PREDICATE == ST_NEAREST || stop.JOIN_PREDICATE == ST_NEAREST_2)
		 && stop.k_neighbors <= 0) {
		#ifdef DEBUG 
		cerr << "K-the number of nearest neighbors is NOT set properly. Please refer to the documentation." << endl ;
		#endif
		return false; 
	}

	#ifdef DEBUG 
	print_stop();
	#endif
	
	return true;
}
