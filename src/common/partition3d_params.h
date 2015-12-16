
/* Containing methods to extract parameters and store them in query operator */
#include <iostream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <vector>

using namespace std;

void init_params_partitioning(struct partition_op & partop) {
	partop.to_be_normalized = false;
	partop.to_be_denormalized = false;
	partop.offset = 1;
	partop.parallel_partitioning = false;
}

/* Extract parameters for partitioning */
bool extract_params_partitioning(int argc, char** argv, struct partition_op & partop){ 
	init_params_partitioning(partop);
	/* getopt_long stores the option index here. */
	int option_index = 0;
	/* getopt_long uses opterr to report error*/
	opterr = 0 ;
	struct option long_options[] =
	{
		{"min_x",   required_argument, 0, 'a'},
		{"min_y",     required_argument, 0, 'b'},
		{"min_z",     required_argument, 0, 'c'},
		{"max_x",     required_argument, 0, 'k'},
		{"max_y",     required_argument, 0, 'l'},
		{"max_z",     required_argument, 0, 'm'},
		{"norm",     required_argument, 0, 'n'},
		{"denorm",     required_argument, 0, 'o'},
		{"offset",     required_argument, 0, 'f'},
		{"file_name",     required_argument, 0, 'z'},
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long (argc, argv, "a:b:c:k:l:m:f:z:no", long_options, &option_index)) != -1){
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

			case 'a':
				partop.min_x = atof(optarg);
				#ifdef DEBUG
					cerr << "Space min_x: " << partop.min_x << endl;
                                #endif
				break;
			case 'b':
				partop.min_y = atof(optarg);
				#ifdef DEBUG
					cerr << "Space min_y: " << partop.min_y << endl;
                                #endif
				break;
			case 'c':
				partop.min_z = atof(optarg);
				#ifdef DEBUG
					cerr << "Space max_x: " << partop.min_z << endl;
                                #endif
				break;
			case 'k':
				partop.max_x = atof(optarg);
				#ifdef DEBUG
					cerr << "Space max_x: " << partop.max_x << endl;
                                #endif
				break;
			case 'l':
				partop.max_y = atof(optarg);
				#ifdef DEBUG
					cerr << "Space max_y: " << partop.max_y << endl;
                                #endif
				break;
			case 'm':
				partop.max_z = atof(optarg);
				#ifdef DEBUG
					cerr << "Space max_z: " << partop.max_z << endl;
                                #endif
				break;
			case 'f':
				partop.offset = atoi(optarg);
				#ifdef DEBUG
					cerr << "Space offset: " << partop.offset << endl;
                                #endif
				break;
			case 'n':
				partop.to_be_normalized = true;
				#ifdef DEBUG
					cerr << "To-be-normalized:" << partop.to_be_normalized << endl;
                                #endif
				break;

			case 'o':
				partop.to_be_denormalized = true;
				#ifdef DEBUG
					cerr << "To-be-denormalized: " << partop.to_be_denormalized << endl;
                                #endif
				break;
			
			case 'z':
				partop.parallel_partitioning = true;
				partop.file_name = optarg;
				#ifdef DEBUG
					cerr << "Use index file:" << partop.file_name << endl;
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
	if (partop.to_be_denormalized && partop.to_be_normalized) {
		/* Mutually exclusive options */
		#ifdef DEBUG
		cerr << "Mutually exclusive functions -n and -o" << endl;
		#endif
		return false;
	}
	if (partop.parallel_partitioning && !partop.file_name) {
		#ifdef DEBUG
		cerr << "Invalid index file name" << endl;
		#endif
		return false;
	}
	return true;
}
