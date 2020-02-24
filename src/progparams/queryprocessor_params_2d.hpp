
#include <progparams/queryprocessor_datastructs_2d.hpp>

namespace po = boost::program_options;

void init_params(struct framework_vars &fr_vars) {
	fr_vars.loaded_1 = fr_vars.loaded_2 = false;
	fr_vars.size_1 = fr_vars.size_2 = -1;
	fr_vars.partition_method = "bsp";
	fr_vars.partition_method_2 = "bsp";
	fr_vars.partitioningparams = "";
	fr_vars.obtain_size_1 = fr_vars.obtain_size_2 = 0;
	fr_vars.sampling_rate = 1.0;
	fr_vars.bucket_size = -1;
	fr_vars.para_partition = false;
	fr_vars.rough_bucket_size = -1;
	fr_vars.knn = 1;
	fr_vars.overwritepath = false;
}


bool extract_params(int argc, char **argv, struct framework_vars &fr_vars) {
	fr_vars.join_cardinality = 1;
	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "This help message")
			("querytype,q", po::value<string>(&fr_vars.query_type), "Query type [ partition | contaiment | spjoin]")
			("bucket,t", po::value<long>(&fr_vars.bucket_size), "Fine-grain level tile size for spjoin")
			("blocksize", po::value<long>(&fr_vars.block_size), "Fine-grain level bucket size for partitioning (loading data)")
			("roughbucket", po::value<long>(&fr_vars.rough_bucket_size), "Rough level bucket size for partitioning")
			("input1,a", po::value<string>(&fr_vars.input_path_1), "HDFS file path to data set 1")
			("input2,b", po::value<string>(&fr_vars.input_path_2), "HDFS file path to data set 2")
			("geom1,i", po::value<int>(&fr_vars.shp_idx_1), "Field number of data set 1 containing the geometry")
			("geom2,j", po::value<int>(&fr_vars.shp_idx_2), "Field number of data set 2 containing the geometry")
			("outputpath,h", po::value<string>(&fr_vars.output_path), "Output path")
			("distance,d", po::value<double>(&fr_vars.distance), "Distance (used for certain predicates)")
			("knn,k", po::value<int>(&fr_vars.knn), "The number of nearest neighbor. \
Only used in conjuction with the st_nearest or st_nearest2 predicate")
			("outputfields,f", po::value<string>(&fr_vars.output_fields), "Fields to be included in the final output \
separated by commas. See the full documentation. Regular fields from datasets are in the format datasetnum:fieldnum, e.g. 1:1,2:3,1:14. \
Field counting starts from 1. Optional statistics include: area1, area2, union, intersect, jaccard, dice, mindist")
			("containfile", po::value<string>(&fr_vars.user_file), "User file containing window used for containment query")
			("containrange", po::value<string>(&fr_vars.containment_window), "Comma separated list of window used for containemtn query")
			("predicate,p", po::value<string>(&fr_vars.predicate), "Predicate for spatial join and nn queries \
[ st_intersects | st_touches | st_crosses | st_contains | st_adjacent | st_disjoint \
| st_equals | st_dwithin | st_within | st_overlaps | st_nearest | st_nearest2 ] ")
			("samplingrate,s", po::value<double>(&fr_vars.sampling_rate), "Sampling rate (0, 1]") 
			("partitioner,u", po::value<string>(&fr_vars.partition_method), "Partitioning method ([fg | bsp \
hc | str | bos | slc | qt ]")
			("partitioner2,v", po::value<string>(&fr_vars.partition_method_2), "(Optional) Partitioning for \
second method [fg | bsp | hc | str | bos | slc | qt ]")
			("mbb1", po::value<string>(&fr_vars.mbb_path_1), "HDFS path to MBBs of data set 1")
			("mbb2", po::value<string>(&fr_vars.mbb_path_2), "HDFS path to MBBs of data set 2")
			("overwrite,o", "Overwrite existing hdfs directories") 
			("parapartition,z", "Use 2 partitioning steps")
			("numreducers,n", po::value<int>(&fr_vars.numreducers), "The number of reducers")
			("removetmp", "Remove temporary directories on HDFS")
			("removembb", "Remove MBB directory on HDFS")
			;
		po::variables_map vm;        
		po::store(po::parse_command_line(argc, argv, desc), vm);
		po::notify(vm);   
		if (vm.count("help") || !vm.count("querytype") ) {
			cerr << desc << endl;
			return false;
		}

		if (vm.count("input2")) {
			fr_vars.join_cardinality = 2;
		}

		// Remove trailing slash in path
		if (fr_vars.output_path.at(fr_vars.output_path.size() - 1) == '/') {
			fr_vars.output_path = fr_vars.output_path.substr(0, fr_vars.output_path.size() - 1);
		}
		if (fr_vars.input_path_1.at(fr_vars.input_path_1.size() - 1) == '/') {
			fr_vars.input_path_1 = fr_vars.input_path_1.substr(0, fr_vars.input_path_1.size() - 1);
		}
		if (vm.count("input2") && fr_vars.input_path_2.at(fr_vars.input_path_2.size() - 1) == '/') {
			fr_vars.input_path_2 = fr_vars.input_path_2.substr(0, fr_vars.input_path_2.size() - 1);
		}

		if (vm.count("parapartition")) {
			fr_vars.para_partition = true;
		}
		if (vm.count("removetmp")) {
			fr_vars.remove_tmp_dirs = true;
		}
		if (vm.count("removembb")) {
			fr_vars.remove_tmp_mbb = true;
		}

		if (vm.count("partitioner")) {
			if (fr_vars.partition_method != PARTITION_FG
				&& fr_vars.partition_method != PARTITION_BSP	
				&& fr_vars.partition_method != PARTITION_SFC
				&& fr_vars.partition_method != PARTITION_BOS	
				&& fr_vars.partition_method != PARTITION_SLC
				&& fr_vars.partition_method != PARTITION_QT	
				&& fr_vars.partition_method != PARTITION_STR) {
				cerr << "Invalid partitioner. Run --help" << endl;
				return false;
			}	
		} else {
			fr_vars.partition_method = PARTITION_BSP;
		}
		if (vm.count("partitioner2")) {
			if (fr_vars.partition_method_2 != PARTITION_FG
				&& fr_vars.partition_method_2 != PARTITION_BSP	
				&& fr_vars.partition_method_2 != PARTITION_SFC
				&& fr_vars.partition_method_2 != PARTITION_BOS	
				&& fr_vars.partition_method_2 != PARTITION_SLC
				&& fr_vars.partition_method_2 != PARTITION_QT	
				&& fr_vars.partition_method_2 != PARTITION_STR) {
				cerr << "Invalid partitioner for step 2. Run --help" << endl;
				return false;
			}	
		} else {
			fr_vars.partition_method_2 = PARTITION_BSP;
		}

		if (vm.count("overwrite")) {
			fr_vars.overwritepath = true;	
		}

		if (fr_vars.query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
			if (vm.count("containfile")) { 
				fr_vars.containment_use_file = true;	
			} else if (vm.count("containrange")) {
				fr_vars.containment_use_file = false;	
			} else {
				cerr << "Missing range information for query. Run --help" << endl;
				return false;
			}
		}


		/* Check for parameter completeness */	
		if (!vm.count("mbb1")) {
			fr_vars.mbb_path_1 = "";
		}
		if (!vm.count("mbb2")) {
			fr_vars.mbb_path_2 = "";
		}

		/* Update environment variables */
		fr_vars.hadoopldlibpath = update_ld_lib_path();
		fr_vars.hadoopcmdpath = getHadoopCmdPath();
		fr_vars.hdfscmdpath = getHdfsCmdPath();
		fr_vars.streaming_path = getHadoopJarPath();
		fr_vars.hadoopgis_prefix = getenv("HADOOPGIS_BIN_PATH") + SLASH;
		#ifdef DEBUG
		cerr << "Hadoop commands path:" << fr_vars.hadoopcmdpath  << endl;
		#endif


		// Common parameter strings for program inside the same 
		stringstream tmpss;
		tmpss.str("");
		if (vm.count("geom1")) {
			tmpss << " -i " << fr_vars.shp_idx_1 << " -a " << fr_vars.input_path_1;
		}
		if (vm.count("predicate")) {
			tmpss << " -p " << fr_vars.predicate;
		}
		if (vm.count("outputfields")) {
			tmpss << " -f " << fr_vars.output_fields;
		}
		if (vm.count("geom2")) {
			tmpss << " -j " << fr_vars.shp_idx_2 << " -b " << fr_vars.input_path_2;
		}
		fr_vars.sharedparams = tmpss.str();

		#ifdef DEBUG
		cerr << "Shared parameters: " << fr_vars.sharedparams << endl;
		#endif

		// Update number of reducers
		tmpss.str("");
		tmpss << fr_vars.numreducers;
		fr_vars.numreducers_str = tmpss.str();
		#ifdef DEBUG
		cerr << "Number reducers: " << fr_vars.numreducers << endl;
		#endif

	} catch (exception& e) {
		cerr << "error here: " << e.what() << "\n";
		return false;
	} catch (...) {
		cerr << "Exception of unknown type!\n";
		return false;
	}
	std::stringstream tmpss;

	// Setting up hdfs paths for temporary outputs
	tmpss.str("");
	tmpss << fr_vars.output_path << "_tmp";
	fr_vars.tmp_path = tmpss.str();

	tmpss.str("");
	if (fr_vars.query_type.compare(QUERYPROC_PARTITION) == 0) {
		tmpss << fr_vars.output_path  << "/mbb";
	} else if (fr_vars.query_type.compare(QUERYPROC_JOIN) == 0) {
		tmpss << fr_vars.output_path  << "_mbb";
	} else if (fr_vars.query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		tmpss << fr_vars.input_path_1 << "/mbb";
	}
	fr_vars.mbb_output = tmpss.str();
	
	tmpss.str("");
	tmpss << fr_vars.mbb_output << "/0";
	// string mbb_path = mbb_output + "/0";
	fr_vars.mbb_path = tmpss.str();
			
	tmpss.str("");
	tmpss << fr_vars.mbb_output << "/SPACE/part-*";
	//string space_path = "/SPACE/part-*";
	fr_vars.space_path = tmpss.str();
	
	tmpss.str("");
	tmpss << fr_vars.output_path << "_partidx";;
	fr_vars.partitionpath = tmpss.str();
	//string partitionpath = output_path + "_partidx";

	tmpss.str("");
	tmpss << fr_vars.output_path << "_partidx2";
	fr_vars.partitionpath2 = tmpss.str();
	//string partitionpath2 = output_path + "_partidx2";

	tmpss.str("");
	tmpss << fr_vars.partitionpath << "/part*";
	fr_vars.partitionpathout = tmpss.str();
	
	tmpss.str("");
	tmpss << fr_vars.partitionpath2 << "/part*";
	fr_vars.partitionpathout2 = tmpss.str();
	
	tmpss.str("");
	tmpss << fr_vars.output_path << "_stat";
	fr_vars.stat_path = tmpss.str();
	
	tmpss.str("");
	tmpss << fr_vars.output_path << "_joinout";
	fr_vars.joinoutputpath = tmpss.str();

	tmpss.str("");
	tmpss << fr_vars.stat_path << "/part*";
	fr_vars.statpathout = tmpss.str();

	tmpss.str("");
	if (fr_vars.query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		tmpss << fr_vars.input_path_1 << "/data";
	} else {
		tmpss << fr_vars.output_path << "/data";
	}
	fr_vars.datapath = tmpss.str();
	
	tmpss.str("");
	if (fr_vars.query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		tmpss << fr_vars.input_path_1;
	} else {
		tmpss << fr_vars.output_path;
	}
	tmpss << "/partition.idx";
	fr_vars.partidx_final = tmpss.str();

	tmpss.str("");
	if (fr_vars.query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		tmpss << fr_vars.input_path_1;
	} else {
		tmpss << fr_vars.output_path;
	}
	tmpss << "/info.cfg";
	fr_vars.config_final = tmpss.str();

	return true;
}

