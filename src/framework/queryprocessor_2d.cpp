/* 
 * This is the framework controller program. It manages calls to MapReduce and holds results, parameters.
 * This serves as the basic front-end interface
 * */

#include <framework/queryprocessor_2d.hpp>

using namespace std;
namespace po = boost::program_options;

int main(int argc, char** argv) {

	cout.precision(20);
	struct framework_vars fr_vars;

	/* Initialize default values */
	fr_vars.numreducers = 10;
	init_params(fr_vars);

	if (!extract_params(argc, argv, fr_vars)) {
		#ifdef DEBUG
		cerr << "Not valid arguments" << endl;
		#endif
		return 1;
	}
	
	#ifdef DEBUGTIME
	double total_exec_time;
	// Temporary variables/placeholders
	total_exec_time = clock();
	#endif


	/////////////////////////////////////////////
	/* Process query */
	/////////////////////////////////////////////

	/////////////////////
	//  Containment ////
	///////////////////
	if (fr_vars.query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		execute_containment(fr_vars);
	}
	/////////////////////
	//  Partition (for containment) ////
	///////////////////
	else if (fr_vars.query_type.compare(QUERYPROC_PARTITION) == 0) {
		execute_partition(fr_vars);
	}
	//////////////////////////////////////////
	//  Spatial join and nearest neighbor ////
	/////////////////////////////////////////
	else if (fr_vars.query_type.compare(QUERYPROC_JOIN) == 0) {
		execute_spjoin(fr_vars);		
	}

}

#ifdef DEBUGSTAT
// Handle tile statistics
void post_process_stat(char *tmpFile, stringstream &output) {
	string input_line;
	vector<string> fields;
	ifstream inputfile(tmpFile);
	stringstream ss;

	double low[2];
	double high[2];

	int tile_count = 0;
	long obj_count = 0;
	double margins = 0;

	long min_count = LONG_MAX;
	long max_count = 0;

	accumulator_set<double, stats<tag::variance> > acc;
	while(getline(inputfile, input_line)) {
		tokenize(input_line, fields, TAB, true);
		low[0] = stod(fields[1]);
		low[1] = stod(fields[2]);
		high[0] = stod(fields[3]);
		high[1] = stod(fields[4]);
		obj_count = strtol(fields[5].c_str(), NULL, 10);
		margins += (high[0] - low[0]) * (high[1] - low[1]);

		acc(static_cast<int>(obj_count));
		if (max_count < obj_count) {
			max_count = obj_count;
		}
		if (min_count > obj_count) {
			min_count = obj_count;
		}

		fields.clear();
		tile_count++;
	}
	output << tile_count << TAB
	//	<< spinfo.num_objects << TAB
		<< margins << TAB
		<< mean(acc) << TAB
		<< min_count << TAB
		<< max_count << TAB
		<< sqrt(variance(acc));
}
#endif

bool extract_mbb(string programpath, vector<string> &input_paths,
	string output_path, string original_params, struct framework_vars &fr_vars) {
	hdfs_delete(programpath, output_path);
	vector<string> arr_args = {"hadoop", "jar", fr_vars.streaming_path};

	arr_args.push_back("-libjars");
	arr_args.push_back(fr_vars.hadoopgis_prefix + CUSTOM_JAR_REL_PATH);
	arr_args.push_back("-outputformat");
	arr_args.push_back("com.custom.CustomMultiOutputFormat");

	for(vector<string>::iterator it = input_paths.begin(); it != input_paths.end(); ++it) {
		arr_args.push_back("-input");
		arr_args.push_back(*it);
	}

	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(fr_vars.hadoopgis_prefix + MANIPULATE);

	arr_args.push_back("-file");
	arr_args.push_back(fr_vars.hadoopgis_prefix + SPACE_EXTRACTOR);

	arr_args.push_back("-mapper");
	arr_args.push_back(MANIPULATE + " --offset 0" + original_params + " --extract ");

	arr_args.push_back("-reducer");
	arr_args.push_back(SPACE_EXTRACTOR);

	// 2 reducers = 1 for outputting mbb and 1 for outputting spacial_dimension
	arr_args.push_back("-numReduceTasks");
	//arr_args.push_back("2");
	arr_args.push_back(fr_vars.numreducers_str);

	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");

	arr_args.push_back("-cmdenv");
	arr_args.push_back(fr_vars.hadoopldlibpath);

#ifdef DEBUG
	cerr << "Extract MBB program params: ";
	for(vector<string>::iterator it = arr_args.begin(); it != arr_args.end(); ++it) {
		cerr << *it << " ";
	}
	cerr << endl;
#endif

	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
#ifdef DEBUG
			cerr << "Succeeded in extracting MBBs: " << status << endl;
#endif
		} else {
#ifdef DEBUG
			cerr << "Failed in extracting MBBs: " << status << endl;
#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}

/* Extract space dimensions from input MBBs */
/*
bool extract_dims(string programpath, string input_path, string output_path, 
	struct framework_vars &fr_vars) {
	hdfs_delete(programpath, output_path);
	stringstream ss;
	vector<string> arr_args = {"hadoop", "jar", fr_vars.streaming_path};

	arr_args.push_back("-input");
	arr_args.push_back(input_path);
	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(fr_vars.hadoopgis_prefix + SPACE_EXTRACTOR);

	arr_args.push_back("-mapper");
	ss.str("");
	ss << SPACE_EXTRACTOR << " 2";
	arr_args.push_back(ss.str());

	arr_args.push_back("-reducer");
	ss.str("");
	ss << SPACE_EXTRACTOR << " 1";
	arr_args.push_back(ss.str());

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back("1");

	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(fr_vars.hadoopldlibpath);

#ifdef DEBUG
	cerr << "Retrieving space dimension program params: ";
	for(vector<string>::iterator it = arr_args.begin(); it != arr_args.end(); ++it) {
		cerr <<  *it  << " ";
	}
	cerr << endl;
#endif
	int status = 0;
	pid_t childpid;

	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
#ifdef DEBUG
			cerr << "Succeeded in obtaining space dimension: " << status << endl;
#endif
		} else {
#ifdef DEBUG
			cerr << "Failed in obtaining space dimension: " << status << endl;
#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
}
*/


bool partition_data(string programpath, string input_path, 
	string output_path, string partitionmethod, int bucket_size, 
	string sharedparams, int step, double samplerate, struct framework_vars &fr_vars, 
	char *cachefilename, char *cachefilefullpath) {

	hdfs_delete(programpath, output_path);
	stringstream ss;
	vector<string> arr_args = {"hadoop", "jar", fr_vars.streaming_path};

	arr_args.push_back("-input");
	arr_args.push_back(input_path);
	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	if (step == 1) {
		// For first step in partitioning
		arr_args.push_back(fr_vars.hadoopgis_prefix + MBB_SAMPLER);
	} else {
		// For 2nd step in partitioning
		arr_args.push_back(fr_vars.hadoopgis_prefix + MANIPULATE);
	}
	arr_args.push_back("-file");
	arr_args.push_back(fr_vars.hadoopgis_prefix + partitionmethod);

	if (cachefilename != NULL) {
		string strtmp(cachefilefullpath);
		arr_args.push_back("-file");
		arr_args.push_back(strtmp);
	}

	arr_args.push_back("-mapper");
	ss.str("");
	if (step == 1) {
		ss << MBB_SAMPLER << " " << samplerate;

	} else {
		ss << MANIPULATE << " -o 1 " << sharedparams 
			<< " -c " << cachefilename << " -m";
	}
	arr_args.push_back(ss.str());

	arr_args.push_back("-reducer");
	ss.str("");
	bucket_size = max(static_cast<int>(floor(bucket_size * samplerate)), 1);
	ss << partitionmethod << " -b " << bucket_size;
	if (cachefilename != NULL) {
		ss << " -c " << cachefilename;
	}
	arr_args.push_back(ss.str());

	arr_args.push_back("-numReduceTasks");
	if (step == 1) {
		arr_args.push_back("1");
	} else {
		//arr_args.push_back("0");
		arr_args.push_back(fr_vars.numreducers_str);
	}
	//arr_args.push_back(numreducers_str);

	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(fr_vars.hadoopldlibpath);

#ifdef DEBUG
	cerr << "Partitioning params: ";
	for(vector<string>::iterator it = arr_args.begin(); it != arr_args.end(); ++it) {
		cerr << *it << " ";
	}
	cerr << endl;
#endif

	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
#ifdef DEBUG
			cerr << "Succeeded in partitioning " << status << endl;
#endif
		} else {
#ifdef DEBUG
			cerr << "Failed in partitioning: " << status << endl;
#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
}


bool collect_stat(string programpath, string input_path, string output_path, 
	string sharedparams, struct framework_vars &fr_vars, 
	char *cachefilename, char *cachefilefullpath) {
	hdfs_delete(programpath, output_path);
	stringstream ss;
	vector<string> arr_args = {"hadoop", "jar", fr_vars.streaming_path};

	arr_args.push_back("-conf");
	arr_args.push_back("/home/vhoang/mapred-site.xml");
	
	arr_args.push_back("-input");
	arr_args.push_back(input_path);
	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(fr_vars.hadoopgis_prefix + STAT_COLLECT_MAPPER);
	arr_args.push_back("-file");
	arr_args.push_back(fr_vars.hadoopgis_prefix + STAT_COLLECT_REDUCER);

	string strtmp(cachefilefullpath);
	arr_args.push_back("-file");
	arr_args.push_back(strtmp);

	arr_args.push_back("-mapper");
	ss.str("");
	ss << STAT_COLLECT_MAPPER << " -o 1 " << sharedparams
			<< " -c " << cachefilename;
	arr_args.push_back(ss.str());

	arr_args.push_back("-reducer");
	arr_args.push_back(STAT_COLLECT_REDUCER);

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back(fr_vars.numreducers_str);

	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(fr_vars.hadoopldlibpath);

	#ifdef DEBUG
	cerr << "Collecting stats";
	for(vector<string>::iterator it = arr_args.begin(); it != arr_args.end(); ++it) {
		cerr << *it << " ";
	}
	cerr << endl;
	#endif

	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
			#ifdef DEBUG
			cerr << "Succeeded in collecting stats MBB: " << status << endl;
			#endif
		} else {
			#ifdef DEBUG
			cerr << "Failed in collecting stats " << status << endl;
			#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
}

bool duplicate_removal(string programpath, string input_path, string output_path, 
	struct framework_vars &fr_vars) {
	vector<string> arr_args = {"hadoop", "jar", fr_vars.streaming_path};
	arr_args.push_back("-input");
	arr_args.push_back(input_path);

	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(fr_vars.hadoopgis_prefix + DUPLICATE_REMOVER);

	arr_args.push_back("-mapper");
	arr_args.push_back(DUPLICATE_REMOVER + " cat");

	//arr_args.push_back("-reducer None");
	arr_args.push_back("-reducer");
	arr_args.push_back(DUPLICATE_REMOVER + " uniq");
	// should also work: 
	// arr_args.push_back("cat");

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back(fr_vars.numreducers_str);
	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(fr_vars.hadoopldlibpath);

#ifdef DEBUG
	cerr << "Removing duplicate program params: ";
	for(vector<string>::iterator it = arr_args.begin(); it != arr_args.end(); ++it) {
		cerr << *it << " ";
	}
	cerr << endl;
#endif

	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
#ifdef DEBUG
			cerr << "Succeeded in boundary handling: " << status << endl;
#endif
		} else {
#ifdef DEBUG
			cerr << "Failed in boundary handling: " << status << endl;
#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}

