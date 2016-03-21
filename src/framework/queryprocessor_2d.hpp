
#include <iostream>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <getopt.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <fcntl.h>

#include <boost/program_options.hpp>

#include <progparams/string_constants.h>
#include <utilities/tokenizer.h>
#include <utilities/queryprocessor_aux.h>

#ifdef DEBUGSTAT
	#include <boost/accumulators/accumulators.hpp>
	#include <boost/accumulators/statistics/stats.hpp>
	#include <boost/accumulators/statistics/mean.hpp>
	#include <boost/accumulators/statistics/variance.hpp>
	using namespace boost::accumulators;
#endif

// MapReduce-C++ interaction
#include <mapreducecpp/mapreduce_cpp.hpp>

#include <progparams/queryprocessor_params_2d.hpp>


bool extract_params(int argc, char **argv, struct framework_vars &fr_vars);

bool extract_dims(string programpath, string input_path, string output_path, struct framework_vars &fr_vars);
bool extract_mbb(string programpath, vector<string> &input_paths,
  string output_path, string original_params, struct framework_vars &fr_vars);
 
bool partition_data(string programpath, string input_path, 
	string output_path, string partitionmethod, int bucket_size, 
	string sharedparams, int step, double samplerate, struct framework_vars &fr_vars, 
	char *cachefilename = NULL, char *cachefilefullpath = NULL);
 
bool collect_stat(string hadoopcmdpath, string input_path, string output_path, 
	string sharedparams, struct framework_vars &fr_vars, char *tmpnameonly, char *tmpFile);
 
bool partition_obj(string programpath, vector<string> &input_paths, 
	string output_path, string original_params, struct framework_vars &fr_vars,
	char *cachefilename, char *cachefilefullpath);
 
bool sp_join(string programpath, vector<string> &input_paths, 
	string output_path, string original_params, struct framework_vars &fr_vars, 
	char *cachefilename, char *cachefilefullpath);
 
bool sp_containment(string programpath, vector<string> input_paths, 
  string output_path, string original_params, struct framework_vars &fr_vars, 
	char *cachefilename = NULL, char *cachefilefullpath = NULL);
bool duplicate_removal(string hadoopcmdpath, string input_path, 
  string output_path, struct framework_vars &fr_vars);
 
bool execute_containment(struct framework_vars &fr_vars);

bool execute_spjoin(struct framework_vars &fr_vars);

bool execute_partition(struct framework_vars &fr_vars);

#ifdef DEBUGSTAT
void post_process_stat(char *tmpFile, stringstream &ss);
#endif

#include <framework/query_containment.hpp>
#include <framework/query_partition.hpp>
#include <framework/query_spjoin.hpp>
