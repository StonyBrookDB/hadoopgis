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
#include <climits>
#include <boost/program_options.hpp>

#include "../common/string_constants.h"
#include "../common/tokenizer.h"
#include "../common/resque_constants.h"
#include "../common/queryproc_constants.h"
#include "../common/partition_structs.h"
#include "../common/queryproc_aux.h"

#ifdef DEBUGSTAT
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/variance.hpp>

/*
 * The main program that handles hadoopgis pipeline
 *
 */

using namespace boost::accumulators;
#endif

using namespace std;
namespace po = boost::program_options;

string getHadoopJarPath();
string getHdfsCmdPath();
string getHadoopCmdPath();
string hdfs_str_result(string programpath, vector<string> &arr_args);
bool hdfs_check_data(string hadoopcmdpath, string input_path);
bool hdfs_cat(string hadoopcmdpath, string path, int outputfd = -1);
long hdfs_get_size(string programpath, string inputpath);
bool hdfs_delete(string hadoopcmdpath, string path);
bool hdfs_move(string programpath, string source, string destination);
bool hdfs_put(string programpath, string source, string destination);
bool extract_dims(string programpath, string input_path, string output_path);
bool extract_mbb(string programpath, vector<string> &input_paths,
	string output_path, string original_params);
bool normalize_mbb(string hadoopcmdpath, string input_path, string output_path, 
	bool denormalize = false);
bool partition_data(string programpath, string inputpath, 
	string output_path, string partitionmethod, int bucket_size, 
	string sharedparams, int step, double samplerate,
	char *cachefilename = NULL, char *cachefilefullpath = NULL);
bool collect_stat(string hadoopcmdpath, string input_path, string output_path, 
	string sharedparams, char *tmpnameonly, char *tmpFile);
bool partition_obj(string programpath, vector<string> &input_paths, 
	string output_path, string original_params, 
	char *cachefilename, char *cachefilefullpath);
bool sp_join(string programpath, vector<string> &input_paths, 
	string output_path, string original_params, 
	char *cachefilename, char *cachefilefullpath);
bool sp_containment(string programpath, vector<string> input_paths, string output_path,
	int shp_idx_1, char *cachefilename = NULL, char *cachefilefullpath = NULL);
bool duplicate_removal(string hadoopcmdpath, string input_path, string output_path);
bool update_ld_lib_path();
pid_t execute_command(string programpath,vector<string> &strargs, int outputfd = -1);

// Extern functions
extern void get_mbb(string tmp_line, double *low, double *high);
extern string get_wkt_from_mbb(double *low, double *high);
extern bool intersects(double *low, double *high, double *wlow, double *whigh);

#ifdef DEBUGSTAT
void post_process_stat(char *tmpFile, stringstream &ss);
#endif

// MapReduce variables
string hadoopgis_prefix;
int numreducers;
string numreducers_str;
string hadoopldlibpath;
string streaming_path; // Absolute path to the hadoop streaming jar
string hadoopcmdpath; // Absolute path to the command line hadoop executable
string hdfscmdpath; // Absolute path to the command line hadoop executable

struct space_info {
	long num_objects;
	double space_low[2];
	double space_high[2];
	double total_size;
} spinfo;

struct flow_info {
	int pipefd[2];
	int join_cardinality;
} flinfo;

char nametemplate[] = "/tmp/hadoopgisXXXXXX";

// Return the absolute path to where the streaming jar is located
string getHadoopJarPath() {
	stringstream ss;
	ss <<  getenv("HADOOP_STREAMING_PATH") << SLASH << JAR_FILE_NAME;
	return ss.str();
}

// Return the absolute path to where the command line for hadoop is located
string getHadoopCmdPath() {
	stringstream ss;
	ss << getenv("HADOOP_HOME") << "/bin/hadoop";
	return ss.str();
}

// Return the absolute path to where the command line for hdfs is located
string getHdfsCmdPath() {
	stringstream ss;
	ss << getenv("HADOOP_HOME") << "/bin/hdfs";
	return ss.str();
}

void addConfSettings(stringstream &ss, string key, string value) {
	ss << " -D" << key << "=" << value << " ";
}

bool update_ld_lib_path() {
	stringstream tmpss;
	#ifdef DEBUG
	cerr << "Updating lib path" << endl;
	#endif
	const char *ld_path;

	char *appended_ld_path = getenv("HADOOPGIS_LIB_PATH");

	if (!appended_ld_path) {
		return false; // fail to obtain Hadoop path
	}
	char *current_ld = getenv("LD_LIBRARY_PATH");
	if (current_ld) {
		// LD_LIBRARY_PATH has been set
		tmpss << current_ld << ":" << appended_ld_path;
		ld_path = tmpss.str().c_str();
	} else {
		// LD_LIBRARY_PATH has not been set
		ld_path = appended_ld_path;
	}
	setenv("LD_LIBRARY_PATH", ld_path, 1);
	stringstream ss;
	ss << "LD_LIBRARY_PATH=" << ld_path;
	hadoopldlibpath = ss.str();
	return true;
}

/* Obtain the first string returned by the command */
string hdfs_str_result(string programpath, vector<string> &arr_args) {
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
			#ifdef DEBUG
			cerr << "Succeeded in obtaining result: " << status << endl;
			#endif
		} else {
			#ifdef DEBUG
			cerr << "Failed in obtaining hdfs block size "  << " and " << "status is: " << status << endl;
			#endif
			exit(1);
		}
		char result[1024];
		FILE *instr = fdopen(flinfo.pipefd[0], "r");
		fscanf(instr, "%s", &result);
		fclose(instr);
		return string(result);
	}
	return "";
}

/* Given if the path exists  */
bool hdfs_check_data(string programpath, string input_path) {
	vector<string> arr_args = {"hadoop", "fs", "-test", "-e", input_path};
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
			#ifdef DEBUG
			cerr << "Succeeded and status is: " << status << endl;
			#endif
		} else {
			#ifdef DEBUG
			cerr << "Failed and status is: " << status << endl;
			#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}

/* Delete the existing directory in hdfs */
bool hdfs_delete(string programpath, string path) {
	vector<string> arr_args = {"hadoop", "fs", "-rm", "-r", path};
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
			#ifdef DEBUG
			cerr << "Succeeded in deleting " << path << "and status is: " << status << endl;
			#endif
		} else {
			#ifdef DEBUG
			cerr << "Failed in deleting " << path << "and status is: " << status << endl;
			#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}

/* Renaming/Moving file in hdfs */
bool hdfs_move(string programpath, string source, string destination) {
	vector<string> arr_args = {"hadoop", "fs", "-mv", source, destination};
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
			#ifdef DEBUG
			cerr << "Succeeded in moving " << source << " to " 
				<< destination << ": " << status << endl;
			#endif
		} else {
			#ifdef DEBUG
			cerr << "Failed in moving " << source << " to " 
				<< destination << ": " << status << endl;
			#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}

/* Put/move a file from local disk to hdfs */
bool hdfs_put(string programpath, string source, string destination) {
	vector<string> arr_args = {"hadoop", "fs", "-put", source, destination};
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
			#ifdef DEBUG
			cerr << "Succeeded in putting " << source << " to " 
				<< destination << ": " << status << endl;
			#endif
		} else {
			#ifdef DEBUG
			cerr << "Failed in putting " << source << " to " 
				<< destination << ": " << status << endl;
			#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}

/* Cat text file stored in hdfs */
bool hdfs_cat(string programpath, string path, int outputfd) {
	vector<string> arr_args = {"hadoop", "fs", "-cat", path};
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args, outputfd)) {
		if (wait(&status)) {
			#ifdef DEBUG
			cerr << "Succeeded in hcat and status is: " << status << endl;
			#endif

		} else {
			#ifdef DEBUG
			cerr << "Failed in hcat and status is: " << status << endl;
			#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}

/* Obtain size in bytes for the current file/directory */
long hdfs_get_size(string programpath, string inputpath) {
	vector<string> arr_args = {"hadoop", "fs", "-du", "-s", inputpath};
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
			#ifdef DEBUG
			cerr << "Succeeded in hdfs dfs -du and status is: " << status << endl;
			#endif
			// Finished
			long totalsize = -1;

			FILE *instr = fdopen(flinfo.pipefd[0], "r");
			fscanf(instr, "%ld", &totalsize);
			fclose(instr);
			return totalsize;

		} else {
			#ifdef DEBUG
			cerr << "Failed in hdfs dfs -du and status is: " << status << endl;
			#endif
			exit(1);
		}
	}
	return -1;
}

/* Extract object MBB and collect the dimension statistics */
bool extract_mbb(string programpath, vector<string> &input_paths, 
		string output_path, string original_params) {
	hdfs_delete(programpath, output_path);
	vector<string> arr_args = {"hadoop", "jar", streaming_path};

	arr_args.push_back("-libjars");
	arr_args.push_back(hadoopgis_prefix + CUSTOM_JAR_REL_PATH);
	arr_args.push_back("-outputformat");
	arr_args.push_back("com.custom.CustomMultiOutputFormat");

	for(vector<string>::iterator it = input_paths.begin(); it != input_paths.end(); ++it) {
		arr_args.push_back("-input");
		arr_args.push_back(*it);
	}

	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + MBB_EXTRACTOR);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + SPACE_EXTRACTOR);

	arr_args.push_back("-mapper");
	arr_args.push_back(MBB_EXTRACTOR + " --offset 0" + original_params + " --extract ");

	arr_args.push_back("-reducer");
	arr_args.push_back(SPACE_EXTRACTOR);

	// 2 reducers = 1 for outputting mbb and 1 for outputting spacial_dimension
	arr_args.push_back("-numReduceTasks");
	//arr_args.push_back("2");
	arr_args.push_back(numreducers_str);

	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");

	arr_args.push_back("-cmdenv");
	arr_args.push_back(hadoopldlibpath);

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

bool extract_dims(string programpath, string input_path, string output_path) {
	hdfs_delete(programpath, output_path);
	stringstream ss;
	vector<string> arr_args = {"hadoop", "jar", streaming_path};

	arr_args.push_back("-input");
	arr_args.push_back(input_path);
	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + SPACE_EXTRACTOR);

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
	arr_args.push_back(hadoopldlibpath);

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

bool normalize_mbb(string programpath, string input_path, string output_path, bool denormalize) {
	hdfs_delete(programpath, output_path);
	stringstream ss;
	vector<string> arr_args = {"hadoop", "jar", streaming_path};

	arr_args.push_back("-input");
	arr_args.push_back(input_path);
	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + MBB_NORMALIZER);

	arr_args.push_back("-mapper");
	ss.str("");
	ss << MBB_NORMALIZER 
		<< " -a " << spinfo.space_low[0]
		<< " -b " << spinfo.space_low[1]
		<< " -c " << spinfo.space_high[0]
		<< " -d " << spinfo.space_high[1]
		<< " -f 1";
	if (!denormalize) {
		ss << " -n";
	} else {
		ss << " -o";
	}
	arr_args.push_back(ss.str());

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back("0");

	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(hadoopldlibpath);
	if (denormalize) {
		arr_args.push_back("-jobconf");
		arr_args.push_back("mapred.min.split.size=10000000");
	}

#ifdef DEBUG
	cerr << "Normalizing program params: ";
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
			cerr << "Succeeded in normalizing MBB: " << status << endl;
#endif
		} else {
#ifdef DEBUG
			cerr << "Failed in normalizing MBB: " << status << endl;
#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
}

bool partition_data(string programpath, string input_path, 
		string output_path, string partitionmethod, int bucket_size, 
		string sharedparams, int step, double samplerate,
		char *cachefilename, char *cachefilefullpath) {
	hdfs_delete(programpath, output_path);
	stringstream ss;
	vector<string> arr_args = {"hadoop", "jar", streaming_path};

	arr_args.push_back("-input");
	arr_args.push_back(input_path);
	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	if (step == 1) {
		// For first step in partitioning
		arr_args.push_back(hadoopgis_prefix + MBB_SAMPLER);
	} else {
		// For 2nd step in partitioning
		arr_args.push_back(hadoopgis_prefix + MBB_EXTRACTOR);
	}
	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + partitionmethod);

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
		ss << MBB_EXTRACTOR << " -o 1 " << sharedparams 
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
		arr_args.push_back(numreducers_str);
	}
	//arr_args.push_back(numreducers_str);

	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(hadoopldlibpath);

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
		string sharedparams, char *cachefilename, char *cachefilefullpath) {
	hdfs_delete(programpath, output_path);
	stringstream ss;
	vector<string> arr_args = {"hadoop", "jar", streaming_path};

	arr_args.push_back("-conf");
	arr_args.push_back("/home/vhoang/mapred-site.xml");
	
	arr_args.push_back("-input");
	arr_args.push_back(input_path);
	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + STAT_COLLECT_MAPPER);
	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + STAT_COLLECT_REDUCER);

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
	arr_args.push_back(numreducers_str);

	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(hadoopldlibpath);

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

bool partition_obj(string programpath, vector<string> &input_paths, 
		string output_path, string original_params, 
		char *cachefilename, char *cachefilefullpath) {
	hdfs_delete(programpath, output_path);
	vector<string> arr_args = {"hadoop", "jar", streaming_path};

	arr_args.push_back("-libjars");
	arr_args.push_back(hadoopgis_prefix + CUSTOM_JAR_REL_PATH);
	arr_args.push_back("-outputformat");
	arr_args.push_back("com.custom.CustomSingleOutputFormat");

	//arr_args.push_back("-conf");
	//arr_args.push_back("/home/vhoang/mapred-site.xml");

	for(vector<string>::iterator it = input_paths.begin(); it != input_paths.end(); ++it) {
		arr_args.push_back("-input");
		arr_args.push_back(*it);
	}

	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + MBB_EXTRACTOR);
	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + DUPLICATE_REMOVER);
	arr_args.push_back("-file");
	string strtmp(cachefilefullpath);
	arr_args.push_back(strtmp);


	arr_args.push_back("-mapper");
	stringstream ss;
	ss << MBB_EXTRACTOR << " -o 0" << original_params
		<< " -c " << cachefilename << " -h"; // -h does not append join index to the object
	arr_args.push_back(ss.str());

	arr_args.push_back("-reducer");
	arr_args.push_back(DUPLICATE_REMOVER + " cat");
	//arr_args.push_back("cat");

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back(numreducers_str);
	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(hadoopldlibpath);

	#ifdef DEBUG
	cerr << "Executing mapping objects to partitions params: ";
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
			cerr << "Succeeded in mapping object to partitions: " << status << endl;
			#endif
		} else {
			#ifdef DEBUG
			cerr << "Failed in mapping objects to partitions: " << status << endl;
			#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}
bool sp_containment(string programpath, vector<string> input_paths, string output_path,
	int geom_idx, char *cachefilename, char *cachefilefullpath) {
	hdfs_delete(programpath, output_path);
	vector<string> arr_args = {"hadoop", "jar", streaming_path};
	//arr_args.push_back("-conf");
	//arr_args.push_back("/home/vhoang/mapred-site.xml");
	for(vector<string>::iterator it = input_paths.begin(); it != input_paths.end(); ++it) {
		arr_args.push_back("-input");
		arr_args.push_back(*it);
	}

	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + CONTAINMENT_PROC);
	arr_args.push_back("-file");
	string strtmp(cachefilefullpath);
	arr_args.push_back(strtmp);

	arr_args.push_back("-mapper");
	stringstream ss;
	ss << CONTAINMENT_PROC << " -o 0 -i " << geom_idx << " -c " << cachefilename;
	arr_args.push_back(ss.str());

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back("0");
	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(hadoopldlibpath);

	#ifdef DEBUG
	cerr << "Executing containment params: ";
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
			cerr << "Succeeded in containment: " << status << endl;
			#endif
		} else {
			#ifdef DEBUG
			cerr << "Failed in containment: " << status << endl;
			#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}

bool sp_join(string programpath, vector<string> &input_paths, 
		string output_path, string original_params, 
		char *cachefilename, char *cachefilefullpath) {
	hdfs_delete(programpath, output_path);
	vector<string> arr_args = {"hadoop", "jar", streaming_path};
	//arr_args.push_back("-conf");
	//arr_args.push_back("/home/vhoang/mapred-site.xml");
	for(vector<string>::iterator it = input_paths.begin(); it != input_paths.end(); ++it) {
		arr_args.push_back("-input");
		arr_args.push_back(*it);
	}

	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + MBB_EXTRACTOR);
	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + RESQUE);
	arr_args.push_back("-file");
	string strtmp(cachefilefullpath);
	arr_args.push_back(strtmp);

	arr_args.push_back("-mapper");
	stringstream ss;
	ss << MBB_EXTRACTOR << " -o 0" << original_params
		<< " -c " << cachefilename;
	arr_args.push_back(ss.str());

	arr_args.push_back("-reducer");
	arr_args.push_back(RESQUE + " -o 2" + original_params); // Offset to account for tile id and join index
	//arr_args.push_back("cat");

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back(numreducers_str);
	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(hadoopldlibpath);

#ifdef DEBUG
	cerr << "Executing spjoin program params: ";
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
			cerr << "Succeeded in sp join: " << status << endl;
#endif
		} else {
#ifdef DEBUG
			cerr << "Failed in sp join: " << status << endl;
#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}

bool duplicate_removal(string programpath, string input_path, string output_path) {
	hdfs_delete(programpath, output_path);
	vector<string> arr_args = {"hadoop", "jar", streaming_path};
	arr_args.push_back("-input");
	arr_args.push_back(input_path);

	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + DUPLICATE_REMOVER);

	arr_args.push_back("-mapper");
	arr_args.push_back(DUPLICATE_REMOVER + " cat");

	//arr_args.push_back("-reducer None");
	arr_args.push_back("-reducer");
	arr_args.push_back(DUPLICATE_REMOVER + " uniq");
	// should also work: 
	// arr_args.push_back("cat");

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back(numreducers_str);
	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(hadoopldlibpath);

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

// Execute a command with arguments stored in strargs. Optionally, the standard output
// is redirected to outputfd file descriptor
pid_t execute_command(string programpath, vector<string> &strargs, int outputfd) {
	pipe(flinfo.pipefd);
	pid_t childpid;

	if ((childpid = fork()) == -1) {
		#ifdef DEBUG
		cerr << "Fork failed" << endl;
		#endif
		perror("fork");
		exit(1);
	}
	if (childpid) {
		// Parent
		// Close the not used pipe
		close(flinfo.pipefd[1]);

		// Close standard input
		//	close(STDIN_FILENO);

		// Parent will read the result from pipefd[0]
		//	dup2(pipefd[0], STDIN_FILENO);
		//	close(pipefd[0]);
		#ifdef DEBUG
		cerr << "Child pid is " << childpid << endl;	
		#endif
		if (outputfd != -1) {
			close(outputfd);
		}
		return childpid;
	} else {
		// Child
		close(flinfo.pipefd[0]);

		close(STDOUT_FILENO);
		// close(STDERR_FILENO); // could suppress standard error as well

		// Pipe the output back to parent process

		if (outputfd == -1) {
			dup2(flinfo.pipefd[1], STDOUT_FILENO);
		} else {
			dup2(outputfd, STDOUT_FILENO);
			close(outputfd);
		}
		close(flinfo.pipefd[1]);

		// Converting arguments into char **
		char * args[strargs.size()];
		int i;
		for (i = 0; i < strargs.size(); i++) {
			args[i] = (char *) strargs[i].c_str();

			#ifdef DEBUG
			cerr << args[i] << SPACE;
			#endif
		}
		#ifdef DEBUG
		cerr << endl;
		#endif
		args[i] = (char *) NULL;

		// Execute the program
		execvp(programpath.c_str(), args);

		// Error if reached here
		#ifdef DEBUG
		cerr << "Fork failed" << endl;
		#endif
		perror("execl");
		exit(1);
	}
	return 0;
}


int main(int argc, char** argv)
{
	cout.precision(20);
	
	#ifdef DEBUGTIME
	double total_exec_time;
	#endif

	// Input data variables	
	string input_path_1;
	string input_path_2;
	string mbb_path_1;
	string mbb_path_2;
	int shp_idx_1;
	int shp_idx_2;

	// Query variables
	string output_fields;
	string output_path;
	string query_type;
	string predicate;
	double distance;
	bool overwritepath = false;
	bool containment_use_file = false; // True == use file, false == use string from command line
	string user_file;
	string containment_window;

	// Partitioning variables
	string partition_method = "bsp";
	string partition_method_2 = "bsp";
	bool para_partition = false; // 1 or 2 steps in partitioning
	bool remove_tmp_dirs = false;
	bool remove_tmp_mbb = false;
	double sampling_rate = 1.0; // to be changed for different sampling method
	int bucket_size = -1;
	long block_size = -1;
	int rough_bucket_size = -1;

	// Temporary variables/placeholders
	FILE *instr = NULL;
	FILE *outstr = NULL;
	string dummystr;

	string sharedparams;
	string partitioningparams;

	#ifdef DEBUGTIME
	total_exec_time = clock();
	#endif

	/* Initialize default values */
	numreducers = 10;
	flinfo.join_cardinality = 1;

	stringstream tmpss;
	
	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "This help message")
			("querytype,q", po::value<string>(&query_type), "Query type [ partition | contaiment | spjoin]")
			("bucket,p", po::value<int>(&bucket_size), "Fine-grain level tile size for spjoin")
			("blocksize", po::value<long>(&block_size), "Fine-grain level bucket size for partitioning (loading data)")
			("roughbucket", po::value<int>(&rough_bucket_size), "Rough level bucket size for partitioning")
			("input1,a", po::value<string>(&input_path_1), "HDFS file path to data set 1")
			("input2,b", po::value<string>(&input_path_2), "HDFS file path to data set 2")
			("mbb1", po::value<string>(&mbb_path_1), "HDFS path to MBBs of data set 1")
			("mbb2", po::value<string>(&mbb_path_2), "HDFS path to MBBs of data set 2")
			("geom1,i", po::value<int>(&shp_idx_1), "Field number of data set 1 containing the geometry")
			("geom2,j", po::value<int>(&shp_idx_2), "Field number of data set 2 containing the geometry")
			("distance,d", po::value<double>(&distance), "Distance (used for certain predicates)")
			("outputfields,f", po::value<string>(&output_fields), "Fields to be included in the final output \
separated by commas. See the full documentation. Regular fields from datasets are in the format datasetnum:fieldnum, e.g. 1:1,2:3,1:14. \
Field counting starts from 1. Optional statistics include: area1, area2, union, intersect, jaccard, dice, mindist")
			("outputpath,h", po::value<string>(&output_path), "Output path")
			("containfile", po::value<string>(&user_file), "User file containing window used for containment query")
			("containrange", po::value<string>(&containment_window), "Comma separated list of window used for containemtn query")
			("predicate,t", po::value<string>(&predicate), "Predicate for spatial join and nn queries \
[ st_intersects | st_touches | st_crosses | st_contains | st_adjacent | st_disjoint \
| st_equals | st_dwithin | st_within | st_overlaps | st_nearest | st_nearest2 ] ")
			("samplingrate,s", po::value<double>(&sampling_rate), "Sampling rate (0, 1]") 
			("partitioner,u", po::value<string>(&partition_method), "Partitioning method ([fg | bsp \
hc | str | bos | slc | qt ]")
			("partitioner2,v", po::value<string>(&partition_method_2), "(Optional) Partitioning for \
second method [fg | bsp | hc | str | bos | slc | qt ]")
			("overwrite,o", "Overwrite existing hdfs directories") 
			("parapartition,z", "Use 2 partitioning steps")
			("numreducers,n", po::value<int>(&numreducers), "The number of reducers")
			("removetmp", "Remove temporary directories on HDFS")
			("removembb", "Remove MBB directory on HDFS")
			;
		po::variables_map vm;        
		po::store(po::parse_command_line(argc, argv, desc), vm);
		po::notify(vm);   
		if (vm.count("help") || !vm.count("querytype") ) {
			cerr << desc << endl;
			return 0;
		}

		if (vm.count("input2")) {
			flinfo.join_cardinality = 2;
		}

		// Remove trailing slash in path
		if (output_path.at(output_path.size() - 1) == '/') {
			output_path = output_path.substr(0, output_path.size() - 1);
		}
		if (input_path_1.at(input_path_1.size() - 1) == '/') {
			input_path_1 = input_path_1.substr(0, input_path_1.size() - 1);
		}
		if (vm.count("input2") && input_path_2.at(input_path_2.size() - 1) == '/') {
			input_path_2 = input_path_2.substr(0, input_path_2.size() - 1);
		}

		if (vm.count("parapartition")) {
			para_partition = true;
		}
		if (vm.count("removetmp")) {
			remove_tmp_dirs = true;
		}
		if (vm.count("removembb")) {
			remove_tmp_mbb = true;
		}

		if (vm.count("partitioner")) {
			if (partition_method != PARTITION_FG
				&& partition_method != PARTITION_BSP	
				&& partition_method != PARTITION_SFC
				&& partition_method != PARTITION_BOS	
				&& partition_method != PARTITION_SLC
				&& partition_method != PARTITION_QT	
				&& partition_method != PARTITION_STR) {
				cerr << "Invalid partitioner. Run --help" << endl;
				return 1;
			}	
		} else {
			partition_method = PARTITION_BSP;
		}
		if (vm.count("partitioner2")) {
			if (partition_method_2 != PARTITION_FG
				&& partition_method_2 != PARTITION_BSP	
				&& partition_method_2 != PARTITION_SFC
				&& partition_method_2 != PARTITION_BOS	
				&& partition_method_2 != PARTITION_SLC
				&& partition_method_2 != PARTITION_QT	
				&& partition_method_2 != PARTITION_STR) {
				cerr << "Invalid partitioner for step 2. Run --help" << endl;
				return 1;
			}	
		} else {
			partition_method = PARTITION_BSP;
		}

		if (vm.count("overwrite")) {
			overwritepath = true;	
		}

		if (query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
			if (vm.count("containfile")) { 
				containment_use_file = true;	
			} else if (vm.count("containrange")) {
				containment_use_file = false;	
			} else {
				cerr << "Missing range information for query. Run --help" << endl;
				return 1;
			}
		}


		/* Check for parameter completeness */	

		if(!update_ld_lib_path()) {
			#ifdef DEBUG
			cerr << "Cannot set LD_LIBRARY_PATH for subprocesses" << endl;
			#endif
			return 1;
		}

		if (!vm.count("mbb1")) {
			mbb_path_1 = "";
		}
		if (!vm.count("mbb2")) {
			mbb_path_2 = "";
		}

		/* Update environment variables */
		hadoopcmdpath = getHadoopCmdPath();
		hdfscmdpath = getHdfsCmdPath();
		streaming_path = getHadoopJarPath();
		hadoopgis_prefix = getenv("HADOOPGIS_BIN_PATH") + SLASH;
		#ifdef DEBUG
		cerr << "Hadoop commands path:" << hadoopcmdpath  << endl;
		#endif


		// Common parameter strings for program inside the same 
		tmpss.str("");
		if (vm.count("geom1")) {
			tmpss << " -i " << shp_idx_1 << " -a " << input_path_1;
		}
		if (vm.count("predicate")) {
			tmpss << " -p " << predicate;
		}
		if (vm.count("outputfields")) {
			tmpss << " -f " << output_fields;
		}
		if (vm.count("geom2")) {
			tmpss << " -j " << shp_idx_2 << " -b " << input_path_2;
		}
		sharedparams = tmpss.str();

		#ifdef DEBUG
		cerr << "Shared parameters: " << sharedparams << endl;
		#endif

		// Update number of reducers
		tmpss.str("");
		tmpss << numreducers;
		numreducers_str = tmpss.str();
		#ifdef DEBUG
		cerr << "Number reducers: " << numreducers << endl;
		#endif

	} catch (exception& e) {
		cerr << "error here: " << e.what() << "\n";
		return 1;
	} catch (...) {
		cerr << "Exception of unknown type!\n";
		return 1;
	}

	string input_line;
	long size_1 = -1;
	long size_2 = -1;

	double obtain_size_1 = 0;
	double obtain_size_2 = 0;
	bool loaded_1 = false;
	bool loaded_2 = false;

	// Setting up hdfs paths for temporary outputs
	tmpss.str("");
	tmpss << output_path << "_tmp";
	string tmp_path = tmpss.str();

	tmpss.str("");
	if (query_type.compare(QUERYPROC_PARTITION) == 0) {
		tmpss << output_path  << "/mbb";
	} else if (query_type.compare(QUERYPROC_JOIN) == 0) {
		tmpss << output_path  << "_mbb";
	} else if (query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		tmpss << input_path_1 << "/mbb";
	}
	string mbb_output = tmpss.str();
	
	tmpss.str("");
	tmpss << mbb_output << "/0";
	// string mbb_path = mbb_output + "/0";
	string mbb_path = tmpss.str();
			
	tmpss.str("");
	tmpss << mbb_output << "/SPACE/part-*";
	//string space_path = "/SPACE/part-*";
	string space_path = tmpss.str();
	
	tmpss.str("");
	tmpss << output_path << "_partidx";;
	string partitionpath = tmpss.str();
	//string partitionpath = output_path + "_partidx";

	tmpss.str("");
	tmpss << output_path << "_partidx2";
	string partitionpath2 = tmpss.str();
	//string partitionpath2 = output_path + "_partidx2";

	tmpss.str("");
	tmpss << partitionpath << "/part*";
	string partitionpathout = tmpss.str();
	
	tmpss.str("");
	tmpss << partitionpath2 << "/part*";
	string partitionpathout2 = tmpss.str();
	
	tmpss.str("");
	tmpss << output_path << "/part*";
	string stat_path = tmpss.str();
	
	tmpss.str("");
	tmpss << output_path << "_joinout";
	string joinoutputpath = tmpss.str();

	tmpss.str("");
	tmpss << stat_path << "/part*";
	string statpathout = tmpss.str();

	tmpss.str("");
	if (query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		tmpss << input_path_1 << "/data";
	} else {
		tmpss << output_path << "/data";
	}
	string datapath = tmpss.str();
	
	tmpss.str("");
	if (query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		tmpss << input_path_1;
	} else {
		tmpss << output_path;
	}
	tmpss << "/partition.idx";
	string partidx_final = tmpss.str();

	tmpss.str("");
	if (query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		tmpss << input_path_1;
	} else {
		tmpss << output_path;
	}
	tmpss << "/info.cfg";
	string config_final = tmpss.str();

	/* Process query */
	if (query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		#ifdef DEBUG
		cerr << "Executing a containment query" << endl;
		#endif
		
		// Check if the partidx file exists
		if (!hdfs_check_data(hadoopcmdpath, partidx_final)
			|| !hdfs_check_data(hadoopcmdpath, config_final)) {
			#ifdef DEBUG
			cerr << "Data was not loaded" << endl;
			#endif
			exit(1);
		}

		if (!hdfs_cat(hadoopcmdpath, config_final)) {
			#ifdef DEBUG
			cerr << "Error reading from pipe" << endl;
			#endif
			exit(1);
			
		}

		FILE *instr = fdopen(flinfo.pipefd[0], "r");
		fscanf(instr, "%d %lf %lf %lf %lf %ld", &shp_idx_1,
			&(spinfo.space_low[0]), &(spinfo.space_low[1]),
			&(spinfo.space_high[0]), &(spinfo.space_high[1]), 
			&(spinfo.num_objects));
		fclose(instr);

		// Retrieve window query
		double low[2];
		double high[2];
		double wlow[2];
		double whigh[2];
		string tmp_line;
		if (containment_use_file) {
			ifstream inputfile(user_file.c_str());
			getline(inputfile, tmp_line);
			get_mbb(tmp_line, wlow, whigh);
			inputfile.close();
		}  else {
			get_mbb(containment_window, wlow, whigh);
		}
		tmp_line = get_wkt_from_mbb(wlow, whigh);
		#ifdef DEBUG
		cerr << "Window range: " << TAB << wlow[0] << TAB << wlow[1] 
				<< TAB << whigh[0] << TAB << whigh[1] << endl;
		#endif

		if (!hdfs_cat(hadoopcmdpath, partidx_final)) {
			#ifdef DEBUG
			cerr << "Error reading partition files from pipe" << endl;
			#endif
			exit(1);
		}

		char tile_id[1024];
		vector<string> input_paths;
		instr = fdopen(flinfo.pipefd[0], "r");
		while (fscanf(instr,"%s %lf %lf %lf %lf", tile_id, &(low[0]), &(low[1]),
			&(high[0]), &(high[1])) != EOF) {

			// If the MBB of tile intersects with MBB of the window query
			if (intersects(low, high, wlow, whigh)) {
				tmpss.str("");
				tmpss << datapath << SLASH << tile_id << ".dat";
				input_paths.push_back(tmpss.str());
			}

			#ifdef DEBUG
			cerr << tile_id << TAB << low[0] << TAB << low[1] 
				<< TAB << high[0] << TAB << high[1] << endl;
			#endif
		}	
		fclose(instr);

		char *tmpFile = mktemp(nametemplate);
		ofstream ofs(tmpFile, ofstream::trunc);
		ofs << tmp_line << endl; // the file contains the WKT string of the window
		char *tmpnameonly = strrchr(tmpFile, '/'); // pointing to just the name of the cache file
		tmpnameonly++;
		ofs.close();

		if (overwritepath || !hdfs_check_data(hadoopcmdpath, output_path)) {
			#ifdef DEBUG
			cerr << "Executing containment" << endl;
			#endif
			if (!sp_containment(hadoopcmdpath, input_paths, output_path, shp_idx_1,
				tmpnameonly, tmpFile)) {
				#ifdef DEBUG
				cerr << "Containment failed." << endl;
				#endif
				exit(1);
			}
		}

	}
	else if (query_type.compare(QUERYPROC_PARTITION) == 0) {
		obtain_size_1 = hdfs_get_size(hadoopcmdpath, input_path_1);
		if (obtain_size_1 >= 0) {
			spinfo.total_size = obtain_size_1;
		}
		#ifdef DEBUG
		cerr << "Total size of 1 " << obtain_size_1 << endl;
		if (flinfo.join_cardinality > 1) {
			cerr << "Total size of 2 " << obtain_size_2 << endl;
		}
		cerr << "Total object size: " << spinfo.total_size << endl;
		cerr << "Data 1 is loaded " << (loaded_1 ? "true" : "false" ) << endl;
		cerr << "Data 2 is loaded " << (loaded_2 ? "true" : "false" ) << endl;
		#endif

		vector<string> inputpaths;
		inputpaths.push_back(input_path_1);

		// Extract object MBB and grab space dimension
		if (overwritepath || !hdfs_check_data(hadoopcmdpath, mbb_output)) {
			#ifdef DEBUG
			cerr << "\n\nExtracting MBBs\n" << endl;
			#endif
			if (!extract_mbb(hadoopcmdpath, inputpaths,
					mbb_output, sharedparams)) {
				cerr << "Failed to extract MBB" << endl;
			}
		}


		char *tmpFile = mktemp(nametemplate);
		int tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
		char *tmpnameonly = strrchr(tmpFile, '/'); // pointing to just the name of the cache file
		tmpnameonly++; // Advance the pointer past the slash delimiter character
		#ifdef DEBUG
		cerr << "Temp file name only: " << tmpnameonly << endl;	
		#endif
		// Obtain the cache file from hdfs
		bool res_cat = hdfs_cat(hadoopcmdpath, space_path, tmpfd);
		close(tmpfd);

		#ifdef DEBUG
		ifstream instr(tmpFile);
		if (instr.is_open()) {
			instr >> dummystr
				>> spinfo.space_low[0] >> spinfo.space_low[1] 
				>> spinfo.space_high[0] >> spinfo.space_high[1]
				>> spinfo.num_objects;
			instr.close();
		}
		cerr << "Space dimensions: " << spinfo.space_low[0] << TAB 
			<< spinfo.space_low[1] << TAB
			<< spinfo.space_high[0] << TAB << spinfo.space_high[1] 
			<< endl;
		cerr << "Number objects: " << spinfo.num_objects << endl;
		#endif	

		// Setting default block size if it has not been set
		if (bucket_size < 0) {
			if (block_size <= 0) {
				vector<string> conf_args = {"hdfs", "getconf", "-confKey", "dfs.blocksize"};
				block_size = stoi(hdfs_str_result(hdfscmdpath, conf_args), NULL, 10);
			}
			// Bucket size was not set
			bucket_size = max(static_cast<int>(floor(block_size
							/ spinfo.total_size * spinfo.num_objects)), 1);
		}

		// 1st step of partition the data to generate tile boundaries
		#ifdef DEBUG
		cerr << "Bucket size: " << bucket_size << endl;
		cerr << "Sampling rate: " << sampling_rate << endl;
		#endif
		if (!para_partition) {
			rough_bucket_size = bucket_size;
			if (overwritepath || !hdfs_check_data(hadoopcmdpath, partitionpath)) {
				#ifdef DEBUG
				cerr << "\n\nPartitioning (1st step)\n" << endl;
				#endif
				if (!partition_data(hadoopcmdpath, mbb_path, 
						partitionpath, partition_method, bucket_size, 
						sharedparams, 1, sampling_rate, tmpnameonly, tmpFile)) {
					cerr << "Failed partitioning 1st step" << endl;
					// Remove cache file
					remove(tmpFile);
					exit(1);
				}
			}
		} else {
			if (rough_bucket_size == -1) {
				// Rough bucket size has not been set
				if (numreducers > 0) {
					rough_bucket_size = max(static_cast<int>(floor(spinfo.num_objects 
									/ numreducers / 2)), 1);
				} else {
					rough_bucket_size = max(static_cast<int>(floor(spinfo.num_objects 
									/ 2)), 1);
				}	
			}
			cerr << "Rough bucket size: " << rough_bucket_size << endl;
			if (overwritepath || !hdfs_check_data(hadoopcmdpath, partitionpath)) {
#ifdef DEBUG
				cerr << "\n\nPartitioning 1st step\n" << endl;
#endif
				if (!partition_data(hadoopcmdpath, mbb_path, 
						partitionpath, partition_method, rough_bucket_size, 
						sharedparams, 1, sampling_rate, tmpnameonly, tmpFile)) {
					cerr << "Failed partitioning 1st step" << endl;
					// Remove cache file
					remove(tmpFile);
					exit(1);
				}
			}
		}
		// Obtain the cache file from hdfs
		tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
		bool res_partition = hdfs_cat(hadoopcmdpath, partitionpathout, tmpfd);
		close(tmpfd);

#ifdef DEBU
		cerr << "Temp file name to hold partition boundary: " << tmpFile << endl;
#endif

		if (para_partition) {
			// Second round of partitioning
#ifdef DEBUG
			cerr << "\n\nPartitioning 2nd steps\n" << endl;
#endif
			if (overwritepath || !hdfs_check_data(hadoopcmdpath, partitionpath2)) {
				if (!partition_data(hadoopcmdpath, mbb_path, 
						partitionpath2, partition_method_2, bucket_size, 
						sharedparams, 2, 1, tmpnameonly, tmpFile)) {
					cerr << "Failed partitioning 2nd step" << endl;
					// Remove cache file
					remove(tmpFile);
					exit(1);
				}
			}
			// Update/Overwrite the tmp file	
			tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
			bool res_partition_2 = hdfs_cat(hadoopcmdpath, partitionpathout2, tmpfd);
			close(tmpfd);

		}

		// tmpFile contains partition index
		#ifdef DEBUGSTAT
		//string stat_path = output_path + "_stat";
		if (overwritepath || !hdfs_check_data(hadoopcmdpath, stat_path)) {
			if (!collect_stat(hadoopcmdpath, mbb_path, stat_path, 
					sharedparams, tmpnameonly, tmpFile)) {
				cerr << "Failed obtaining stats" << endl;
				// Remove cache file
				remove(tmpFile);
				exit(1);
			}
		}
		cerr << "Done collecting tile counts" << endl;

		// Overwrite the cache file
		tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
		hdfs_cat(hadoopcmdpath, statpathout, tmpfd);
		close(tmpfd);

		stringstream outputss;
		outputss << (para_partition ? "true" : "false") << TAB 
			<< partition_method << TAB
			<< rough_bucket_size << TAB 
			<< sampling_rate << TAB 
			<< partition_method_2 << TAB
			<< bucket_size << TAB
			<< spinfo.num_objects << TAB;
		post_process_stat(tmpFile, outputss);
		cout << outputss.str() << endl;
		#else

		// Mapping objects to physical partition
		if (overwritepath || !hdfs_check_data(hadoopcmdpath, datapath)) {
			#ifdef DEBUG
			cerr << "\n\nMapping objects to partitions/Creating physical partitions\n" << endl;
			#endif
			if (!partition_obj(hadoopcmdpath, inputpaths, 
				datapath, sharedparams, tmpnameonly, tmpFile)) {
				cerr << "Failed mapping object to partitions"  << endl;
				remove(tmpFile);		
				exit(1);
			}
		}
		
		cerr << "Done with loading objects to partition." << endl;
		hdfs_delete(hadoopcmdpath, partidx_final);
		if (para_partition) {
			hdfs_move(hadoopcmdpath, partitionpathout2, partidx_final);
			hdfs_delete(hadoopcmdpath, partitionpathout2);
		} else {
			hdfs_move(hadoopcmdpath, partitionpathout, partidx_final);
			hdfs_delete(hadoopcmdpath, partitionpathout);
		}

		ofstream ofs(tmpFile, ofstream::trunc);
		ofs << shp_idx_1 << TAB << spinfo.space_low[0] 
				<< TAB << spinfo.space_low[1]
				<< TAB << spinfo.space_high[0]
				<< TAB << spinfo.space_high[1]
				<< TAB <<  spinfo.num_objects << endl;
		ofs.close();
		hdfs_put(hadoopcmdpath, string(tmpFile), config_final);
		cerr << "Done copying global indices to " << partidx_final << endl;

		#endif
		// Removing cache file
		remove(tmpFile);		
		
	}
	else if ( query_type.compare(QUERYPROC_JOIN) == 0) {
		// First check if dataset 1 has been loaded or not

		// loaded_1 = hdfs_check_data(hadoopcmdpath, input_path_1 + "/" + PARTITION_FILE_NAME);
		obtain_size_1 = hdfs_get_size(hadoopcmdpath, input_path_1);
		if (obtain_size_1 >= 0) {
			spinfo.total_size = obtain_size_1;
		}

		// If there is a 2nd dataset, also check if it has been loaded and obtain its size
		if (flinfo.join_cardinality > 1) {
			// loaded_2 = hdfs_check_data(hadoopcmdpath, input_path_2 + "/" + PARTITION_FILE_NAME);
			obtain_size_2 = hdfs_get_size(hadoopcmdpath, input_path_2);
			if (obtain_size_2 >= 0) {
				spinfo.total_size += obtain_size_2;
			}
		}
		#ifdef DEBUG
		cerr << "Total size of 1 " << obtain_size_1 << endl;
		if (flinfo.join_cardinality > 1) {
			cerr << "Total size of 2 " << obtain_size_2 << endl;
		}
		cerr << "Total object size: " << spinfo.total_size << endl;
		cerr << "Data 1 is loaded " << (loaded_1 ? "true" : "false" ) << endl;
		cerr << "Data 2 is loaded " << (loaded_2 ? "true" : "false" ) << endl;
		#endif

		vector<string> inputpaths;

		// Neither data has been indexed
		// string mbb_output = output_path + "_mbb";	
		inputpaths.push_back(input_path_1);
		if (flinfo.join_cardinality > 1) {
			inputpaths.push_back(input_path_2);
		}

		// Extract object MBB and grab space dimension
		if (overwritepath || !hdfs_check_data(hadoopcmdpath, mbb_output)) {
			#ifdef DEBUG
			cerr << "\n\nExtracting MBBs\n" << endl;
			#endif
			if (!extract_mbb(hadoopcmdpath, inputpaths,
					mbb_output, sharedparams)) {
				cerr << "Failed extracting MBB"  << endl;
				exit(1);
			}

		}			
		/* This was achieved by the extract_mbb
		// Obtain object dimensions
		string spacedimension = mbb_output + "_dim";
		if (overwritepath || !hdfs_check_data(hadoopcmdpath, spacedimension)) {
		bool obtained_dims = extract_dims(hadoopcmdpath,
		mbb_output, spacedimension);
		}
		*/

		char *tmpFile = mktemp(nametemplate);
		int tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
		char *tmpnameonly = strrchr(tmpFile, '/'); // pointing to just the name of the cache file
		tmpnameonly++; // Advance the pointer past the slash delimiter character
		#ifdef DEBUG
		cerr << "Temp file name only: " << tmpnameonly << endl;	
		#endif
		// Obtain the cache file from hdfs
		bool res_cat = hdfs_cat(hadoopcmdpath, space_path, tmpfd);
		close(tmpfd);

		#ifdef DEBUG
		ifstream instr(tmpFile);
		if (instr.is_open()) {
			instr >> dummystr
				>> spinfo.space_low[0] >> spinfo.space_low[1] 
				>> spinfo.space_high[0] >> spinfo.space_high[1]
				>> spinfo.num_objects;
			instr.close();
		}
		cerr << "Space dimensions: " << spinfo.space_low[0] << TAB 
			<< spinfo.space_low[1] << TAB
			<< spinfo.space_high[0] << TAB << spinfo.space_high[1] 
			<< endl;
		cerr << "Number objects: " << spinfo.num_objects << endl;
		#endif	

		// Setting default block size if it has not been set
		if (bucket_size < 0) {
			// Bucket size was not set
			double blockSize = 16000000; // approximately 16MB
			bucket_size = max(static_cast<int>(floor(blockSize
							/ spinfo.total_size * spinfo.num_objects)), 1);
		}

		// 1st step of partition the data to generate tile boundaries
		#ifdef DEBUG
		cerr << "Bucket size: " << bucket_size << endl;
		cerr << "Sampling rate: " << sampling_rate << endl;
		cerr << "\n\nPartitioning (1st step)\n" << endl;
		#endif
		if (!para_partition) {
			rough_bucket_size = bucket_size;
			if (overwritepath || !hdfs_check_data(hadoopcmdpath, partitionpath)) {
				#ifdef DEBUG
				cerr << "\n\nPartitioning 2nd steps\n" << endl;
				#endif
				if (!partition_data(hadoopcmdpath, mbb_path, 
						partitionpath, partition_method, bucket_size, 
						sharedparams, 1, sampling_rate, tmpnameonly, tmpFile)) {
					cerr << "Failed partitioning 1st step" << endl;
					// Remove cache file
					remove(tmpFile);
					exit(1);
				}
			}
		} else {
			if (rough_bucket_size == -1) {
				// Rough bucket size has not been set
				if (numreducers > 0) {
					rough_bucket_size = max(static_cast<int>(floor(spinfo.num_objects 
									/ numreducers / 2)), 1);
				} else {
					rough_bucket_size = max(static_cast<int>(floor(spinfo.num_objects 
									/ 2)), 1);
				}	
			}
			cerr << "Rough bucket size: " << rough_bucket_size << endl;
			if (overwritepath || !hdfs_check_data(hadoopcmdpath, partitionpath)) {
#ifdef DEBUG
				cerr << "\n\nPartitioning 1st step\n" << endl;
#endif
				if (!partition_data(hadoopcmdpath, mbb_path, 
						partitionpath, partition_method, rough_bucket_size, 
						sharedparams, 1, sampling_rate, tmpnameonly, tmpFile)) {
					cerr << "Failed partitioning 1st step" << endl;
					// Remove cache file
					remove(tmpFile);
					exit(1);
				}
			}
		}
		// Obtain the cache file from hdfs
		tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
		bool res_partition = hdfs_cat(hadoopcmdpath, partitionpathout, tmpfd);
		close(tmpfd);

#ifdef DEBU
		cerr << "Temp file name to hold partition boundary: " << tmpFile << endl;
#endif

		if (para_partition) {
			// Second round of partitioning
#ifdef DEBUG
			cerr << "\n\nPartitioning 2nd steps\n" << endl;
#endif
			if (overwritepath || !hdfs_check_data(hadoopcmdpath, partitionpath2)) {
				if (!partition_data(hadoopcmdpath, mbb_path, 
						partitionpath2, partition_method_2, bucket_size, 
						sharedparams, 2, 1, tmpnameonly, tmpFile)) {
					cerr << "Failed partitioning 2nd step" << endl;
					// Remove cache file
					remove(tmpFile);
					exit(1);
				}
			}
			// Update/Overwrite the tmp file	
			tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
			bool res_partition_2 = hdfs_cat(hadoopcmdpath, partitionpathout2, tmpfd);
			close(tmpfd);

		}

		// tmpFile contains partition index
		#ifdef DEBUGSTAT
		//string stat_path = output_path + "_stat";
		if (overwritepath || !hdfs_check_data(hadoopcmdpath, stat_path)) {
			if (!collect_stat(hadoopcmdpath, mbb_path, stat_path, 
					sharedparams, tmpnameonly, tmpFile)) {
				cerr << "Failed obtaining stats" << endl;
				// Remove cache file
				remove(tmpFile);
				exit(1);
			}
		}
		cerr << "Done collecting tile counts" << endl;

		// Overwrite the cache file
		tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
		hdfs_cat(hadoopcmdpath, statpathout, tmpfd);
		close(tmpfd);

		stringstream outputss;
		outputss << (para_partition ? "true" : "false") << TAB 
			<< partition_method << TAB
			<< rough_bucket_size << TAB 
			<< sampling_rate << TAB 
			<< partition_method_2 << TAB
			<< bucket_size << TAB
			<< spinfo.num_objects << TAB;
		post_process_stat(tmpFile, outputss);
		cout << outputss.str() << endl;
		
		#else

		// Spatial join step
		if (overwritepath || !hdfs_check_data(hadoopcmdpath, joinoutputpath)) {
			#ifdef DEBUG
			cerr << "\n\nExecuting spatial joins\n" << endl;
			#endif
			if (!sp_join(hadoopcmdpath, inputpaths, 
					joinoutputpath, sharedparams, tmpnameonly, tmpFile)) {
				cerr << "Failed spatial join" << endl;
				// Remove cache file
				remove(tmpFile);
				exit(1);
			}
		}
		cerr << "Done with spatial join." << endl;
;
		// Perform duplicate removal

		if (overwritepath || !hdfs_check_data(hadoopcmdpath, output_path)) {
			#ifdef DEBUG
			cerr << "\n\nBoundary object handling\n" << endl;
			#endif
			if (!duplicate_removal(hadoopcmdpath, 
					joinoutputpath, output_path)) {
				cerr << "Failed boundary handling" << endl;
				// Remove cache file
				remove(tmpFile);
				exit(1);
			}
		}
		cerr << "Done with boundary handling. Results are stored at "
			<< output_path << endl;

		#endif
		cerr << "Cleaning up/Removing temporary directories" << endl;
		remove(tmpFile);
		if (remove_tmp_dirs) {
			hdfs_delete(hadoopcmdpath, partitionpath);
			if (para_partition) {
				hdfs_delete(hadoopcmdpath, partitionpath2);
			}
			hdfs_delete(hadoopcmdpath, joinoutputpath);
		}
		if (remove_tmp_mbb) {
			hdfs_delete(hadoopcmdpath, mbb_output);
			
		}
	}

	#ifdef DEBUGTIME
	total_exec_time = clock() - total_exec_time;
	cerr << "Total execution time: " 
		<< (double) total_exec_time / CLOCKS_PER_SEC 
		<< " seconds." << endl;
	#endif

	cout.flush();
	cerr.flush();
	return 0;
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
		<< spinfo.num_objects << TAB
		<< margins << TAB
		<< mean(acc) << TAB
		<< min_count << TAB
		<< max_count << TAB
		<< sqrt(variance(acc));
}
#endif
