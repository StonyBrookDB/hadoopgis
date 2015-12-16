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
#include "../common/string_constants.h"
#include "../common/tokenizer.h"
#include "../common/resque_constants.h"
#include "../common/queryproc3d_constants.h"


/*
 * Query Controller for 3D
 *
 */
using namespace std;
namespace po = boost::program_options;

string getHadoopJarPath();
string getHadoopCmdPath();
bool hdfs_check_data(string hadoopcmdpath, string input_path);
bool hdfs_cat(string hadoopcmdpath, string path, int outputfd = -1);
long hdfs_get_size(string programpath, string inputpath);
bool hdfs_delete(string hadoopcmdpath, string path);
bool extract_dims(string programpath, string input_path, string output_path);
bool extract_mbb(string programpath, vector<string> &input_paths,
	string output_path, string original_params);
bool normalize_mbb(string hadoopcmdpath, string input_path, string output_path, 
		bool denormalize = false);
bool partition_data(string programpath, string inputpath, 
		string output_path, string partitionmethod, int bucket_size);
bool sp_join(string programpath, vector<string> &input_paths, 
		string output_path, string original_params, 
		char *cachefilename, char *cachefilefullpath);
bool duplicate_removal(string hadoopcmdpath, string input_path, string output_path);
bool update_ld_lib_path();
pid_t execute_command(string programpath,vector<string> &strargs, int outputfd = -1);


// MapReduce variables
string hadoopgis_prefix;
string streaming_path; // Absolute path to the hadoop streaming jar
string hadoopcmdpath; // Absolute path to the command line hadoop executable
string hadoopldlibpath;
bool overwritepath = false;
int numreducers;
string numreducers_str;
struct space_info {
	long num_objects;
	double space_min_x;
	double space_min_y;
	double space_min_z;
	double space_max_x;
	double space_max_y;
	double space_max_z;
	double total_size;
} spinfo;

struct flow_info {
	int pipefd[2];
	int join_cardinality;
} flinfo;

char nametemplate[] = "/tmp/hadoopgisXXXXXX";

// Return the absolute path to where the streaming jar is located
string getHadoopJarPath() {
	return getenv("HADOOP_STREAMING_PATH") + SLASH + JAR_FILE_NAME;
}

// Return the absolute path to where the command line for hadoop is located
string getHadoopCmdPath() {
	return getenv("HADOOP_HOME") + string("/bin/hadoop");
}

void addConfSettings(stringstream &ss, string key, string value) {
	ss << " -D" << key << "=" << value << " ";
}

bool update_ld_lib_path() {
	/* Updating LD_LIBRARY_ATH */
	stringstream tmpss;
	cerr << "Updating lib path" << endl;
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

/* Given if the data is loaded, i.e. there exists an index file  */
bool hdfs_check_data(string hadoopcmdpath, string input_path) {
	vector<string> arr_args = {"hadoop", "fs", "-test", "-e", input_path};
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(hadoopcmdpath, arr_args)) {
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
		if (status) {
			// The file doesn't exist
			return false;
		} else {
			// The file does exist
			return true;
		}
	}
	return false;
}

/* Delete the existing directory in hdfs */
bool hdfs_delete(string hadoopcmdpath, string path) {
	vector<string> arr_args = {"hadoop", "fs", "-rm", "-r", path};
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(hadoopcmdpath, arr_args)) {
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
		return status ? true : false; // this is the same as return status
	}
	return false;
}
/* Delete the existing directory in hdfs */
bool hdfs_cat(string hadoopcmdpath, string path, int outputfd) {
	vector<string> arr_args = {"hadoop", "fs", "-cat", path};
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(hadoopcmdpath, arr_args, outputfd)) {
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
		return status ? true : false; // this is the same as return status
	}
	return false;
}


long hdfs_get_size(string programpath, string inputpath) {
	vector<string> arr_args = {"hadoop", "fs", "-du", "-s", inputpath};
	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(hadoopcmdpath, arr_args)) {
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


bool extract_mbb(string programpath, vector<string> &input_paths, 
			string output_path, string original_params) {
	hdfs_delete(programpath, output_path);
	vector<string> arr_args = {"hadoop", "jar", streaming_path};
	arr_args.push_back("-libjars");
	arr_args.push_back(hadoopgis_prefix + CUSTOM_JAR_REL_PATH);
	arr_args.push_back("-inputformat");
	arr_args.push_back("com.custom.WholeFileInputFormat");
	for(vector<string>::iterator it = input_paths.begin(); it != input_paths.end(); ++it) {
		arr_args.push_back("-input");
		arr_args.push_back(*it);
	}

	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + MBB_EXTRACTOR);

	arr_args.push_back("-mapper");
	//arr_args.push_back("cat");
	arr_args.push_back("" + MBB_EXTRACTOR + " -o 0" + original_params + " -x ");
	

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back("0");
	arr_args.push_back("-jobconf");
	arr_args.push_back("mapred.task.timeout=36000000");
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
	if (childpid = execute_command(hadoopcmdpath, arr_args)) {
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
		return status;
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
	arr_args.push_back("mapred.task.timeout=36000000");
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

	if (childpid = execute_command(hadoopcmdpath, arr_args)) {
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
		return status ? true : false; // this is the same as return status
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
		<< " --min_x  " << spinfo.space_min_x
		<< " --min_y " << spinfo.space_min_y
		<< " --min_z " << spinfo.space_min_z
		<< " --max_x " << spinfo.space_max_z
		<< " --max_y " << spinfo.space_max_y
		<< " --max_z " << spinfo.space_max_z
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
	arr_args.push_back("mapred.task.timeout=36000000");
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
	if (childpid = execute_command(hadoopcmdpath, arr_args)) {
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
		return status ? true : false; // this is the same as return status
	}
}

bool partition_data(string programpath, string input_path, 
		string output_path, string partitionmethod, int bucket_size) {
	hdfs_delete(programpath, output_path);
	stringstream ss;
	vector<string> arr_args = {"hadoop", "jar", streaming_path};

	arr_args.push_back("-input");
	arr_args.push_back(input_path);
	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + DUPLICATE_REMOVER);
	arr_args.push_back("-file");
	arr_args.push_back(hadoopgis_prefix + partitionmethod);

	arr_args.push_back("-mapper");
	ss << DUPLICATE_REMOVER << " cat";
	arr_args.push_back(ss.str());

	arr_args.push_back("-reducer");
	ss.str("");
	if (partitionmethod == PARTITION_FG) {
		ss << partitionmethod 
			<< " --min_x " << spinfo.space_min_x
			<< " --min_y " << spinfo.space_min_y
			<< " --min_z " << spinfo.space_min_z
			<< " --max_x " << spinfo.space_max_x
			<< " --max_y " << spinfo.space_max_y
			<< " --max_z " << spinfo.space_max_z
			<< " --bucket " << bucket_size;
		arr_args.push_back(ss.str());
	} else {
		ss << partitionmethod << " -b " << bucket_size;
		arr_args.push_back(ss.str());
	}

	
	arr_args.push_back("-numReduceTasks");
	arr_args.push_back("1");

	arr_args.push_back("-jobconf");
	arr_args.push_back("mapred.task.timeout=36000000");
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
	if (childpid = execute_command(hadoopcmdpath, arr_args)) {
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
		return status ? true : false; // this is the same as return status
	}
}


bool sp_join(string programpath, vector<string> &input_paths, 
			string output_path, string original_params, 
		char *cachefilename, char *cachefilefullpath) {
	hdfs_delete(programpath, output_path);
	vector<string> arr_args = {"hadoop", "jar", streaming_path};
	arr_args.push_back("-libjars");
	arr_args.push_back(hadoopgis_prefix + CUSTOM_JAR_REL_PATH);
	arr_args.push_back("-inputformat");
	arr_args.push_back("com.custom.WholeFileInputFormat");
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
	stringstream ss2;

	// offset = 2 to account for tile and join index
	ss2 << RESQUE << " -o 2" << original_params
		<< " -c " << cachefilename;
	arr_args.push_back(ss2.str());
	//arr_args.push_back("cat");

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back(numreducers_str);
	arr_args.push_back("-jobconf");
	arr_args.push_back("mapred.task.timeout=36000000");
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
	if (childpid = execute_command(hadoopcmdpath, arr_args)) {
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
		return status;
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
	// should also work: 
	
	//arr_args.push_back("-reducer None");
	arr_args.push_back("-reducer");
	arr_args.push_back(DUPLICATE_REMOVER + " uniq");
	// should also work: 
	// arr_args.push_back("cat");

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back(numreducers_str);
	arr_args.push_back("-jobconf");
	arr_args.push_back("mapred.task.timeout=36000000");
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
	if (childpid = execute_command(hadoopcmdpath, arr_args)) {
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
		return status;
	}
	return false;
}

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

			cerr << args[i] << SPACE;
		}
		cerr << endl;
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

/* 
 *
 *
 * */

int main(int argc, char** argv)
{
	#ifdef DEBUGTIME
	double total_exec_time;
	#endif


	// Input data variables	
	string input_path_1;
	string input_path_2;
	
	// Query variables
	string output_fields;
	string output_path;
	string query_type;
	string predicate;
	double distance;

	// Partitioning variables
	string partition_method = "fg3d";
	double sampling_rate = 1.0; // to be changed for different sampling method
	int bucket_size = -1;

	// Temporary variables/placeholders
	FILE *instr = NULL;
	FILE *outstr = NULL;

	string sharedparams;
	string partitioningparams;

	#ifdef DEBUGTIME
	total_exec_time = clock();
	#endif

	/* Initialize default values */
	numreducers = 10;
	flinfo.join_cardinality = 1;

	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "This help message")
			("numreducers,n", po::value<int>(&numreducers), "The number of reducers")
			("bucket,p", po::value<int>(&bucket_size), "Bucket size for partitioning")
			("input1,a", po::value<string>(&input_path_1), "HDFS file path to data set 1")
			("input2,b", po::value<string>(&input_path_2), "HDFS file path to data set 2")
			("distance,d", po::value<double>(&distance), "Distance (used for certain predicates)")
			("outputfields,f", po::value<string>(&output_fields), "Fields to be output. See the full documentation.")
			("outputpath,h", po::value<string>(&output_path), "Output path")
			("predicate,t", po::value<string>(&predicate), "Predicate for spatial join and nn queries")
			("querytype,q", po::value<string>(&query_type), "Query type [contaiment | spjoin]")
			("samplingrate,s", po::value<double>(&sampling_rate), "Sampling rate (0, 1]") 
			("partitioner,u", po::value<string>(&partition_method), "Partitioning method ([fg3d | bsp3d | hc3d | str3d | bos3d ]") 
			("overwrite,o", "Overwrite existing hdfs directory.") 
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


		if (vm.count("partitioner")) {
			if (partition_method != PARTITION_FG
				&& partition_method != PARTITION_BSP	
				&& partition_method != PARTITION_SFC
				&& partition_method != PARTITION_BOS	
				&& partition_method != PARTITION_STR) {
				cerr << "Invalid partitioner. Run --help" << endl;
				return 1;
			}	
		} else {
			partition_method = PARTITION_BSP;
		}

		if (vm.count("overwrite")) {
			overwritepath = true;	
		}

		#ifdef DEBUG
		cerr << "Number reducers: " << numreducers << endl;
		#endif
		/* Check for parameter completeness */	

		if(!update_ld_lib_path()) {
			#ifdef DEBUG
			cerr << "Cannot set LD_LIBRARY_PATH for subprocesses" << endl;
			#endif
			return 1;
		}

		/* Update environment variables */
		hadoopcmdpath = getHadoopCmdPath();
		streaming_path = getHadoopJarPath();
		hadoopgis_prefix = getenv("HADOOPGIS_BIN_PATH") + SLASH;
		#ifdef DEBUG
		cerr << "Hadoop commands path:" << hadoopcmdpath  << endl;
		#endif


		// Common parameter strings for program inside the same 
		stringstream tmpss;
		tmpss << " -a " << input_path_1 << " -p " << predicate;
		if (vm.count("outputfields")) {
			tmpss << " -f " << output_fields;
		}
		if (vm.count("input2")) {
			tmpss  << " -b " << input_path_2;
		}
		sharedparams = tmpss.str();

		if (vm.count("samplingrate")) {
			tmpss << " -q " << sampling_rate;
			partitioningparams = tmpss.str();
		} else {
			partitioningparams = sharedparams;
		}

		#ifdef DEBUG
		cerr << "Shared parameters: " << sharedparams << endl;
		cerr << "Partitioning parameters: " << partitioningparams << endl;
		#endif
		tmpss.str("");
		tmpss << numreducers;
		numreducers_str = tmpss.str();

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

	bool loaded_1 = false;
	bool loaded_2 = false;

	/* Process query */
	if (!query_type.compare(QUERYPROC_CONTAINMENT)) {
		#ifdef DEBUG
		cerr << "Executing a containment query" << endl;
		#endif
	} 	
	else if (!query_type.compare(QUERYPROC_JOIN)) {
		#ifdef DEBUG
		cerr << "Executing a join query" << endl;
		#endif

		// First check if dataset 1 has been loaded or not

		bool loaded_1 = hdfs_check_data(hadoopcmdpath, input_path_1 + "/" + PARTITION_FILE_NAME);
		double obtain_size_1 = hdfs_get_size(hadoopcmdpath, input_path_1);
		if (obtain_size_1 >= 0) {
			spinfo.total_size = obtain_size_1;
		}
		double obtain_size_2 = 0;	
		// If there is a 2nd dataset, also check if it has been loaded and obtain its size
		if (flinfo.join_cardinality > 1) {
			loaded_2 = hdfs_check_data(hadoopcmdpath, input_path_2 + "/" + PARTITION_FILE_NAME);
			obtain_size_2 = hdfs_get_size(hadoopcmdpath, input_path_2);
			if (obtain_size_2 >= 0) {
				spinfo.total_size += obtain_size_2;
			}
		}
		#ifdef DEBUG
		cerr << "Total size of 1 " << obtain_size_1 << endl;
		cerr << "Total size of 2 " << obtain_size_2 << endl;
		cerr << "Total object size: " << spinfo.total_size << endl;
		#endif

		#ifdef DEBUG
		cerr << "Data 1 is loaded " << (loaded_1 ? "true" : "false" ) << endl;
		cerr << "Data 2 is loaded " << (loaded_2 ? "true" : "false" ) << endl;
		#endif
		/*
		while (cin && getline(cin, input_line) && !cin.eof()) {
			cout << "Output: " << input_line << endl;
		}*/
		vector<string> inputpaths;

		if (!loaded_1 && !loaded_2) {
			// Neither data has been indexed
		
			string mbb_output = output_path + "_mbb";	
			if (flinfo.join_cardinality > 1) {
				inputpaths = {input_path_1, input_path_2};
			} else {
				inputpaths = {input_path_1};
			}
			// Extract object MBB
			if (overwritepath || !hdfs_check_data(hadoopcmdpath, mbb_output)) {
				bool extracted = extract_mbb(hadoopcmdpath, inputpaths,
					output_path + "_mbb", partitioningparams);			

			} else {
				// do nothing and advance to the next step
			}			
				

			// Obtain object dimensions
			string spacedimension = mbb_output + "_dim";
			if (overwritepath || !hdfs_check_data(hadoopcmdpath, spacedimension)) {
				bool obtained_dims = extract_dims(hadoopcmdpath,
					mbb_output, spacedimension);
			}
			
			bool res_cat = hdfs_cat(hadoopcmdpath, spacedimension + "/part-00000");
		

			instr = fdopen(flinfo.pipefd[0], "r");
			fscanf(instr, "%lf %lf %lf %lf %lf %lf %ld", 
				&(spinfo.space_min_x), &(spinfo.space_min_y), &(spinfo.space_min_z),
				&(spinfo.space_max_x), &(spinfo.space_max_y), &(spinfo.space_max_z),
				&(spinfo.num_objects));
			fclose(instr);
			#ifdef DEBUG
			cerr << "Space dimensions: " << spinfo.space_min_x << TAB 
				<< spinfo.space_min_y << TAB << spinfo.space_min_z << TAB
				<< spinfo.space_max_x << TAB << spinfo.space_max_y 
				<< TAB << spinfo.space_max_z << endl;
			cerr << "Number objects: " << spinfo.num_objects << endl;
			#endif	

			// Normalize data and partition
			string normpath = output_path + "_norm";
			if (overwritepath || !hdfs_check_data(hadoopcmdpath, normpath)) {
				bool normalized = normalize_mbb(hadoopcmdpath, mbb_output, normpath);
			}
			#ifdef DEBUG
			cerr << "Done normalizing MBBs" << endl;
			#endif	

			// Setting default block size
			if (bucket_size < 0) {
				// Bucket size was not set
				double blockSize = 16000000; // approximately 16MB
				bucket_size = static_cast<int>(floor(blockSize * sampling_rate / spinfo.total_size  * spinfo.num_objects));
			}
			// Partition the data to generate tile boundaries
			#ifdef DEBUG
			cerr << "Bucket size: " << bucket_size << endl;
			cerr << "Sampling rate: " << sampling_rate << endl;
			#endif
			string partitionpath = output_path + "_partidx";
			if (overwritepath || !hdfs_check_data(hadoopcmdpath, partitionpath)) {
				bool partition = partition_data(hadoopcmdpath, normpath, 
				partitionpath, partition_method, bucket_size);
			}

			// Obtain the hdfs copy 
			char *tmpFile = mktemp(nametemplate);
			// char tmpFile[] = "hadoopgispartfile";
			int tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
		//	int tmpfd = mkstemp(nametemplate);

			#ifdef DEBUG
			cerr << "Temp file name to hold partition boundary: " << tmpFile << endl;
			#endif


			// TO BE UPDATED: For octree and other methods we will need to denormalize the partition boundary back to original space
			//
			// Currently FG already output the original space
			//
			// bool res_partition = hdfs_cat(hadoopcmdpath, partitionpath + "/part-00000", tmpfd);
	//		string partitiondenorm = output_path + "_denorm";					
	//		bool denormalized = normalize_mbb(hadoopcmdpath, 
	//			partitionpath + "/part-00000" , partitiondenorm, true);
	// For current Fix grid, the result is already denormalized	
	//		bool res_partition = hdfs_cat(hadoopcmdpath, partitiondenorm + "/part*", tmpfd);
			bool res_partition = hdfs_cat(hadoopcmdpath, partitionpath + "/part*", tmpfd);
			
			// Obtain the cache file from hdfs
			char *tmpnameonly = strrchr(tmpFile, '/');
			tmpnameonly++;
			#ifdef DEBUG
			cerr << "Name only: " << tmpnameonly << endl;	
			#endif
			string joinoutputpath = output_path + "_joinout";
			cerr << "Executing spatial join." << endl;	
			bool res_join = sp_join(hadoopcmdpath, inputpaths, 
				joinoutputpath, sharedparams, tmpnameonly, tmpFile);
			
			cerr << "Done with spatial join." << endl;	
			exit(0);
			// Perform duplicate removal
			
			bool res_duprem = duplicate_removal(hadoopcmdpath, 
				joinoutputpath, output_path);
			cerr << "Done with boundary handling. Results are stored at "
				<< output_path << endl;
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
}
