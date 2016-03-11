/* 
 * This header contains relevant function for interaction between Hadoop MapReduce and CPP code
 * */

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

using namespace std;
// Function definition
//
//
// Global variable
struct framework_vars;
struct flow_info;


struct flow_info {
	int pipefd[2];
} flinfo;


// MapReduce constants
const string JAR_FILE_NAME = "hadoop-streaming.jar";
// relative path with respect to the lib path
const string CUSTOM_JAR_REL_PATH = "../../build/libjar/myCustomLibs.jar"; 
const string PARTITION_FILE_NAME = "partfile.idx";


string update_ld_lib_path();
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
pid_t execute_command(string programpath,vector<string> &strargs, int outputfd = -1);

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

string update_ld_lib_path() {
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
	return ss.str();
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
