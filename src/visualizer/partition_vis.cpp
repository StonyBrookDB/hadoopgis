#include <iostream>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <algorithm>   
#include <vector>
#include <map>
#include <cstdlib>
#include <random> 
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <progparams/string_constants.h>
#include <utilities/tokenizer.h>
#include <utilities/Timer.hpp>
#include <boost/program_options.hpp>

/* 
 * This program plots partition boundaries and objects
 *  It requires gnuplot
 * */

using namespace std;
namespace po = boost::program_options;

// main method
int main(int ac, char** av) {
	cout.precision(20);
	string partidx_file;
	string obj_file;
	string space_file;
	string output_file;;
	int offset = 1;

	// Use to handle input lines
	string input_line;
	vector<string> fields;

	cout.precision(15);
	double low[2];
	double high[2];
	double global_low[2];
	double global_high[2];
	string tile_id;
	int count = 1;
	srand(time(NULL));
	int max_color = 5;

	global_low[0] = global_low[1] = std::numeric_limits<double>::max();
	global_high[0] = global_high[1] = std::numeric_limits<double>::min();
	
	bool use_global_space = false;
	// Invoke gnuplot
	int pipefd[2];
	pid_t childpid;
	int status;

	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "this help message")
			("partidxfile,p", po::value<string>(&partidx_file), "File name of the \
partition boundaries in MBB format. Partition boundaries have random colors.")
			("offsetpartidx,f", po::value<int>(&offset), "Field offset for the start of MBB fields. Default is 1")
			("objfile,q", po::value<string>(&obj_file), "File name of the objects in MBB format. Objects have black border.")
			("spacefile,s", po::value<string>(&space_file), "File containing the global space information. Global space info should have the same format as partition boundary. If none is specified, the max/min of partition boundaries will be used as the global space.")
			("outputname,o", po::value<string>(&output_file), "Name of the output file. Should contain .png extension");

		po::variables_map vm;        
		po::store(po::parse_command_line(ac, av, desc), vm);
		po::notify(vm);    

		if (vm.count("help")) {
			cerr << desc << endl;
			return 0; 
		}

		if (!vm.count("outputname")) {
			cerr << "Missing output name" << endl;
			return 1;
		}

		if (!vm.count("objfile") && !vm.count("partidxfile") ) {
			cerr << "Requiring at least an object file or partition boundary file" << endl;
			return 1;
		}

		if (vm.count("spacefile")) {
			use_global_space = true;
		}

               	stringstream ofs;
		//ofstream ofs(output_file.c_str(), ofstream::trunc);

		//ofs << "#! /usr/bin/gnuplot\n";
		ofs << "reset\nunset tics\n"
		<< "set term png\n"
		<< "set output \"" << output_file  <<  "\"" << endl;
		
		
		if (vm.count("partidxfile")) {
			ifstream in_partidx(partidx_file.c_str());
			if (in_partidx.is_open()) {
				while (getline(in_partidx, input_line)) {
					istringstream tmpss(input_line);
					tmpss >> tile_id >> low[0] >> low[1] >> high[0] >> high[1];
					if (!use_global_space) {
						global_low[0] = min(global_low[0], low[0]);
						global_low[1] = min(global_low[1], low[1]);
						global_high[0] = max(global_high[0], high[0]);
						global_high[1] = max(global_high[1], high[1]);
					}
					ofs << "set object " << count++ 
						<< " rect from " << low[0] << "," << low[1]
						<< " to " << high[0] << "," << high[1]
						<< " fs empty border " << (rand() % max_color + 2)
						<< " lw 2"
						<< endl;
				}
			}
		}

		if (vm.count("objfile")) {
			ifstream in_objfile(obj_file.c_str());
			if (in_objfile.is_open()) {
				while (getline(in_objfile, input_line)) {
					istringstream tmpss(input_line);
					tmpss >> tile_id >> low[0] >> low[1] >> high[0] >> high[1];
					ofs << "set object " << count++ 
						<< " rect from " << low[0] << "," << low[1]
						<< " to " << high[0] << "," << high[1]
						<< " fs empty border 1 lw 1"
						<< endl;
				}
			}
		}

		if (use_global_space) {
			ifstream in_globalspace(space_file.c_str());
			if (in_globalspace.is_open()) {
				getline(in_globalspace, input_line);
				istringstream tmpss(input_line);
				tmpss >> tile_id >> global_low[0] >> global_low[1] 
					>> global_high[0] >> global_high[1];
			}
		}
		ofs << "plot [" << global_low[0] << ":" << global_high[0] << "] "
			<< "[" << global_low[1] << ":" << global_high[1] << "] "
			<< "NaN notitle" << endl;
		#ifdef DEBUG
		cerr << ofs.str();
		#endif


		// Create a new process to run gnuplot
		pipe(pipefd);
		if ((childpid = fork()) == -1) {
			#ifdef DEBUG
			cerr << "Fork failed" << endl;
			#endif
			exit(1);
		}

		if (!childpid) {
			// Child
			close(pipefd[1]);
			close(STDIN_FILENO);
			dup2(pipefd[0], STDIN_FILENO);
			close(pipefd[0]);
			execl("/usr/bin/gnuplot", "gnuplot", 0, 0);
			//execl("/bin/cat", "cat", NULL, NULL);
			cerr << "Should not be here. Exec failed" << endl;
			exit(1);
		} else {
			// Parent
			close(pipefd[0]);
			string tmpstr = ofs.str();
			int len = tmpstr.size();
			const char * tmpptr = tmpstr.c_str();
			// Write to pipefd[0];
			//cout << tmpptr << endl;	
			write(pipefd[1], tmpptr, len);
			close(pipefd[1]);
		}
		int status;
		if (wait(&status)) {
			#ifdef DEBUG
			cerr << "Done. Result is stored in " << output_file << endl;
			#endif
		}

	}
	catch(exception& e) {
		cerr << "error: " << e.what() << "\n";
		return 1;
	}
	catch(...) {
		cerr << "Exception of unknown type!\n";
		return 1;
	}




	return 0;	
}



