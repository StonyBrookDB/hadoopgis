#include <iostream>
#include <iomanip>
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
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

/* 
 * This program plots partition boundaries and objects
 *  It requires gnuplot
 * */

struct mbb_2d {
	double low[2];
	double high[2];
};

using namespace std;
namespace po = boost::program_options;

string POLYGON_OPEN = "POLYGON((";
string POLYGON_END = "))";


// main method
int main(int ac, char** av) {
	cout.precision(20);
	#ifdef DEBUG
	cerr.precision(20);
	#endif
	// Use to handle input lines
	string input_line;
	vector<string> fields;

	vector<double> cords[2];
	vector<int> dis_cords[2];

	cout.precision(15);
	double old_low[2];
	double old_high[2];
	double new_low[2];
	double new_high[2];

	double old_span[2];
	double new_span[2];
	vector<int> hole_positions; // contains position holes

	int mbb_low[2];
	int mbb_high[2];

	string tile_id;
	string prev_id = "";

	string join_idx;
	int geom_idx;

	int count = 0;
	int offset = 2;
	string delimiters = " ,";
	size_t prev;
	size_t pos;
	double tmpVal;
	int cords_counter;
	int num_vertices;
	int i;
	int k;

	bool tile_disc = false; // use index file
	string file_name; // name of the index file
	map<string, struct mbb_2d *> tile_boundaries;
	struct mbb_2d *mbb_ptr;

	bool skip_complex = false;

	try {
		po::options_description desc("Options");
		desc.add_options()
			("help", "this help message")
			("oldminx,a", po::value<double>(&old_low[0]), "Old min x")
			("oldminy,b", po::value<double>(&old_low[1]), "Old min y")
			("oldmaxx,c", po::value<double>(&old_high[0]), "Old max x")
			("oldmaxy,d", po::value<double>(&old_high[1]), "Old max y")
			("newminx,r", po::value<double>(&new_low[0]), "New min x")
			("newminy,s", po::value<double>(&new_low[1]), "New min y")
			("newmaxx,t", po::value<double>(&new_high[0]), "New max x")
			("newmaxy,u", po::value<double>(&new_high[1]), "New max y")
			("offset,o", po::value<int>(&offset), "Offset (Number of meta fields before \
the actual fields numbers - numbers of inserted attributes by MR). \
Default value is 2 (for tile id and join id).")
			("geomidx,g", po::value<int>(&geom_idx), "Geometry field number (starting from 1)")
			("joinidx,j", po::value<string>(&join_idx), "Join index [ 1 | 2]")
			("indexfile,h", po::value<string>(&file_name), "Index file name (incl. path)")
			("skipcomplex,k", "Skip polygons with holes");

		po::variables_map vm;        
		po::store(po::parse_command_line(ac, av, desc), vm);
		po::notify(vm);    

		if (vm.count("help")) {
			cerr << desc << endl;
			return 0; 
		}

		if (!vm.count("oldminx") || !vm.count("oldminy") || !vm.count("oldmaxx") || !vm.count("oldmaxy")
			|| !vm.count("newminx") || !vm.count("newminy") || !vm.count("newmaxx") || !vm.count("newmaxy")) {
			cerr << "Missing coordinate parameters" << endl;
			return 1;
		}
		if (!vm.count("geomidx")) {
			cerr << "Missing geometry index" << endl;
			return 1;
		}

		if (vm.count("indexfile")) {
			tile_disc = true;
			// Reading index files
			ifstream index_input(file_name);
			while (index_input) {
				mbb_ptr = new struct mbb_2d();
				index_input >> tile_id >> mbb_ptr->low[0] >> mbb_ptr->low[1]
					>> mbb_ptr->high[0] >> mbb_ptr->high[1];
				tile_boundaries[tile_id] = mbb_ptr;	

			}
		}
		if (vm.count("skipcomplex")) {
			skip_complex = true;
		}

		// Processing objects
		while(cin && getline(cin, input_line) && !cin.eof()) {
			count++;
			//cerr << "Processing: " << count << endl;
			tokenize(input_line, fields, TAB, true);
			tile_id = fields[0];

			if (prev_id.compare(tile_id) != 0) {
				// adjust the space information to the new tile
				mbb_ptr = tile_boundaries[tile_id];
				for (k = 0; k < 2; k++) {
					old_low[k] = mbb_ptr->low[k];		
					old_high[k] = mbb_ptr->high[k];

					old_span[k] = old_high[k] - old_low[k];
					new_span[k] = new_high[k] - new_low[k];
				}
				#ifdef DEBUG
				cerr << "Space info: " << old_low[0] << TAB << old_low[1] 
					<< TAB << old_high[0] << TAB << old_high[1] << endl;
	
				#endif
				// Update the tile id
				prev_id = tile_id;
			}

			join_idx = fields[1];			
			

		
			// Parsing the polygon coordinates
			string poly_str = fields[geom_idx + offset - 1];

			boost::replace_all(poly_str, POLYGON_OPEN, "");
			boost::replace_all(poly_str, POLYGON_END, "");

			prev = 0;
			cords_counter = 0;

    			while ((pos = poly_str.find_first_of(" ,)", prev)) != string::npos) {
				if (pos > prev) {
					// Alternating x and y					
				//	cerr << "parsing:" << cords_counter << SPACE 
				//		<< poly_str.substr(prev, pos-prev) << endl;
            				cords[cords_counter % 2].push_back(
						stod(poly_str.substr(prev, pos-prev)));
					cords_counter++;
					// parsing cords_counter
				}
				if (poly_str.at(pos) == ')') {
					// position of a hole
			//		cerr << "skipping hole " << pos << endl;
					hole_positions.push_back(cords_counter / 2);
					pos = pos + 2; // skip , and (
					/*
					while (poly_str.at(pos) != '(') {
						pos++;
					}
					*/
				}
        			prev = pos+1;
    			}

    			if (prev < poly_str.length()) {
        			cords[cords_counter % 2].push_back(
					stod(poly_str.substr(prev, string::npos)));	
				//cerr << "parsing:" << cords_counter << endl;
				cords_counter++;
			}

			if (skip_complex && hole_positions.size() > 0) {
				// Skip the current polygon if it has holes
				cords[0].clear();
				cords[1].clear();
				dis_cords[0].clear();
				dis_cords[1].clear();
				hole_positions.clear();
				fields.clear();
				continue;
			}

			// Number of vertices = number of coordinates / 2
			num_vertices = cords_counter / 2;
			#ifdef DEBUG
			cerr << "Read a polygon with " << num_vertices << " vertices." << endl;
			cerr << "Number of coordinates:" << cords_counter << endl;
			#endif

			// Skipping polygons (optional)
			if (num_vertices < 3) {
				cerr << "Skipping polygon with " << num_vertices << endl;
				continue;
			}

			#ifdef DEBUG
			cerr << "x: ";
			for (std::vector<double>::iterator it = cords[0].begin(); 
							it != cords[0].end(); ++it) {
    				cerr << *it << SPACE;
			}
			cerr << endl << "y: ";
			for (std::vector<double>::iterator it = cords[1].begin();
							it != cords[1].end(); ++it) {
    				cerr << *it << SPACE;
			}
			cerr << endl;
			#endif
		
			#ifdef DEBUG
			cerr << "Original coordinates: " << old_low[0] << TAB << old_low[1] << TAB
						<< old_high[0] << TAB << old_high[1] << endl;
			cerr << "New coordinates: " << new_low[0] << TAB << new_low[1] << TAB
						<< new_high[0] << TAB << new_high[1] << endl;
			cerr << "Hole positions: ";
			for (vector<int>::const_iterator it = hole_positions.begin(); it != hole_positions.end(); ++it)
    				cerr << *it << SPACE;
			cerr << endl;
			#endif


			#ifdef DEBUG
			cerr << "Transforming coordinates." << endl;
			#endif
			for (k = 0; k < 2; k++) {
				for (i = 0; i < num_vertices; i++) {
					dis_cords[k].push_back(ceil(
						(cords[k][i] - old_low[k]) / old_span[k] * new_span[k] + new_low[k]));
					//cerr << "Converting: " << cords[k][i] << " into " << dis_cords[k][i] << endl;
				}
			}
			#ifdef DEBUG
			cerr << "Computing MBB." << endl;
			#endif
			// Compute MBB
			for (k = 0; k < 2; k++) {
				mbb_low[k] = mbb_high[k] = dis_cords[k][0];
				for (i = 1; i < num_vertices; i++) {
					if (dis_cords[k][i] < mbb_low[k]) {
						mbb_low[k] = dis_cords[k][i];			
					}
					if (dis_cords[k][i] > mbb_high[k]) {
						mbb_high[k] = dis_cords[k][i];			
					}
				}
			}

			// Transform and output the coordinates
			cout << tile_id << TAB << join_idx << COMMA << count << COMMA;
			cout.fill('0');
			cout << setw(4) << num_vertices;
			// MBB - convention is changed here minx maxx miny maxy
			cout << COMMA <<  mbb_low[0] << SPACE << mbb_low[1]
				<< SPACE << mbb_high[0] << SPACE << mbb_high[1];			

			cout << COMMA << POLYGON_OPEN << dis_cords[0][0] << SPACE << dis_cords[1][0];
			int idx_hole = 0;
			for (i = 1; i < num_vertices; i++) {
				if (idx_hole < hole_positions.size() &&  i == hole_positions[idx_hole]) {
					cout << "),(";
					idx_hole++;
				} else {
					cout << COMMA;
				}
				cout << dis_cords[0][i] << SPACE  << dis_cords[1][i];
			}
			cout << POLYGON_END << endl;

			cords[0].clear();
			cords[1].clear();
			dis_cords[0].clear();
			dis_cords[1].clear();
			hole_positions.clear();
			fields.clear();
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
	cout.flush();
	cerr.flush();
	return 0;	
}



