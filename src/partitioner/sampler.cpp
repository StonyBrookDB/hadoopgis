#include <iostream>
#include <cstring>
#include <cmath>
#include <cstdlib> 
#include <limits>
#include <random>
#include <ctime>

using namespace std;

/* This program performs random coin flipping with a fixed sampling rate
 * */

int main(int argc, char **argv) {

	if (argc < 2) {
		cerr << "Missing usage mode. Usage: " << argv[0] << " [ samplerate ]" << endl;
		return -1;
	}

	double sample_rate = atof(argv[1]);
	int max = std::numeric_limits<int>::max();
	int threshold = static_cast<int>(ceil(max * sample_rate));

	srand(time(NULL));

	#ifdef DEBUG
	cerr << "Usage mode: " << argv[1] << endl;
	#endif
	string input_line = "";
	while(cin && getline(cin, input_line) && !cin.eof()) {
		if (rand() % max <= threshold ) {
			cout << input_line << endl;
		}
	}

	cout.flush();
}
