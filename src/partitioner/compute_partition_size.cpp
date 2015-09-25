
#include <iostream>
#include <cstring>
#include <cmath>
#include <cstdlib>

using namespace std;

/* This program computes the bucket size given blocksize, sampleratio and avg object size
 *
 * */

int main(int argc, char **argv) {
	if (argc < 4) {
		cerr << "Not enough arguments. Usage: " << argv[0] 
			<< " [blocksize] [sampleratio] [average_object_size]" << endl;
		return -1;
	}

	long blockSize = atol(argv[1]);
	double sampleRatio = atof(argv[2]);
	double avgObjectSize = atof(argv[3]);

	cout << "partitionSize=" << static_cast<long>(floor(blockSize * sampleRatio / avgObjectSize)) << endl;
	cout.flush();

	return 0;
}
