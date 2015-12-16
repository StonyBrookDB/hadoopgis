
#include <iostream>
#include <cstring>
#include <cmath>
#include <cstdlib>

using namespace std;

/* This program computes the bucket size given blocksize, sampleratio and avg object size
 *
 * */

int main(int argc, char **argv) {
	if (argc < 5) {
		cerr << "Not enough arguments. Usage: " << argv[0] 
			<< " [blocksize] [sampleratio] [total_object_size] [total_object_count]" << endl;
		return -1;
	}

	double blockSize = atof(argv[1]);
	double sampleRatio = atof(argv[2]);
	double totalObjectSize = atof(argv[3]);
	double totalObjectCount = atof(argv[4]);

	cout << "partitionSize=" << static_cast<long>(floor(blockSize * sampleRatio / totalObjectSize * totalObjectCount)) << endl;
	cout.flush();

	return 0;
}
