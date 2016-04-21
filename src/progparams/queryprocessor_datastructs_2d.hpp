
char nametemplate[] = "/tmp/hadoopgisXXXXXX";

// Supported query types
const std::string QUERYPROC_PARTITION = "partition";
const std::string QUERYPROC_CONTAINMENT = "containment";
const std::string QUERYPROC_JOIN = "spjoin";

// Names of binaries
const std::string MANIPULATE = "manipulate_2d";
const std::string SPACE_EXTRACTOR = "stats_extract_space_dims";
const std::string DUPLICATE_REMOVER = "duplicate_remover";
const std::string STAT_COLLECT_MAPPER = "collect_tile_stat";
const std::string STAT_COLLECT_REDUCER = "combine_stat";
const std::string MBB_SAMPLER = "sampler";

const std::string RESQUE = "resque_2d";
const std::string PARTITION_FG = "fg_2d";
const std::string PARTITION_BSP = "bsp_2d";
const std::string PARTITION_SFC = "hc_2d";
const std::string PARTITION_BOS = "bos_2d";
const std::string PARTITION_STR = "str_2d";
const std::string PARTITION_SLC = "slc_2d";
const std::string PARTITION_QT = "qt_2d";

/* Parameter placeholders */
// Holds spatial dimensions of the space
struct space_info {
	long num_objects;
	double space_low[2];
	double space_high[2];
	double total_size;
};


// Manages piped I/O
struct flow_info;
struct framework_vars;

// Combined framework variables
struct framework_vars {
	// MapReduce-related parameter
	std::string hadoopgis_prefix;
	std::string streaming_path; // Absolute path to the hadoop streaming jar
	std::string hadoopcmdpath; // Absolute path to the command line hadoop executable
	std::string hdfscmdpath; // Absolute path to the command line hadoop executable
	std::string hadoopldlibpath; // LD_LIBRARY_PATH on the system
	bool overwritepath; // Overwrite HDFS directory if it already exists
	int numreducers; // number of reducer
	std::string numreducers_str; // number of reducers as a c string (for convenience)

	struct space_info spinfo;

	// Input data variables	
	std::string input_path_1;
	std::string input_path_2;
	int shp_idx_1;
	int shp_idx_2;
	long size_1;
	long size_2;
	double obtain_size_1;
	double obtain_size_2;
	bool loaded_1;
	bool loaded_2;
	int join_cardinality;
	int knn; // k in nearest neighbor
	std::string sharedparams;
	
	// Query variables
	std::string output_fields;
	std::string output_path;
	std::string query_type;
	std::string predicate;
	double distance;
	bool containment_use_file;

	std::string user_file;
	std::string containment_window;

	// Partitioning variables
	std::string partition_method; // Default selection
	std::string partition_method_2; // Default selection
	std::string partitioningparams;
	double sampling_rate; // to be changed for different sampling method
	long bucket_size;
	bool para_partition;
	long block_size;
	long rough_bucket_size;
	
	std::string mbb_path_1;
	std::string mbb_path_2;

	// Cleanup flags
	bool remove_tmp_dirs;
	bool remove_tmp_mbb;

	// Temporary paths
	std::string tmp_path;
	std::string mbb_output;
	std::string mbb_path;
	std::string space_path;
	std::string partitionpath;
	std::string partitionpath2;
	std::string partitionpathout;
	std::string partitionpathout2;
	std::string stat_path;
	std::string joinoutputpath;
	std::string statpathout;
	std::string datapath;	
	std::string partidx_final;
	std::string config_final;
};

void init_params(struct framework_vars &fr_vars);
bool extract_params(int argc, char **argv, struct framework_vars &fr_vars);
