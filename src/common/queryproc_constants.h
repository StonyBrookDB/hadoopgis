
using namespace std;

// MapReduce constants
const string JAR_FILE_NAME = "hadoop-streaming.jar";
const string CUSTOM_JAR_REL_PATH = "../../build/libjar/myCustomLibs.jar"; 
// relative path with respect to the lib path

const string PARTITION_FILE_NAME = "partfile.idx";

// Supported query types
const string QUERYPROC_PARTITION = "partition";
const string QUERYPROC_CONTAINMENT = "containment";
const string QUERYPROC_JOIN = "spjoin";

const string MBB_EXTRACTOR = "map_obj_to_tile";
const string SPACE_EXTRACTOR = "get_space_dimension";
const string MBB_NORMALIZER = "mbb_normalizer";
const string DUPLICATE_REMOVER = "duplicate_remover";
const string STAT_COLLECT_MAPPER = "collect_tile_stat";
const string STAT_COLLECT_REDUCER = "combine_stat";
const string MBB_SAMPLER = "sampler";
const string CONTAINMENT_PROC = "containment_proc";

const string RESQUE = "resque";
const string PARTITION_FG = "fg";
const string PARTITION_BSP = "bsp";
const string PARTITION_SFC = "hc";
const string PARTITION_BOS = "bos";
const string PARTITION_STR = "str";
const string PARTITION_SLC = "slc";
const string PARTITION_QT = "qt";
