
using namespace std;

// MapReduce constants
const string JAR_FILE_NAME = "hadoop-streaming.jar";

const string PARTITION_FILE_NAME = "partfile.idx";

// Supported query types
const string QUERYPROC_CONTAINMENT = "containment";
const string QUERYPROC_JOIN = "spjoin";
const string CUSTOM_JAR_REL_PATH = "../../build/libjar/myCustomLibs.jar";

const string MBB_EXTRACTOR = "map_obj_to_tile_3d";
const string SPACE_EXTRACTOR = "get_space_dimension_3d";
const string MBB_NORMALIZER = "mbb_normalizer_3d";
const string DUPLICATE_REMOVER = "duplicate_remover";
const string RESQUE = "resque3d";
const string PARTITION_FG = "fg3d";
const string PARTITION_BSP = "bsp3d";
const string PARTITION_SFC = "hc3d";
const string PARTITION_BOS = "bos3d";
const string PARTITION_STR = "str3d";
