using namespace std;

const double EARTH_RADIUS = 3958.75;

const int OSM_SRID = 4326;
const int ST_INTERSECTS = 1;
const int ST_TOUCHES = 2;
const int ST_CROSSES = 3;
const int ST_CONTAINS = 4;
const int ST_ADJACENT = 5;
const int ST_DISJOINT = 6;
const int ST_EQUALS = 7;
const int ST_DWITHIN = 8;
const int ST_WITHIN = 9;
const int ST_OVERLAPS = 10;
const int ST_NEAREST = 11;
const int ST_NEAREST_2 = 12;

const string PARAM_PREDICATE_INTERSECTS = "st_intersects";
const string PARAM_PREDICATE_TOUCHES = "st_touches";
const string PARAM_PREDICATE_CROSSES = "st_crosses";
const string PARAM_PREDICATE_CONTAINS = "st_contains";
const string PARAM_PREDICATE_ADJACENT = "st_adjacent";
const string PARAM_PREDICATE_DISJOINT = "st_disjoint";
const string PARAM_PREDICATE_EQUALS = "st_equals";
const string PARAM_PREDICATE_DWITHIN = "st_dwithin";
const string PARAM_PREDICATE_WITHIN = "st_within";
const string PARAM_PREDICATE_OVERLAPS = "st_overlaps";
const string PARAM_PREDICATE_NEAREST = "st_nearest";
const string PARAM_PREDICATE_NEAREST_NO_BOUND = "st_nearest2";

const int SID_1 = 1;
const int SID_2 = 2;
const int SID_NEUTRAL = 0;

const int STATS_AREA_1 = -1;
const int STATS_AREA_2 = -2;
const int STATS_UNION_AREA = -3;
const int STATS_INTERSECT_AREA = -4;
const int STATS_JACCARD_COEF = -5;
const int STATS_DICE_COEF = -6;
const int STATS_TILE_ID = -7;
const int STATS_MIN_DIST = -8;

const string PARAM_STATS_AREA_1 = "area1";
const string PARAM_STATS_AREA_2 = "area2";
const string PARAM_STATS_UNION_AREA = "union";
const string PARAM_STATS_INTERSECT_AREA = "intersect";
const string PARAM_STATS_JACCARD_COEF = "jaccard";
const string PARAM_STATS_DICE_COEF = "dice";
const string PARAM_STATS_TILE_ID= "tileid";
const string PARAM_STATS_MIN_DIST = "mindist";


const string STR_SET_DELIM = ":";
const string STR_OUTPUT_DELIM = COMMA;

#define FillFactor 0.9
#define IndexCapacity 10 
#define LeafCapacity 50
#define COMPRESS true
