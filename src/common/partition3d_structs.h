
/* Query operator */
struct partition_op {

	/* Space span  */
	double min_x;
	double min_y;
	double min_z;
	double max_x;
	double max_y;
	double max_z;
	int offset; // number of fields before the first MBB field (min_x)

	/* only at most one can be true  */
	bool to_be_normalized;
	bool to_be_denormalized;

	bool parallel_partitioning;
	char *file_name;

};

struct mbb_info {
	double low[3];
	double high[3];
};


