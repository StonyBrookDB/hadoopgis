
bool sp_join(string programpath, vector<string> &input_paths, 
		string output_path, string original_params, struct framework_vars &fr_vars, 
		char *cachefilename, char *cachefilefullpath) {
	hdfs_delete(programpath, output_path);
	vector<string> arr_args = {"hadoop", "jar", fr_vars.streaming_path};
	//arr_args.push_back("-conf");
	//arr_args.push_back("/home/vhoang/mapred-site.xml");
	for(vector<string>::iterator it = input_paths.begin(); it != input_paths.end(); ++it) {
		arr_args.push_back("-input");
		arr_args.push_back(*it);
	}

	arr_args.push_back("-output");
	arr_args.push_back(output_path);

	arr_args.push_back("-file");
	arr_args.push_back(fr_vars.hadoopgis_prefix + MANIPULATE);
	arr_args.push_back("-file");
	arr_args.push_back(fr_vars.hadoopgis_prefix + RESQUE);
	arr_args.push_back("-file");
	string strtmp(cachefilefullpath);
	arr_args.push_back(strtmp);

	arr_args.push_back("-mapper");
	stringstream ss;
	ss << MANIPULATE << " -o 0" << original_params
		<< " -c " << cachefilename;
	arr_args.push_back(ss.str());

	arr_args.push_back("-reducer");
	arr_args.push_back(RESQUE + " -o 2" + original_params); // Offset to account for tile id and join index
	//arr_args.push_back("cat");

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back(fr_vars.numreducers_str);
	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(fr_vars.hadoopldlibpath);

#ifdef DEBUG
	cerr << "Executing spjoin program params: ";
	for(vector<string>::iterator it = arr_args.begin(); it != arr_args.end(); ++it) {
		cerr << *it << " ";
	}
	cerr << endl;
#endif

	int status = 0;
	pid_t childpid;
	if (childpid = execute_command(programpath, arr_args)) {
		if (wait(&status)) {
#ifdef DEBUG
			cerr << "Succeeded in sp join: " << status << endl;
#endif
		} else {
#ifdef DEBUG
			cerr << "Failed in sp join: " << status << endl;
#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}



bool execute_spjoin(struct framework_vars &fr_vars) {
	// First check if dataset 1 has been loaded or not

	// loaded_1 = hdfs_check_data(hadoopcmdpath, input_path_1 + "/" + PARTITION_FILE_NAME);
	fr_vars.obtain_size_1 = hdfs_get_size(fr_vars.hadoopcmdpath, fr_vars.input_path_1);
	if (fr_vars.obtain_size_1 >= 0) {
		fr_vars.spinfo.total_size = fr_vars.obtain_size_1;
	}

	// If there is a 2nd dataset, also check if it has been loaded and obtain its size
	if (fr_vars.join_cardinality > 1) {
		// loaded_2 = hdfs_check_data(hadoopcmdpath, input_path_2 + "/" + PARTITION_FILE_NAME);
		fr_vars.obtain_size_2 = hdfs_get_size(fr_vars.hadoopcmdpath, fr_vars.input_path_2);
		if (fr_vars.obtain_size_2 >= 0) {
			fr_vars.spinfo.total_size += fr_vars.obtain_size_2;
		}
	}
#ifdef DEBUG
	cerr << "Total size of 1 " << fr_vars.obtain_size_1 << endl;
	if (fr_vars.join_cardinality > 1) {
		cerr << "Total size of 2 " << fr_vars.obtain_size_2 << endl;
	}
	cerr << "Total object size: " << fr_vars.spinfo.total_size << endl;
	cerr << "Data 1 is loaded " << (fr_vars.loaded_1 ? "true" : "false" ) << endl;
	cerr << "Data 2 is loaded " << (fr_vars.loaded_2 ? "true" : "false" ) << endl;
#endif

	vector<string> inputpaths;

	// Neither data has been indexed
	// string mbb_output = output_path + "_mbb";	
	inputpaths.push_back(fr_vars.input_path_1);
	if (fr_vars.join_cardinality > 1) {
		inputpaths.push_back(fr_vars.input_path_2);
	}

	// Extract object MBB and grab space dimension
	if (fr_vars.overwritepath || !hdfs_check_data(fr_vars.hadoopcmdpath, fr_vars.mbb_output)) {
#ifdef DEBUG
		cerr << "\n\nExtracting MBBs\n" << endl;
#endif
		if (!extract_mbb(fr_vars.hadoopcmdpath, inputpaths,
					fr_vars.mbb_output, fr_vars.sharedparams, fr_vars)) {
			cerr << "Failed extracting MBB"  << endl;
			exit(1);
		}
	}

	char *tmpFile = mktemp(nametemplate);
	int tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
	char *tmpnameonly = strrchr(tmpFile, '/'); // pointing to just the name of the cache file
	tmpnameonly++; // Advance the pointer past the slash delimiter character
#ifdef DEBUG
	cerr << "Temp file name only: " << tmpnameonly << endl;	
#endif
	// Obtain the cache file from hdfs
	bool res_cat = hdfs_cat(fr_vars.hadoopcmdpath, fr_vars.space_path, tmpfd);
	close(tmpfd);

#ifdef DEBUG
	ifstream instr(tmpFile);
	string dummystr;
	if (instr.is_open()) {
		instr >> dummystr
			>> fr_vars.spinfo.space_low[0] >> fr_vars.spinfo.space_low[1] 
			>> fr_vars.spinfo.space_high[0] >> fr_vars.spinfo.space_high[1]
			>> fr_vars.spinfo.num_objects;
		instr.close();
	}
	cerr << "Space dimensions: " << fr_vars.spinfo.space_low[0] << TAB 
		<< fr_vars.spinfo.space_low[1] << TAB
		<< fr_vars.spinfo.space_high[0] << TAB << fr_vars.spinfo.space_high[1] 
		<< endl;
	cerr << "Number objects: " << fr_vars.spinfo.num_objects << endl;
#endif	

	// Setting default block size if it has not been set
	if (fr_vars.bucket_size < 0) {
		// Bucket size was not set
		double blockSize = 16000000; // approximately 16MB
		fr_vars.bucket_size = max(static_cast<int>(floor(blockSize
						/ fr_vars.spinfo.total_size * fr_vars.spinfo.num_objects)), 1);
	}

	// 1st step of partition the data to generate tile boundaries
#ifdef DEBUG
	cerr << "Bucket size: " << fr_vars.bucket_size << endl;
	cerr << "Sampling rate: " << fr_vars.sampling_rate << endl;
	cerr << "\n\nPartitioning (1st step)\n" << endl;
#endif
	if (!fr_vars.para_partition) {
		fr_vars.rough_bucket_size = fr_vars.bucket_size;
		if (fr_vars.overwritepath || !hdfs_check_data(fr_vars.hadoopcmdpath, fr_vars.partitionpath)) {
#ifdef DEBUG
			cerr << "\n\nPartitioning 2nd steps\n" << endl;
#endif
			if (!partition_data(fr_vars.hadoopcmdpath, fr_vars.mbb_path, 
						fr_vars.partitionpath, fr_vars.partition_method, fr_vars.bucket_size, 
						fr_vars.sharedparams, 1, fr_vars.sampling_rate, fr_vars, tmpnameonly, tmpFile)) {
				cerr << "Failed partitioning 1st step" << endl;
				// Remove cache file
				remove(tmpFile);
				exit(1);
			}
		}
	} else {
		if (fr_vars.rough_bucket_size == -1) {
			// Rough bucket size has not been set
			if (fr_vars.numreducers > 0) {
				fr_vars.rough_bucket_size = max(static_cast<int>(floor(fr_vars.spinfo.num_objects 
								/ fr_vars.numreducers / 2)), 1);
			} else {
				fr_vars.rough_bucket_size = max(static_cast<int>(floor(fr_vars.spinfo.num_objects 
								/ 2)), 1);
			}	
		}
		cerr << "Rough bucket size: " << fr_vars.rough_bucket_size << endl;
		if (fr_vars.overwritepath || !hdfs_check_data(fr_vars.hadoopcmdpath, fr_vars.partitionpath)) {
#ifdef DEBUG
			cerr << "\n\nPartitioning 1st step\n" << endl;
#endif
			if (!partition_data(fr_vars.hadoopcmdpath, fr_vars.mbb_path, 
						fr_vars.partitionpath, fr_vars.partition_method, fr_vars.rough_bucket_size, 
						fr_vars.sharedparams, 1, fr_vars.sampling_rate, fr_vars, tmpnameonly, tmpFile)) {
				cerr << "Failed partitioning 1st step" << endl;
				// Remove cache file
				remove(tmpFile);
				exit(1);
			}
		}
	}
	// Obtain the cache file from hdfs
	tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
	bool res_partition = hdfs_cat(fr_vars.hadoopcmdpath, fr_vars.partitionpathout, tmpfd);
	close(tmpfd);

#ifdef DEBU
	cerr << "Temp file name to hold partition boundary: " << tmpFile << endl;
#endif

	if (fr_vars.para_partition) {
		// Second round of partitioning
#ifdef DEBUG
		cerr << "\n\nPartitioning 2nd steps\n" << endl;
#endif
		if (fr_vars.overwritepath || !hdfs_check_data(fr_vars.hadoopcmdpath, fr_vars.partitionpath2)) {
			if (!partition_data(fr_vars.hadoopcmdpath, fr_vars.mbb_path, 
						fr_vars.partitionpath2, fr_vars.partition_method_2, fr_vars.bucket_size, 
						fr_vars.sharedparams, 2, 1, fr_vars, tmpnameonly, tmpFile)) {
				cerr << "Failed partitioning 2nd step" << endl;
				// Remove cache file
				remove(tmpFile);
				exit(1);
			}
		}
		// Update/Overwrite the tmp file	
		tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
		bool res_partition_2 = hdfs_cat(fr_vars.hadoopcmdpath, fr_vars.partitionpathout2, tmpfd);
		close(tmpfd);

	}

	// tmpFile contains partition index
#ifdef DEBUGSTAT
	//string stat_path = output_path + "_stat";
	if (fr_vars.overwritepath || !hdfs_check_data(fr_vars.hadoopcmdpath, fr_vars.stat_path)) {
		if (!collect_stat(fr_vars.hadoopcmdpath, fr_vars.mbb_path, fr_vars.stat_path, 
					fr_vars.sharedparams, fr_vars, tmpnameonly, tmpFile)) {
			cerr << "Failed obtaining stats" << endl;
			// Remove cache file
			remove(tmpFile);
			exit(1);
		}
	}
	cerr << "Done collecting tile counts" << endl;

	// Overwrite the cache file
	tmpfd = open(tmpFile, O_RDWR | O_CREAT | O_TRUNC , 0777);
	hdfs_cat(fr_vars.hadoopcmdpath, fr_vars.statpathout, tmpfd);
	close(tmpfd);

	stringstream outputss;
	outputss << (fr_vars.para_partition ? "true" : "false") << TAB 
		<< fr_vars.partition_method << TAB
		<< fr_vars.rough_bucket_size << TAB 
		<< fr_vars.sampling_rate << TAB 
		<< fr_vars.partition_method_2 << TAB
		<< fr_vars.bucket_size << TAB
		<< fr_vars.spinfo.num_objects << TAB;
	post_process_stat(tmpFile, outputss);
	cout << outputss.str() << endl;

#else

	// Spatial join step
	if (fr_vars.overwritepath || !hdfs_check_data(fr_vars.hadoopcmdpath, fr_vars.joinoutputpath)) {
#ifdef DEBUG
		cerr << "\n\nExecuting spatial joins\n" << endl;
#endif
		if (!sp_join(fr_vars.hadoopcmdpath, inputpaths, 
					fr_vars.joinoutputpath, fr_vars.sharedparams, fr_vars, 
					tmpnameonly, tmpFile)) {
			cerr << "Failed spatial join" << endl;
			// Remove cache file
			remove(tmpFile);
			exit(1);
		}
	}
	cerr << "Done with spatial join." << endl;
	;
	// Perform duplicate removal

	if (fr_vars.overwritepath || !hdfs_check_data(fr_vars.hadoopcmdpath, fr_vars.output_path)) {
#ifdef DEBUG
		cerr << "\n\nBoundary object handling\n" << endl;
#endif
		if (!duplicate_removal(fr_vars.hadoopcmdpath, 
					fr_vars.joinoutputpath, fr_vars.output_path, fr_vars)) {
			cerr << "Failed boundary handling" << endl;
			// Remove cache file
			remove(tmpFile);
			exit(1);
		}
	}
	cerr << "Done with boundary handling. Results are stored at "
		<< fr_vars.output_path << endl;

#endif
	cerr << "Cleaning up/Removing temporary directories" << endl;
	remove(tmpFile);
	if (fr_vars.remove_tmp_dirs) {
		hdfs_delete(fr_vars.hadoopcmdpath, fr_vars.partitionpath);
		if (fr_vars.para_partition) {
			hdfs_delete(fr_vars.hadoopcmdpath, fr_vars.partitionpath2);
		}
		hdfs_delete(fr_vars.hadoopcmdpath, fr_vars.joinoutputpath);
	}
	if (fr_vars.remove_tmp_mbb) {
		hdfs_delete(fr_vars.hadoopcmdpath, fr_vars.mbb_output);

	}

#ifdef DEBUGTIME
	total_exec_time = clock() - total_exec_time;
	cerr << "Total execution time: " 
		<< (double) total_exec_time / CLOCKS_PER_SEC 
		<< " seconds." << endl;
#endif

	cout.flush();
	cerr.flush();
	return true;
}
