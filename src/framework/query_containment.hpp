
bool sp_containment(string programpath, vector<string> input_paths, string output_path,
	string original_params, struct framework_vars &fr_vars, 
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
	arr_args.push_back(fr_vars.output_path);

	arr_args.push_back("-file");
	arr_args.push_back(fr_vars.hadoopgis_prefix + RESQUE);
	arr_args.push_back("-file");
	string strtmp(cachefilefullpath);
	arr_args.push_back(strtmp);

	arr_args.push_back("-mapper");
	stringstream ss;
	ss << RESQUE << " -o 0 " << original_params <<  " -c " << cachefilename;
	arr_args.push_back(ss.str());

	arr_args.push_back("-numReduceTasks");
	arr_args.push_back("0");
	arr_args.push_back("-jobconf");
	arr_args.push_back("mapreduce.task.timeout=36000000");
	arr_args.push_back("-cmdenv");
	arr_args.push_back(fr_vars.hadoopldlibpath);

	#ifdef DEBUG
	cerr << "Executing containment params: ";
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
			cerr << "Succeeded in containment: " << status << endl;
			#endif
		} else {
			#ifdef DEBUG
			cerr << "Failed in containment: " << status << endl;
			#endif
			exit(1);
		}
		return status == 0 ? true : false;
	}
	return false;
}


bool execute_containment(struct framework_vars &fr_vars) {
	if (fr_vars.query_type.compare(QUERYPROC_CONTAINMENT) == 0) {
		#ifdef DEBUG
		cerr << "Executing a containment query" << endl;
		#endif
		
		// Check if the partidx file exists
		if (!hdfs_check_data(fr_vars.hadoopcmdpath, fr_vars.partidx_final)
			|| !hdfs_check_data(fr_vars.hadoopcmdpath, fr_vars.config_final)) {
			#ifdef DEBUG
			cerr << "Data was not loaded" << endl;
			#endif
			exit(1);
		}

		if (!hdfs_cat(fr_vars.hadoopcmdpath, fr_vars.config_final)) {
			#ifdef DEBUG
			cerr << "Error reading from pipe" << endl;
			#endif
			exit(1);
			
		}

		FILE *instr = fdopen(flinfo.pipefd[0], "r");
		fscanf(instr, "%d %lf %lf %lf %lf %ld", &fr_vars.shp_idx_1,
			&(fr_vars.spinfo.space_low[0]), &(fr_vars.spinfo.space_low[1]),
			&(fr_vars.spinfo.space_high[0]), &(fr_vars.spinfo.space_high[1]), 
			&(fr_vars.spinfo.num_objects));
		fclose(instr);

		// Retrieve window query
		double low[2];
		double high[2];
		double wlow[2];
		double whigh[2];
		string tmp_line;
		if (fr_vars.containment_use_file) {
			ifstream inputfile(fr_vars.user_file.c_str());
			getline(inputfile, tmp_line);
			get_mbb(tmp_line, wlow, whigh);
			inputfile.close();
		}  else {
			get_mbb(fr_vars.containment_window, wlow, whigh);
		}
		tmp_line = get_wkt_from_mbb(wlow, whigh);
		#ifdef DEBUG
		cerr << "Window range: " << TAB << wlow[0] << TAB << wlow[1] 
				<< TAB << whigh[0] << TAB << whigh[1] << endl;
		#endif

		if (!hdfs_cat(fr_vars.hadoopcmdpath, fr_vars.partidx_final)) {
			#ifdef DEBUG
			cerr << "Error reading partition files from pipe" << endl;
			#endif
			exit(1);
		}

		char tile_id[1024];
		stringstream tmpss;

		vector<string> input_paths;
		instr = fdopen(flinfo.pipefd[0], "r");
		while (fscanf(instr,"%s %lf %lf %lf %lf", tile_id, &(low[0]), &(low[1]),
			&(high[0]), &(high[1])) != EOF) {

			// If the MBB of tile intersects with MBB of the window query
			if (intersects(low, high, wlow, whigh)) {
				tmpss.str("");
				tmpss << fr_vars.datapath << SLASH << tile_id << ".dat";
				input_paths.push_back(tmpss.str());
			}

			#ifdef DEBUG
			cerr << tile_id << TAB << low[0] << TAB << low[1] 
				<< TAB << high[0] << TAB << high[1] << endl;
			#endif
		}	
		fclose(instr);

		char *tmpFile = mktemp(nametemplate);
		ofstream ofs(tmpFile, ofstream::trunc);
		ofs << tmp_line << endl; // the file contains the WKT string of the window
		char *tmpnameonly = strrchr(tmpFile, '/'); // pointing to just the name of the cache file
		tmpnameonly++;
		ofs.close();

		if (fr_vars.overwritepath || !hdfs_check_data(fr_vars.hadoopcmdpath, fr_vars.output_path)) {
			#ifdef DEBUG
			cerr << "Executing containment" << endl;
			#endif
			if (!sp_containment(fr_vars.hadoopcmdpath, input_paths, fr_vars.output_path, 
				fr_vars.sharedparams, fr_vars,
				tmpnameonly, tmpFile)) {
				#ifdef DEBUG
				cerr << "Containment failed." << endl;
				#endif
				exit(1);
			}
		}

	}
}
