


/* Update the spatial dimension of the bucket */
void update_bucket_dimension(struct query_op &stop, struct query_temp &sttemp, 
	const Envelope * env) {
	if (sttemp.bucket_min_x >= env->getMinX()) {
		sttemp.bucket_min_x = env->getMinX();
	}
	if (sttemp.bucket_min_y >= env->getMinY()) {
		sttemp.bucket_min_y = env->getMinY();
	}
	if (sttemp.bucket_max_x <= env->getMaxX()) {
		sttemp.bucket_max_x = env->getMaxX();
	}
	if (sttemp.bucket_max_y <= env->getMaxY()) {
		sttemp.bucket_max_y = env->getMaxY();
	}
}

// Returns the number of satisfied pairs
int join_bucket_knn(struct query_op &stop, struct query_temp &sttemp)
{
	IStorageManager *storage = NULL;
	ISpatialIndex *spidx = NULL;
	bool selfjoin = stop.join_cardinality == 1  ? true : false;

	/* Indicates where original data is mapped to */
	int idx1 = SID_1; 
	int idx2 = selfjoin ? SID_1 : SID_2;
	
	int pairs = 0; // number of satisfied results

	double low[2], high[2];  // Temporary value placeholders for MBB
	double tmp_distance; // Temporary distance for nearest neighbor query
	double def_search_radius = -1; // Default search radius 
					//for nearest neighbor (NN) with unknown bounds
	double max_search_radius; // max_radius to search for NN

	try { 

		vector<Geometry*>  & poly_set_one = sttemp.polydata[idx1];
		vector<Geometry*>  & poly_set_two = sttemp.polydata[idx2];
		
		int len1 = poly_set_one.size();
		int len2 = poly_set_two.size();

		#ifdef DEBUG
		cerr << "Bucket size: " << len1 << " joining with " << len2 << endl;
		#endif
		cerr << "Bucket size: " << len1 << " joining with " << len2 << endl;

		if (len1 <= 0 || len2 <= 0) {
			return 0;
		}

		/* Build index on the "second data set */
		map<int, Geometry*> geom_polygons2;
		geom_polygons2.clear();

		// Make a copy of the vector to map to build index (API restriction)
		for (int j = 0; j < len2; j++) {
			geom_polygons2[j] = poly_set_two[j];
		}

		/* Handling for special nearest neighbor query */	
		
		if (stop.join_predicate == ST_NEAREST_2) {
			// Updating bucket information	
			if (len2 > 0) {
				const Envelope * envtmp = poly_set_two[0]->getEnvelopeInternal();
				sttemp.bucket_min_x = envtmp->getMinX();
				sttemp.bucket_min_y = envtmp->getMinY();
				sttemp.bucket_max_x = envtmp->getMaxX();
				sttemp.bucket_max_y = envtmp->getMaxY();
			}
			for (int j = 0; j < len1; j++) {
				update_bucket_dimension(stop, sttemp, poly_set_one[j]->getEnvelopeInternal());
			}
			if (!selfjoin) {
				for (int j = 0; j < len2; j++) {
					update_bucket_dimension(stop, sttemp, poly_set_two[j]->getEnvelopeInternal());
				}
			}
			
			max_search_radius = max(sttemp.bucket_max_x - sttemp.bucket_min_x, 
						sttemp.bucket_max_y - sttemp.bucket_min_y);
			def_search_radius = min(sqrt((sttemp.bucket_max_x - sttemp.bucket_min_x)
						* (sttemp.bucket_max_y - sttemp.bucket_min_y) 
						* stop.k_neighbors / len2), max_search_radius);
			if (def_search_radius == 0) {
				def_search_radius = DistanceOp::distance(poly_set_one[0], poly_set_two[0]);
			}
			#ifdef DEBUG
			cerr << "Bucket dimension min-max: " << sttemp.bucket_min_x << TAB 
				<< sttemp.bucket_min_y << TAB << sttemp.bucket_max_x
				<< TAB << sttemp.bucket_max_y << endl;
			cerr << "Width and height (x-y span) " 
			<< sttemp.bucket_max_x - sttemp.bucket_min_x
			<< TAB << sttemp.bucket_max_y - sttemp.bucket_min_y  << endl;
			#endif
		}

		// build the actual spatial index for input polygons from idx2
		if (! build_index_geoms(geom_polygons2, spidx, storage)) {
			#ifdef DEBUG
			cerr << "Building index on geometries from set 2 has failed" << endl;
			#endif
			return -1;
		}
		cerr << "done building indices" << endl;

		for (int i = 0; i < len1; i++) {		
			/* Extract minimum bounding box */
			const Geometry* geom1 = poly_set_one[i];
			const Envelope * env1 = geom1->getEnvelopeInternal();

			low[0] = env1->getMinX();
			low[1] = env1->getMinY();
			high[0] = env1->getMaxX();
			high[1] = env1->getMaxY();
			/* Handle the buffer expansion for R-tree in 
			the case of Dwithin and nearest neighbor predicate */
			if (stop.join_predicate == ST_NEAREST_2) {
				/* Nearest neighbor when max search radius 
					is not determined */
				stop.expansion_distance = def_search_radius;
				// Initial value
			}
			if (stop.join_predicate == ST_DWITHIN ||
				stop.join_predicate == ST_NEAREST) {
				low[0] -= stop.expansion_distance;
				low[1] -= stop.expansion_distance;
				high[0] += stop.expansion_distance;
				high[1] += stop.expansion_distance;
			}

			/* Regular handling */
			Region r(low, high, 2);
			MyVisitor vis;
			vis.matches.clear();
			/* R-tree intersection check */
			spidx->intersectsWithQuery(r, vis);

			/* Retrieve enough candidate neighbors */
			if (stop.join_predicate == ST_NEAREST_2) {
				double search_radius = def_search_radius;

				while (vis.matches.size() <= stop.k_neighbors + 1
					&& vis.matches.size() <= len2 // there can't be more neighbors than number of objects in the bucket
					&& search_radius <= sqrt(2) * max_search_radius) {
					// Increase the radius to find more neighbors
					// Stopping criteria when there are not enough neighbors in a tile
					low[0] = env1->getMinX() - search_radius;
					low[1] = env1->getMinY() - search_radius;
					high[0] = env1->getMaxX() + search_radius;
					high[1] = env1->getMaxY() + search_radius;
					
					Region r2(low, high, 2);
					
					vis.matches.clear();
					spidx->intersectsWithQuery(r2, vis);

					#ifdef DEBUG
					cerr << "Search radius:" << search_radius << " hits: " << vis.matches.size() << endl;
					#endif
					search_radius *= sqrt(2);
				}

				sttemp.nearest_distances.clear();

				/* Handle the special case of rectangular/circle expansion -sqrt(2) expansion */
				vis.matches.clear();
				low[0] = env1->getMinX() - search_radius;
				low[1] = env1->getMinY() - search_radius;
				high[0] = env1->getMaxX() + search_radius;
				high[1] = env1->getMaxY() + search_radius;
				Region r3(low, high, 2);
				spidx->intersectsWithQuery(r3, vis);
			}
			
			for (uint32_t j = 0; j < vis.matches.size(); j++) 
			{
				const Geometry* geom2 = poly_set_two[vis.matches[j]];
				const Envelope * env2 = geom2->getEnvelopeInternal();

				if (stop.join_predicate == ST_NEAREST 
					&& (!selfjoin || vis.matches[j] != i)) { // remove selfjoin candidates
					/* Handle nearest neighbor candidate */	
					tmp_distance = DistanceOp::distance(geom1, geom2);
					if (tmp_distance < stop.expansion_distance) {
						update_nn(stop, sttemp, vis.matches[j], tmp_distance);
					}
					
				}
				else if (stop.join_predicate == ST_NEAREST_2 
					&& (!selfjoin || vis.matches[j] != i)) {
					tmp_distance = DistanceOp::distance(geom1, geom2);
					update_nn(stop, sttemp, vis.matches[j], tmp_distance);
	//				cerr << "updating: " << vis.matches[j] << " " << tmp_distance << endl;
				}

			}
			/* Nearest neighbor outputting */
			for (std::list<struct query_nn_dist *>::iterator 
					it = sttemp.nearest_distances.begin(); 
					it != sttemp.nearest_distances.end(); it++) 
			{
					sttemp.distance = (*it)->distance;
					report_result(stop, sttemp, i, (*it)->object_id);	
					pairs++;
					/* Cleaning up memory */
					delete *it;
			}
			sttemp.nearest_distances.clear();
	
		}
	} // end of try
	catch (Tools::Exception& e) {
	//catch (...) {
		std::cerr << "******ERROR******" << std::endl;
		#ifdef DEBUG
		cerr << e.what() << std::endl;
		#endif
		return -1;
	} // end of catch

	cerr << "Done with tile" << endl;

	delete spidx;
	delete storage;
	return pairs ;
}

void update_nn(struct query_op &stop, struct query_temp &sttemp, int object_id, double distance)
{
	list<struct query_nn_dist *>::iterator it;
	bool new_inserted = false;
	struct query_nn_dist * tmp;

	for (it = sttemp.nearest_distances.begin(); 
		it != sttemp.nearest_distances.end(); it++) {
		if ((*it)->distance > distance) {
			/* Insert the new distance in */
			tmp = new struct query_nn_dist();
			tmp->distance = distance;
			tmp->object_id = object_id;
			sttemp.nearest_distances.insert(it, tmp);
			new_inserted = true;
			break;
		}
    	}

	if (sttemp.nearest_distances.size() > stop.k_neighbors) {
		// the last element is the furthest from all tracked nearest neighbors
		tmp = sttemp.nearest_distances.back();
		sttemp.nearest_distances.pop_back(); 
		delete tmp;
	} else if (!new_inserted && 
		sttemp.nearest_distances.size() < stop.k_neighbors) {
		/* Insert at the end */
		tmp = new struct query_nn_dist();
		tmp->distance = distance;
		tmp->object_id = object_id;
		sttemp.nearest_distances.insert(it, tmp);
	}

}

