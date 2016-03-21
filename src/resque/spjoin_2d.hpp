

// performs spatial join on the current tile (bucket)
int join_bucket_spjoin(struct query_op &stop, struct query_temp &sttemp) {
	IStorageManager *storage = NULL;
	ISpatialIndex *spidx = NULL;
	bool selfjoin = stop.join_cardinality == 1  ? true : false;

	/* Indicates where original data is mapped to */
	int idx1 = SID_1; 
	int idx2 = selfjoin ? SID_1 : SID_2;
	
	int pairs = 0; // number of satisfied results

	double low[2], high[2];  // Temporary value placeholders for MBB

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
		// build the actual spatial index for input polygons from idx2
		if (! build_index_geoms(geom_polygons2, spidx, storage)) {
			#ifdef DEBUG
			cerr << "Building index on geometries from set 2 has failed" << endl;
			#endif
			return -1;
		}

		for (int i = 0; i < len1; i++) {		
			/* Extract minimum bounding box */
			const Geometry* geom1 = poly_set_one[i];
			const Envelope * env1 = geom1->getEnvelopeInternal();

			low[0] = env1->getMinX();
			low[1] = env1->getMinY();
			high[0] = env1->getMaxX();
			high[1] = env1->getMaxY();
			if (stop.join_predicate == ST_DWITHIN) {
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
			
			for (uint32_t j = 0; j < vis.matches.size(); j++) 
			{
				/* Skip results that have been seen before (self-join case) */
				if (selfjoin && ((vis.matches[j] == i) ||  // same objects in self-join
				    (!stop.result_pair_duplicate && vis.matches[j] <= i))) { // duplicate pairs
					#ifdef DEBUG
					cerr << "skipping (selfjoin): " << j << " " << vis.matches[j] << endl;
					#endif
					continue;
				}
				const Geometry* geom2 = poly_set_two[vis.matches[j]];
				const Envelope * env2 = geom2->getEnvelopeInternal();
				if (join_with_predicate(stop, sttemp, geom1, geom2, env1, env2,
							stop.join_predicate))  {
					report_result(stop, sttemp, i, vis.matches[j]);
					pairs++;
				}

			}
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


/* Perform (Refine) spatial computation between 2 geometry objects */
bool join_with_predicate(
		struct query_op &stop, struct query_temp &sttemp, 
		const geos::geom::Geometry * geom1 , const geos::geom::Geometry * geom2, 
		const geos::geom::Envelope * env1, const geos::geom::Envelope * env2,
		const int jp){
	bool flag = false; // flag == true means the predicate is satisfied 
	
	BufferOp * buffer_op1;
	BufferOp * buffer_op2;
	Geometry* geom_buffer1;
	Geometry* geom_buffer2;
	Geometry* geomUni;
	Geometry* geomIntersect; 

	
	#ifdef DEBUG
	
	cerr << "1: (envelope) " << env1->toString() 
		<< " and actual geom: " << geom1->toString()
		<< " 2: (envelope)" << env2->toString() 
		<< " and actual geom: " << geom2->toString() << endl;
	
	#endif

	switch (jp){
		case ST_INTERSECTS:
			flag = geom1->intersects(geom2);
			break;

		case ST_TOUCHES:
			flag = geom1->touches(geom2);
			break;

		case ST_CROSSES:
			flag = geom1->crosses(geom2);
			break;

		case ST_CONTAINS:
			flag = env1->contains(env2) && geom1->contains(geom2);
			break;

		case ST_ADJACENT:
			flag = !geom1->disjoint(geom2);
			break;

		case ST_DISJOINT:
			flag = geom1->disjoint(geom2);
			break;

		case ST_EQUALS:
			flag = env1->equals(env2) && geom1->equals(geom2);
			break;

		case ST_DWITHIN:
			/* Special spatial handling for the point-point case */
			if (geom1->getGeometryTypeId() == geos::geom::GEOS_POINT 
				&& geom2->getGeometryTypeId() == geos::geom::GEOS_POINT) 				{
				/* Replace with spherical distance computation if points are on eath */
				if (stop.use_earth_distance) {
					flag = get_distance_earth(
						dynamic_cast<const geos::geom::Point*>(geom1),
						dynamic_cast<const geos::geom::Point*>(geom2)) 
						<= stop.expansion_distance;
				} else {
					flag = DistanceOp::distance(geom1, geom2)
						<= stop.expansion_distance;
				}

				/* flag = distance(
					dynamic_cast<const geos::geom::Point*>(geom1), 
					dynamic_cast<const geos::geom::Point*>(geom2) ) 
					 <= stop.expansion_distance; */
			}
			else {
				// Regular handling for other object types
				buffer_op1 = new BufferOp(geom1);
				// buffer_op2 = new BufferOp(geom2);
				if (NULL == buffer_op1)
					cerr << "NULL: buffer_op1" <<endl;
				geom_buffer1 = buffer_op1->getResultGeometry(stop.expansion_distance);
				env1 = geom_buffer1->getEnvelopeInternal();
				// geom_buffer2 = buffer_op2->getResultGeometry(expansion_distance);
				//Envelope * env_temp = geom_buffer1->getEnvelopeInternal();
				if (NULL == geom_buffer1) {
					cerr << "NULL: geom_buffer1" << endl;
				}
				flag = join_with_predicate(stop, sttemp, geom_buffer1, geom2, 
					env1, env2, ST_INTERSECTS);
				delete geom_buffer1;
				delete buffer_op1;
			}
			break;

		case ST_WITHIN:
			flag = geom1->within(geom2);
			break; 

		case ST_OVERLAPS:
			flag = geom1->overlaps(geom2);
			break;
		/*
		case ST_NEAREST:
		case ST_NEAREST_2:
			// Execution only reaches here if this is already the nearest neighbor
			flag = true;
			break;
		*/
		default:
			cerr << "ERROR: unknown spatial predicate " << endl;
			break;
	}
	/* Spatial computation is only performed once for a result pair */
	if (flag) {
		if (stop.needs_area_1) {
			sttemp.area1 = geom1->getArea();
		}
		if (stop.needs_area_2) {
			sttemp.area2 = geom2->getArea();
		}
		if (stop.needs_union) {
			Geometry * geomUni = geom1->Union(geom2);
			sttemp.union_area = geomUni->getArea();
			delete geomUni;
		}
		if (stop.needs_intersect) {
			Geometry * geomIntersect = geom1->intersection(geom2);
			sttemp.intersect_area = geomIntersect->getArea();
			delete geomIntersect;
		}
		/* Statistics dependent on previously computed statistics */
		if (stop.needs_jaccard) {
			sttemp.jaccard = compute_jaccard(sttemp.union_area, sttemp.intersect_area);
		}

		if (stop.needs_dice) {
			sttemp.dice = compute_dice(sttemp.area1, sttemp.area2, sttemp.intersect_area);
		}

		if (stop.needs_min_distance) {
			if (stop.use_earth_distance
				&& geom1->getGeometryTypeId() == geos::geom::GEOS_POINT
				&& geom2->getGeometryTypeId() == geos::geom::GEOS_POINT) 				{
				sttemp.distance = get_distance_earth(
						dynamic_cast<const geos::geom::Point*>(geom1),
						dynamic_cast<const geos::geom::Point*>(geom2)); 
			}
			else {
				sttemp.distance = DistanceOp::distance(geom1, geom2);
			}
		}
	}
	return flag; 
}

