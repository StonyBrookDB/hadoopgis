#include <iostream>
#include <sstream>
#include <cstring>
#include <cmath>
#include <map>
#include <cstdlib> 
#include <vector>
#include <list>
//#include <CGAL/Exact_predicates_inexact_constructions_kernel.h>
#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Polyhedron_3.h>
#include <CGAL/IO/Polyhedron_iostream.h>
#include <CGAL/box_intersection_d.h>
#include <CGAL/Bbox_3.h>
#include <CGAL/intersections.h>

#include <CGAL/Nef_polyhedron_3.h>
#include <CGAL/Triangulation_3.h>
#include <CGAL/convex_decomposition_3.h> 
#include <CGAL/Tetrahedron_3.h>


typedef CGAL::Bbox_3                                     Bbox;
//typedef CGAL::Exact_predicates_inexact_constructions_kernel  Kernel;
typedef CGAL::Exact_predicates_exact_constructions_kernel  Kernel;
typedef Kernel::Triangle_3                               Triangle;
typedef Kernel::Segment_3                                Segment;
typedef CGAL::Polyhedron_3<Kernel>                       Polyhedron;
typedef Polyhedron::Halfedge_const_handle                Halfedge_const_handle;
typedef Polyhedron::Vertex_iterator                     Vertex_iterator;
typedef Polyhedron::Facet_const_iterator                 Facet_const_iterator;
typedef Polyhedron::Facet_const_handle                   Facet_const_handle;
typedef CGAL::Box_intersection_d::Box_with_handle_d<double, 3, Facet_const_handle>               Box;

typedef CGAL::Nef_polyhedron_3<Kernel> Nef_polyhedron;
typedef Kernel::FT FT;
typedef Kernel::Vector_3  Vector_3;
typedef CGAL::Triangulation_3<Kernel> Triangulation;
typedef Triangulation::Point        CGAL_Point;
typedef Triangulation::Tetrahedron 	Tetrahedron;
typedef Nef_polyhedron::Volume_const_iterator Volume_const_iterator;

bool intersects(Polyhedron *P1, Polyhedron *P2) {
	return true;
	if(P1->is_closed() && P2->is_closed()) {
		//cerr << "got here 1" << endl;
		Nef_polyhedron N1(*P1);		
		Nef_polyhedron N2(*P2);
		//cerr << "got here 2" << endl;

		Nef_polyhedron inputpoly = N1 * N2;

		if(inputpoly.number_of_vertices() > 0) { return true; }
		else { return false; }
	}
	else
		cerr << "ERROR: Polyhedron is not closed!" << endl;

	return false;
}

bool intersects(const mbb_3d * m1, const mbb_3d *m2) {
	return !(m1->low[0] > m2->high[0] || m1->high[0] < m2->low[0] 
	      || m1->low[1] > m2->high[1] || m1->high[1] < m2->low[1]
	      || m1->low[2] > m2->high[2] || m1->high[2] < m2->low[2] );
}

/* Obtain a structure containing mbb of the object  */
/*
struct mbb_3d* get_mbb(Polyhedron* poly) {
	double low[0], low[1], low[2], high[0], high[1], high[2];
	double tmp_x, tmp_y, tmp_z;
	Vertex_iterator begin = poly->vertices_begin();
	low[0] = high[0] = to_double(begin->point().x());
	low[1] = high[1] = to_double(begin->point().y());
	low[2] = high[2] = to_double(begin->point().z());
	begin++;
	for ( ; begin != poly->vertices_end(); ++begin) {
		tmp_x = to_double(begin->point().x());
		tmp_y = to_double(begin->point().y());
		tmp_z = to_double(begin->point().z());
		if (tmp_x < low[0]) min_x = tmp_x;
		if (tmp_y < low[1]) min_y = tmp_y;
		if (tmp_z < low[2]) min_z = tmp_z;
		if (tmp_x > high[0]) max_x = tmp_x;
		if (tmp_y > high[1]) max_y = tmp_y;
		if (tmp_z > high[2]) max_z = tmp_z;
	}
	struct mbb_3d* mbb_ptr = new mbb_3d();
	mbb_ptr->low[0] = min_x;
	mbb_ptr->low[1] = min_y;
	mbb_ptr->low[2] = min_z;
	mbb_ptr->high[0] = max_x;
	mbb_ptr->high[1] = max_y;
	mbb_ptr->high[2] = max_z;
	return mbb_ptr;
}
*/

double get_volume(Nef_polyhedron &inputpoly) {
	// to check if the intersected object can be converted to polyhedron or not
	vector<Polyhedron> PList;
	if(inputpoly.is_simple()) {
		Polyhedron P;
		inputpoly.convert_to_polyhedron(P);
		PList.push_back(P);
	}
	else {
		// decompose non-convex volume to convex parts
		convex_decomposition_3(inputpoly); 
		Volume_const_iterator ci = ++inputpoly.volumes_begin();
		for( ; ci != inputpoly.volumes_end(); ++ci) {
			if(ci->mark()) {
				Polyhedron P;
				inputpoly.convert_inner_shell_to_polyhedron(ci->shells_begin(), P);
				PList.push_back(P);
			}
		}
	}
	//cout<< "# of Polyhedrons: " << PList.size() <<endl;
	// triangulate the polyhedrons to generate mesh and use terahedron to calculate volumes
	Polyhedron poly;
	double total_volume = 0, hull_volume = 0;
	for(int i = 0; i < PList.size(); i++) {
		poly = PList[i];
		vector<CGAL_Point> L;
		for (Polyhedron::Vertex_const_iterator  it = poly.vertices_begin(); it != poly.vertices_end(); it++) { 
			L.push_back(CGAL_Point(it->point().x(), it->point().y(), it->point().z())); 
		}
		Triangulation T(L.begin(), L.end()); 
		hull_volume = 0; 
		for(Triangulation::Finite_cells_iterator it = T.finite_cells_begin(); it != T.finite_cells_end(); it++) { 
			Tetrahedron tetr = T.tetrahedron(it); 
			hull_volume += to_double(tetr.volume());
		}

		total_volume += hull_volume;
	}
	return total_volume;
}
