/* Compute the Jaccard similarity measure */
double compute_jaccard(double union_area, double intersection_area)
{
	return intersection_area / union_area;
}

/* Compute the DICE similarity measure */
double compute_dice(double area1, double area2, double intersection_area)
{
	return 2 * intersection_area / (area1 + area2);
}


