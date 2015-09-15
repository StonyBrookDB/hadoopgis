
/* Convert from degrees to radians */
double to_radians(double degrees) 
{
  double radians = degrees * M_PI / 180;
  return radians;
}

/* Compute the earth spherical distance between two points
	 given latitude and longtitude of points */
double earth_distance(double lat1, double lng1, double lat2, double lng2) 
{
  double dLat = to_radians(lat2-lat1);
  double dLng = to_radians(lng2-lng1);
  double a = sin(dLat/2) * sin(dLat/2) + 
             cos(to_radians(lat1)) * cos(to_radians(lat2)) * 
             sin(dLng/2) * sin(dLng/2);
  double c = 2 * atan2(sqrt(a), sqrt(1-a));
  double dist = EARTH_RADIUS * c;
  double meterConversion = 1609.00;
  return dist * meterConversion;
}

