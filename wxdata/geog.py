import geopy
import geopy.distance
import math

from wxdata.utils import persistent_cache


def calc_bbox(ctr, west_east, north_south, dist_method='great_circle'):
    if len(ctr) != 2:
        raise ValueError("Center point must be a (lat, lon) pair")

    start = geopy.Point(*ctr)
    if dist_method == 'vincenty':
        dist_westeast = geopy.distance.VincentyDistance(kilometers=west_east)
        dist_northsouth = geopy.distance.VincentyDistance(kilometers=north_south)
    else:
        dist_westeast = geopy.distance.GreatCircleDistance(kilometers=west_east)
        dist_northsouth = geopy.distance.GreatCircleDistance(kilometers=north_south)

    lon0 = dist_westeast.destination(start, 270).longitude
    lon1 = dist_westeast.destination(start, 90).longitude
    lat0 = dist_northsouth.destination(start, 180).latitude
    lat1 = dist_northsouth.destination(start, 0).latitude

    return lon0, lon1, lat0, lat1


@persistent_cache(filename='cities')
def find_latlon(loc, geocodor=None):
    if geocodor is None:
        from geopy.geocoders import Nominatim
        geocodor = Nominatim()

    location = geocodor.geocode(loc)

    if not location:
        raise ValueError("Cannot find latlon for {}".format(loc))
    return location.latitude, location.longitude


def angle_between(pt1, pt2):
    lat1 = math.radians(pt1[0])
    lat2 = math.radians(pt2[0])
    difflong = math.radians(pt2[1] - pt1[1])

    x = math.sin(difflong) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) * math.cos(difflong))
    return math.atan2(y, x)


def dist_between(pt1, pt2):
    return geopy.distance.great_circle(pt1, pt2).km