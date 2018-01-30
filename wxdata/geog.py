from geopy.distance import VincentyDistance
from geopy import Point


def dest_latlon(point, km, bearing):
    if not isinstance(point, Point):
        point = Point(*point)
    dist = VincentyDistance(kilometers=km)
    dest = dist.destination(point, bearing)
    return dest.latitude, dest.longitude


def bbox_zoom(loc, km_x, km_y):
    lat1, _ = dest_latlon(loc, km_y, 0)
    _, lon1 = dest_latlon(loc, km_x, 90)
    lat0, _ = dest_latlon(loc, km_y, 180)
    _, lon0 = dest_latlon(loc, km_x, 270)

    return (lon0, lon1, lat0, lat1)