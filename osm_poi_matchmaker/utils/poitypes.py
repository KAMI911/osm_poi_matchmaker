# -*- coding: utf-8 -*-
__author__ = 'kami911'

try:
    import logging
    import sys
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)


def getPOITypes(ptype):
    distance = config.get_geo_default_poi_distance()
    if ptype == 'shop':
        query_type = "shop='convenience' OR shop='supermarket'"
        distance = config.get_geo_shop_poi_distance()
    elif ptype == 'fuel':
        query_type = "amenity='fuel'"
    elif ptype == 'bank':
        query_type = "amenity='bank'"
    elif ptype == 'atm':
        query_type = "amenity='atm'"
        distance = config.get_geo_amenity_atm_poi_distance()
    elif ptype == 'post_office':
        query_type = "amenity='post_office'"
        distance = config.get_geo_amenity_post_office_poi_distance()
    elif ptype == 'vending_machine':
        query_type = "amenity='vending_machine'"
    elif ptype == 'pharmacy':
        query_type = "amenity='pharmacy'"
    elif ptype == 'chemist':
        query_type = "shop='chemist'"
    elif ptype == 'bicycle_rental':
        query_type = "amenity='bicycle_rental'"
    elif ptype == 'vending_machine_cheques':
        query_type = "amenity='vending_machine' AND vending='cheques'"
    elif ptype == 'vending_machine_parcel_locker':
        # New scheme: https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dparcel_locker
        # Old (also supported to find) scheme was: https://wiki.openstreetmap.org/wiki/Tag:vending%3Dparcel_pickup
        query_type = "amenity='parcel_locker' OR vending='parcel_pickup'"
    elif ptype == 'vending_machine_parcel_mail_in':
        # New scheme: https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dparcel_locker
        # New scheme: https://wiki.openstreetmap.org/wiki/Key:parcel_mail_in (yes)
        # New scheme: https://wiki.openstreetmap.org/wiki/Key:parcel_pickup (no)
        query_type = "(amenity='parcel_locker' AND parcel_mail_in='yes' AND parcel_pickup='no') OR " \
                     "(amenity='parcel_locker') OR " \
                     "vending='parcel_mail_in'"
    elif ptype == 'vending_machine_parcel_locker_and_mail_in':
        # New scheme: https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dparcel_locker
        # New scheme: https://wiki.openstreetmap.org/wiki/Key:parcel_mail_in (yes)
        # New scheme: https://wiki.openstreetmap.org/wiki/Key:parcel_pickup (no)
        query_type = "(amenity='parcel_locker' AND parcel_mail_in='yes' AND parcel_pickup='no') OR " \
                     "(amenity='parcel_locker') OR " \
                     "(amenity='vending_machine' AND" \
                     "(vending='parcel_pickup;parcel_mail_in' OR vending='parcel_mail_in;parcel_pickup') OR " \
                     "vending='parcel_pickup')"
    elif ptype == 'vending_machine_parking_tickets':
        query_type = "amenity='vending_machine' AND vending='parking_tickets'"
    elif ptype == 'tobacco':
        query_type = "shop='tobacco'"
    elif ptype == 'clothes':
        query_type = "shop='clothes' OR shop='fashion'"
    elif ptype == 'doityourself':
        query_type = "shop='doityourself'"
    elif ptype == 'cosmetics':
        query_type = "shop='cosmetics' OR shop='beauty'"
    elif ptype == 'furniture':
        query_type = "shop='furniture'"
    elif ptype == 'charging_station':
        query_type = "amenity='charging_station'"
    elif ptype == 'waterway_fuel':
        query_type = "waterway='fuel'"
    elif ptype == 'fastfood':
        query_type = "amenity='fastfood'"
    elif ptype == 'shoes':
        query_type = "shop='shoes'"
    elif ptype == 'optician':
        query_type = "shop='optician'"
    elif ptype == 'bus_stop':
        query_type = "highway='bus_stop'"  # OR public_transport='stop_area'"
    elif ptype == 'train_stop':
        query_type = "railway='train_stop'"
    elif ptype == 'fire_station':
        query_type = "amenity='fire_station'"
    else:
        query_type = None
        distance = 0
    return query_type, distance
