# -*- coding: utf-8 -*-
__author__ = 'kami911'

try:
    import traceback
    import logging
    from sys import exit
    from osm_poi_matchmaker.utils import config
except ImportError as err:
    logging.error('Error {0} import module: {1}'.format(__name__, err))
    logging.error(traceback.print_exc())
    exit(128)

def getPOITypes(ptype):
    distance = config.get_geo_default_poi_distance()
    if ptype == 'shop':
        query_type = "shop='convenience' OR shop='supermarket'"
        distance = config.get_geo_shop_poi_distance()
    elif ptype == 'fuel':
        query_type = "amenity='fuel'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'bank':
        query_type = "amenity='bank'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'atm':
        query_type = "amenity='atm'"
        distance = config.get_geo_amenity_atm_poi_distance()
    elif ptype == 'post_office':
        query_type = "amenity='post_office'"
        distance = config.get_geo_amenity_post_office_poi_distance()
    elif ptype == 'vending_machine':
        query_type = "amenity='vending_machine'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'pharmacy':
        query_type = "amenity='pharmacy'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'chemist':
        query_type = "shop='chemist'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'bicycle_rental':
        query_type = "amenity='bicycle_rental'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'vending_machine_cheques':
        query_type = "amenity='vending_machine' AND vending='cheques'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'vending_machine_parcel_pickup':
        query_type = "amenity='vending_machine' AND vending='parcel_pickup'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'vending_machine_parcel_mail_in':
        query_type = "amenity='vending_machine' AND vending='parcel_mail_in'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'vending_machine_parcel_pickup_and_mail_in':
        query_type = "amenity='vending_machine' AND" \
                     "(vending='parcel_pickup;parcel_mail_in' OR vending='parcel_mail_in;parcel_pickup' OR vending='parcel_pickup')"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'vending_machine_parking_tickets':
        query_type = "amenity='vending_machine' AND vending='parking_tickets'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'tobacco':
        query_type = "shop='tobacco'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'clothes':
        query_type = "shop='clothes' OR shop='fashion'"
        distance = config.get_geo_default_poi_distance()
    elif ptype == 'doityourself':
        query_type = "shop='doityourself'"
        distance = config.get_geo_default_poi_distance()
    else:
        query_type = None
        distance = 0
    return query_type, distance
