# -*- coding: utf-8 -*-

POI_COLS_RAW = ['poi_code', 'poi_postcode', 'poi_city', 'poi_name', 'poi_branch',
            'poi_website', 'poi_description',
            'poi_fuel_adblue', 'poi_fuel_octane_100', 'poi_fuel_octane_98',
            'poi_fuel_octane_95', 'poi_fuel_diesel_gtl', 'poi_fuel_diesel',
            'poi_fuel_lpg', 'poi_fuel_e85', 'poi_rent_lpg_bottles',
            'poi_compressed_air', 'poi_restaurant', 'poi_food', 'poi_truck',
            'poi_authentication_app', 'poi_authentication_membership_card',
            'poi_capacity', 'poi_fee', 'poi_parking_fee', 'poi_motorcar',
            'poi_socket_chademo', 'poi_socket_chademo_output',
            'poi_socket_chademo_current', 'poi_socket_chademo_voltage',
            'poi_socket_type2_combo', 'poi_socket_type2_combo_output',
            'poi_socket_type2_combo_current', 'poi_socket_type2_combo_voltage',
            'poi_socket_type2_cable', 'poi_socket_type2_cable_output',
            'poi_socket_type2_cable_current', 'poi_socket_type2_cable_voltage',
            'poi_socket_type2_cableless', 'poi_socket_type2_cableless_output',
            'poi_socket_type2_cableless_current', 'poi_socket_type2_cableless_voltage',
            'poi_manufacturer', 'poi_model',
            'original', 'poi_addr_street', 'poi_addr_housenumber', 'poi_conscriptionnumber',
            'poi_ref', 'poi_additional_ref', 'poi_phone', 'poi_mobile', 'poi_email',
            'poi_geom',
            'poi_opening_hours_nonstop',
            'poi_opening_hours_mo_open',
            'poi_opening_hours_tu_open',
            'poi_opening_hours_we_open',
            'poi_opening_hours_th_open',
            'poi_opening_hours_fr_open',
            'poi_opening_hours_sa_open',
            'poi_opening_hours_su_open',
            'poi_opening_hours_mo_close',
            'poi_opening_hours_tu_close',
            'poi_opening_hours_we_close',
            'poi_opening_hours_th_close',
            'poi_opening_hours_fr_close',
            'poi_opening_hours_sa_close',
            'poi_opening_hours_su_close',
            'poi_opening_hours_summer_mo_open',
            'poi_opening_hours_summer_tu_open',
            'poi_opening_hours_summer_we_open',
            'poi_opening_hours_summer_th_open',
            'poi_opening_hours_summer_fr_open',
            'poi_opening_hours_summer_sa_open',
            'poi_opening_hours_summer_su_open',
            'poi_opening_hours_summer_mo_close',
            'poi_opening_hours_summer_tu_close',
            'poi_opening_hours_summer_we_close',
            'poi_opening_hours_summer_th_close',
            'poi_opening_hours_summer_fr_close',
            'poi_opening_hours_summer_sa_close',
            'poi_opening_hours_summer_su_close',
            'poi_opening_hours_lunch_break_start', 'poi_opening_hours_lunch_break_stop',
            'poi_public_holiday_open', 'poi_opening_hours', 'poi_lat', 'poi_lon'
            ]
POI_COLS = POI_COLS_RAW + ['poi_good', 'poi_bad']
POI_ADDR_COLS = ['poi_postcode', 'poi_city', 'poi_addr_street', 'poi_addr_housenumber',
                 'poi_addr_conscriptionnumber'
                 ]
OSM_ADDR_COLS = ['addr:postcode', 'addr:city', 'addr:street', 'addr:housenumber',
                 'addr:conscriptionnumber'
                 ]

POI_DB_RAW = ['pa_id', 'poi_common_id', 'poi_name', 'poi_branch', 'poi_addr_city', 'poi_postcode', 'poi_city', 'poi_addr_street', 'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_geom', 'original', 'poi_website', 'poi_description', 'poi_fuel_adblue', 'poi_fuel_octane_100', 'poi_fuel_octane_98', 'poi_fuel_octane_95', 'poi_fuel_diesel_gtl', 'poi_fuel_diesel', 'poi_fuel_lpg', 'poi_fuel_e85', 'poi_rent_lpg_bottles', 'poi_compressed_air', 'poi_restaurant', 'poi_food', 'poi_truck', 'poi_ref', 'poi_additional_ref', 'poi_phone', 'poi_mobile', 'poi_email', 'poi_authentication_app', 'poi_authentication_none', 'poi_authentication_membership_card', 'poi_capacity', 'poi_fee', 'poi_parking_fee', 'poi_motorcar', 'poi_socket_chademo', 'poi_socket_chademo_output', 'poi_socket_chademo_current', 'poi_socket_chademo_voltage', 'poi_socket_type2_combo', 'poi_socket_type2_combo_output', 'poi_socket_type2_combo_current', 'poi_socket_type2_combo_voltage', 'poi_socket_type2_cable', 'poi_socket_type2_cable_output', 'poi_socket_type2_cable_current', 'poi_socket_type2_cable_voltage', 'poi_socket_type2_cableless', 'poi_socket_type2_cableless_output', 'poi_socket_type2_cableless_curent', 'poi_socket_type2_cableless_voltage', 'poi_manufacturer', 'poi_model', 'poi_opening_hours_nonstop', 'poi_opening_hours_mo_open', 'poi_opening_hours_tu_open', 'poi_opening_hours_we_open', 'poi_opening_hours_th_open', 'poi_opening_hours_fr_open', 'poi_opening_hours_sa_open', 'poi_opening_hours_su_open', 'poi_opening_hours_mo_close', 'poi_opening_hours_tu_close', 'poi_opening_hours_we_close', 'poi_opening_hours_th_close', 'poi_opening_hours_fr_close', 'poi_opening_hours_sa_close', 'poi_opening_hours_su_close', 'poi_opening_hours_summer_mo_open', 'poi_opening_hours_summer_tu_open', 'poi_opening_hours_summer_we_open', 'poi_opening_hours_summer_th_open', 'poi_opening_hours_summer_fr_open', 'poi_opening_hours_summer_sa_open', 'poi_opening_hours_summer_su_open', 'poi_opening_hours_summer_mo_close', 'poi_opening_hours_summer_tu_close', 'poi_opening_hours_summer_we_close', 'poi_opening_hours_summer_th_close', 'poi_opening_hours_summer_fr_close', 'poi_opening_hours_summer_sa_close', 'poi_opening_hours_summer_su_close', 'poi_opening_hours_lunch_break_start', 'poi_opening_hours_lunch_break_stop', 'poi_public_holiday_open', 'poi_opening_hours', 'poi_lat', 'poi_lon', 'poi_created', 'poi_updated', 'poi_deleted' ]
POI_DB =     ['pa_id', 'poi_common_id', 'poi_name', 'poi_branch', 'poi_addr_city', 'poi_postcode', 'poi_city', 'poi_addr_street', 'poi_addr_housenumber', 'poi_conscriptionnumber', 'poi_geom', 'original', 'poi_website', 'poi_description', 'poi_fuel_adblue', 'poi_fuel_octane_100', 'poi_fuel_octane_98', 'poi_fuel_octane_95', 'poi_fuel_diesel_gtl', 'poi_fuel_diesel', 'poi_fuel_lpg', 'poi_fuel_e85', 'poi_rent_lpg_bottles', 'poi_compressed_air', 'poi_restaurant', 'poi_food', 'poi_truck', 'poi_ref', 'poi_additional_ref', 'poi_phone', 'poi_mobile', 'poi_email', 'poi_authentication_app', 'poi_authentication_none', 'poi_authentication_membership_card', 'poi_capacity', 'poi_fee', 'poi_parking_fee', 'poi_motorcar', 'poi_socket_chademo', 'poi_socket_chademo_output', 'poi_socket_chademo_current', 'poi_socket_chademo_voltage', 'poi_socket_type2_combo', 'poi_socket_type2_combo_output', 'poi_socket_type2_combo_current', 'poi_socket_type2_combo_voltage', 'poi_socket_type2_cable', 'poi_socket_type2_cable_output', 'poi_socket_type2_cable_current', 'poi_socket_type2_cable_voltage', 'poi_socket_type2_cableless', 'poi_socket_type2_cableless_output', 'poi_socket_type2_cableless_curent', 'poi_socket_type2_cableless_voltage', 'poi_manufacturer', 'poi_model', 'poi_opening_hours_nonstop', 'poi_opening_hours_mo_open', 'poi_opening_hours_tu_open', 'poi_opening_hours_we_open', 'poi_opening_hours_th_open', 'poi_opening_hours_fr_open', 'poi_opening_hours_sa_open', 'poi_opening_hours_su_open', 'poi_opening_hours_mo_close', 'poi_opening_hours_tu_close', 'poi_opening_hours_we_close', 'poi_opening_hours_th_close', 'poi_opening_hours_fr_close', 'poi_opening_hours_sa_close', 'poi_opening_hours_su_close', 'poi_opening_hours_summer_mo_open', 'poi_opening_hours_summer_tu_open', 'poi_opening_hours_summer_we_open', 'poi_opening_hours_summer_th_open', 'poi_opening_hours_summer_fr_open', 'poi_opening_hours_summer_sa_open', 'poi_opening_hours_summer_su_open', 'poi_opening_hours_summer_mo_close', 'poi_opening_hours_summer_tu_close', 'poi_opening_hours_summer_we_close', 'poi_opening_hours_summer_th_close', 'poi_opening_hours_summer_fr_close', 'poi_opening_hours_summer_sa_close', 'poi_opening_hours_summer_su_close', 'poi_opening_hours_lunch_break_start', 'poi_opening_hours_lunch_break_stop', 'poi_public_holiday_open', 'poi_opening_hours', 'osm_changeset', 'osm_search_distance_unsafe', 'osm_search_distance_safe', 'osm_search_distance_perfect', 'osm_id', 'osm_timestamp', 'poi_tags', 'osm_live_tags', 'poi_type', 'osm_version', 'osm_node', 'poi_url_base', 'preserve_original_name', 'preserve_original_post_code', 'poi_search_name', 'poi_search_avoid_name', 'poi_lat', 'poi_lon', 'poi_good', 'poi_bad', 'poi_hash', 'poi_created', 'poi_updated', 'poi_deleted']