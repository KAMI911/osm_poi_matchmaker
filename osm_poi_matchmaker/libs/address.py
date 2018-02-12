
try:
    import re
    import logging
except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)

# Patterns for re
PATTERN_POSTCODE_CITY = re.compile('^((\d){4})([.\s]{0,2})([a-zA-ZáÁéÉíÍóÓúÚüÜöÖőŐűŰ]{3,40})')
PATTERN_CITY = re.compile('\s?[XVI]{1,5}[.:,]{0,3}\s*$')
PATTERN_JS_2 = re.compile('\s*;\s*$')
PATTERN_HOUSENUMBER = re.compile('[0-9]{1,3}(\/[A-z]{1}|\-[0-9]{1,3}|)', re.IGNORECASE)
PATTERN_STREET = re.compile(
    '\s*.*\s*(útgyűrű|útfél|sétány|lejtő|liget|aluljáró|lejtó|park|ltp\.|ltp|sarok|szél|sor|körönd|köz|tér|tere|utca|körút|lakótelep|krt\.|krt|út|rét|sgt.|u\.|u){1}',
    re.UNICODE | re.IGNORECASE)
PATTERN_CONSCRIPTIONNUMBER = re.compile('(hrsz[.:]{0,2}\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1}|\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1}\s*hrsz[s.]{0,1})', re.IGNORECASE)
PATTERN_CONSCRIPTIONNUMBER_1 = re.compile('(hrsz[.:]{0,2}\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1})', re.IGNORECASE)
PATTERN_CONSCRIPTIONNUMBER_2 = re.compile('(\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1}\s*hrsz[s.]{0,1})', re.IGNORECASE)

def clean_javascript_variable(clearable, removable):
    """Remove javascript variable notation from the selected JSON variable.

    :param clearable: This is the text with Javascript text
    :param removable: The name of Javascript variable
    :return: Javascript clean text/JSON file
    """
    # Match on start
    PATTERN_JS = re.compile('^\s*var\s*{}\s*=\s*'.format(removable))
    data = re.sub(PATTERN_JS, '', clearable)
    # Match on end
    return re.sub(PATTERN_JS_2, '', data)


def extract_street_housenumber(clearable):
    '''Try to separate street and house number from a Hungarian style address string

    :param clearable: An input string with Hungarian style address string
    return: Separated street and housenumber
    '''
    # Split and clean up house number
    housenumber = clearable.split('(')[0]
    housenumber = housenumber.split(' ')[-1]
    housenumber = housenumber.replace('.', '')
    housenumber = housenumber.replace('–', '-')
    housenumber = housenumber.lower()
    # Split and clean up street
    street = clearable.split('(')[0]
    street = street.rsplit(' ', 1)[0]
    street = street.replace(' u.', ' utca')
    street = street.replace(' krt.', ' körút')
    return street, housenumber


def extract_all_address(clearable):
    clearable = clearable.strip()
    pc_match = PATTERN_POSTCODE_CITY.search(clearable)
    if pc_match is not None:
        postcode = pc_match.group(1)
    else:
        postcode = None
    if pc_match is not None:
        city = pc_match.group(4)
    else:
        city = None
    street, housenumber, conscriptionnumber = extract_street_housenumber_better(clearable.split(',')[1].strip())
    return (postcode, city, street, housenumber, conscriptionnumber)

def extract_street_housenumber_better(clearable):
    '''Try to separate street and house number from a Hungarian style address string

    :param clearable: An input string with Hungarian style address string
    return: Separated street and housenumber
    '''
    # Split and clean up street
    data = clearable.split('(')[0]

    cn_match_1 = PATTERN_CONSCRIPTIONNUMBER_1.search(data)
    cn_match_2 = PATTERN_CONSCRIPTIONNUMBER_2.search(data)
    if cn_match_1 is not None:
        conscriptionnumber = cn_match_1.group(2) if cn_match_1.group(2) is not None else None
        cnn_length = len(cn_match_1.group(0))
        logging.debug('Matching conscription number with method 1: {} from {}'.format(conscriptionnumber, clearable))
    elif cn_match_2 is not None:
        conscriptionnumber = cn_match_2.group(2) if cn_match_2.group(2) is not None else None
        cnn_length = len(cn_match_2.group(0))
        logging.debug('Matching conscription number with method 2: {} from {}'.format(conscriptionnumber, clearable))
    else:
        conscriptionnumber = None
        cnn_length = None
    # Try to match street
    street_match = PATTERN_STREET.search(data)
    if street_match is None:
        logging.debug('Non matching street: {}'.format(clearable))
        street, housenumber = None, None
    else:
        # Normalize street
        street = street_match.group(0)
        street_length = len(street)
        street = street.replace('Petőfi S.', 'Petőfi Sándor')
        street = street.replace('Szt.István', 'Szent István')
        street = street.replace('Kossuth L.', 'Kossuth Lajos')
        street = street.replace('Kossuth L.u.', 'Kossuth Lajos utca')
        street = street.replace('Ady E.', 'Ady Endre')
        street = street.replace('Erkel F.', 'Erkel Ferenc')
        street = street.replace('Bajcsy-Zs.', 'Bajcsy-Zsilinszky Endre')
        street = street.replace('Bethlen G.', 'Bethlen Gábor')
        street = street.replace('Dózsa Gy.', 'Dózsa György')
        street = street.replace('Mammut', '')
        street = street.replace('Jókai M.', 'Jókai Mór')
        street = street.replace('Szabó D.', 'Szabó Dezső')
        street = street.replace('Baross G.', 'Baross Gábor')
        street = street.replace('Kossuth F.', 'Kossuth F.')
        street = street.replace('Móricz Zs.', 'Móricz Zsigmond')
        street = street.replace('BERCSÉNYI U.', 'Bercsényi utca')
        street = street.replace(' u.',   ' utca')
        street = street.replace('.u.',   ' utca')
        street = street.replace(' krt.', ' körút')
        street = street.replace(' ltp.', ' lakótelep')
        street = street.replace(' ltp',  ' lakótelep')
        street = street.replace(' sgt.', ' sugárút')

        # Search for house number
        if cnn_length is not None:
            hn_match = PATTERN_HOUSENUMBER.search(data[street_length:-cnn_length])
        else:
            hn_match = PATTERN_HOUSENUMBER.search(data[street_length:])
        if hn_match is not None:
            # Split and clean up house number
            housenumber = hn_match.group(0)
            housenumber = housenumber.replace('.', '')
            housenumber = housenumber.replace('–', '-')
            housenumber = housenumber.lower()
        else:
            housenumber = None


    return street, housenumber, conscriptionnumber


def clean_city(clearable):
    '''Remove additional things from cityname

    :param clearable: Not clear cityname
    :return: Clear cityname
    '''
    city = re.sub(PATTERN_CITY, '', clearable)
    return city.strip()
