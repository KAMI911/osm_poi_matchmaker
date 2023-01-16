# -*- coding: utf-8 -*-

try:
    import logging
    import sys
    import re
    import phonenumbers
    import json
    import traceback
    import math
    import osm_poi_matchmaker.libs.waxeye
    import osm_poi_matchmaker.libs.hu.hu_address_parser as hu_address_parser
    from osm_poi_matchmaker.libs.waxeye_process import waxeye_process

    from functools import reduce
except ImportError as err:
    logging.error('Error %s import module: %s', __name__, err)
    logging.exception('Exception occurred')

    sys.exit(128)

# Patterns for re
PATTERN_POSTCODE_CITY = re.compile('^((\d){4})([.\s]{0,2})([a-zA-ZáÁéÉíÍóÓúÚüÜöÖőŐűŰ]{3,40})')
PATTERN_CITY_ADDRESS = re.compile('^([a-zA-ZáÁéÉíÍóÓúÚüÜöÖőŐűŰ]{3,40})')
PATTERN_CITY = re.compile('\s?[XVI]{1,5}[.:,]{0,3}\s*$')
PATTERN_JS_2 = re.compile('\s*;\s*$')
PATTERN_HOUSENUMBER = re.compile('[0-9]{1,3}(\/[A-z]{1}|\-[0-9]{1,3}|)', re.IGNORECASE)
PATTERN_CONSCRIPTIONNUMBER = re.compile(
    '(hrsz[.:]{0,2}\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1}|\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1}\s*hrsz[s.]{0,1})',
    re.IGNORECASE)
PATTERN_CONSCRIPTIONNUMBER_1 = re.compile('((?:belterület\s*)?hrsz[.:]{0,2}\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1})', re.IGNORECASE)
PATTERN_CONSCRIPTIONNUMBER_2 = re.compile('(\s*([0-9]{2,6}(\/[0-9]{1,3}){0,1})[.]{0,1}\s*hrsz[s.]{0,1})', re.IGNORECASE)
PATTERN_OPENING_HOURS = re.compile('0*[0-9]{1,2}\:0*[0-9]{1,2}\s*-\s*0*[0-9]{1,2}:0*[0-9]{1,2}')
PATTERN_PHONE = re.compile('[\/\\\(\)\-\+]')

PATTERN_STREET_RICH = re.compile(
    '\s*(.*)\s+(akna|alja|almáskert|alsó|alsósor|aluljáró|autópálya|autóversenypálya|állomás|árok|átjáró|barakképület|bánya|bányatelep|bekötőút|benzinkút|bérc|bisztró|bokor|burgundia|büfé|camping|campingsor|centrum|célgazdaság|csapás|csarnok|csárda|cser|csoport|domb|dunapart|dunasor|dűlő|dűlője|dűlők|dűlőút|egyesület|egyéb|elágazás|erdeje|erdészház|erdészlak|erdő|erdősarok|erdősor|épület|épületek|észak|étterem|falu|farm|fasor|fasora|feketeerdő|feketeföldek|felső|felsősor|fennsík|fogadó|fok|forduló|forrás|föld|földek|földje|főcsatorna|főtér|főút|fürdő|fürdőhely|fürésztelepe|gazdaság|gát|gátőrház|gátsor|gimnázium|gödör|gulyakút|gyár|gyártelep|halom|határátkelőhely|határrész|határsor|határút|hatházak|hát|ház|háza|házak|hegy|hegyhát|hegyhát dűlő|hely|hivatal|híd|hídfő|horgásztanya|hotel|intézet|ipari park|ipartelep|iparterület|irodaház|irtás|iskola|jánoshegy|járás|juhászház|kapcsolóház|kapu|kastély|kálvária|kemping|kert|kertek|kertek-köze|kertsor|kertváros|kerület|kikötő|kilátó|kishajtás|kitérő|kocsiszín|kolónia|korzó|kórház|környék|körönd|körtér|körút|körútja|körvasútsor|körzet|köz|köze|középsor|központ|kút|kútház|kültelek|külterület|külterülete|lakás|lakások|lakóház|lakókert|lakónegyed|lakópark|lakótelep|laktanya|legelő|lejáró|lejtő|lépcső|liget|lovasiskola|lovastanya|magánút|major|malom|malomsor|megálló|mellékköz|mező|mélyút|MGTSZ|munkásszálló|műút|nagymajor|nagyút|nádgazdaság|nyaraló|oldal|országút|otthon|otthona|öböl|öregszőlők|ösvény|ötház|őrház|őrházak|pagony|pallag|palota|park|parkfalu|parkja|parkoló|part|pavilonsor|pálya|pályafenntartás|pályaudvar|piac|pihenő|pihenőhely|pince|pinceköz|pincesor|pincék|présházak|puszta|rakodó|rakpart|repülőtér|rész|rét|rétek|rév|ring|sarok|sertéstelep|sétatér|sétány|sikátor|sor|sora|sportpálya|sporttelep|stadion|strand|strandfürdő|sugárút|szabadstrand|szakiskola|szállás|szálló|szárító|szárnyasliget|szektor|szer|szél|széle|sziget|szigete|szivattyútelep|szög|szőlő|szőlőhegy|szőlők|szőlőkert|szőlős|szőlősor|tag|tanya|tanyaközpont|tanyasor|tanyák|tavak|tábor|tároló|társasház|teherpályaudvar|telek|telep|telepek|település|temető|tere|terményraktár|terület|teteje|tető|téglagyár|tér|tipegő|tormás|torony|tó|tömb|TSZ|turistaház|udvar|udvara|ugarok|utca|utcája|újfalu|újsor|újtelep|útfél|útgyűrű|útja|út|üdülő|üdülő központ|üdülő park|üdülők|üdülőközpont|üdülőpart|üdülő-part|üdülősor|üdülő-sor|üdülőtelep|üdülő-telep|üdülőterület|ürbő|üzem|üzletház|üzletsor|vadászház|varroda|vasútállomás|vasúti megálló|vasúti őrház|vasútsor|vám|vár|város|városrész|vásártér|vendéglő|vég|villa|villasor|viztároló|vízmű|vízmű telep|völgy|zsilip|zug|ltp\.|ltp|krt\.|krt|sgt\.|u\.|u\s+|Várkerület){1}.*',
    re.UNICODE | re.IGNORECASE)
PATTERN_URL_SLASH = re.compile('(?<!:)(//{1,})')
PATTERN_FULL_URL = re.compile('((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)')

SZFKL = '. számú főközlekedési út'


def remove_whitespace(wsp: str, rpl: str = '') -> str:
    """
    Remove whitespaces or replace to the selected sequence
    :param wsp: Text string to be cleared or replace.
    :param rpl: String replaced to.
    :return: Whitespaces cleaned text string.
    """
    pattern = re.compile(r'\s+')
    return re.sub(pattern, rpl, wsp)


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


def extract_javascript_variable(input_soup, removable, use_replace=False):
    """Extract JavaScript variable from <script> tag from a soup.

    :param sp: Input soup
    :param removable: The name of Javascript variable
    :param user_replace: Additional step to replace from ' to "
    :return: Javascript clean text/JSON file
    """
    # Match on start
    try:
        pattern = re.compile('.*\s*var\s*{}\s*=\s*(.*?[}}\]]);'.format(removable), re.MULTILINE | re.DOTALL)
        script = str(input_soup.find('script', text=pattern))
        if use_replace is True: script = script.replace("'", '"')
        m = pattern.match(script)
        try:
            if m is not None:
                return m.group(1)
            else:
                return None
        except AttributeError as e:
            logging.warning('An exception has occurred during JavaScript variable extraction.')
    except Exception as e:
        logging.error(e)
        logging.exception('Exception occurred')

        logging.error(pattern)
        logging.error(script)
        logging.error(m.group(1))


def extract_street_housenumber(clearable):
    '''Try to separate street and house number from a Hungarian style address string

    :param clearable: An input string with Hungarian style address string
    return: Separated street and housenumber
    '''
    # Split and clean up house number
    housenumber = clearable.split('(')[0]
    housenumber = clean_string(housenumber)
    if housenumber is not None:
        housenumber = housenumber.split(' ')[-1]
        housenumber = housenumber.replace('.', '')
        housenumber = housenumber.replace('–', '-')
        housenumber = housenumber.upper()
    # Split and clean up street
    street = clearable.split('(')[0]
    street = clean_string(street)
    if street is not None:
        street = street.rsplit(' ', 1)[0]
        street = street.replace(' u.', ' utca')
        street = street.replace(' u ', ' utca')
        street = street.replace(' krt.', ' körút')
    return street, housenumber


def extract_all_address(clearable):
    if clearable is not None and clearable != '':
        clearable = clean_string(clearable)
        pc_match = PATTERN_POSTCODE_CITY.search(clearable)
        if pc_match is not None:
            postcode = pc_match.group(1)
        else:
            postcode = None
        if pc_match is not None:
            city = pc_match.group(4)
        else:
            city = None
        if len(clearable.split(',')) > 1:
            street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(
                clearable.split(',')[-1].strip())
            return postcode, city, street, housenumber, conscriptionnumber
        else:
            space_separated = ' '.join(clearable.split(' ')[2:]).strip()
            street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(space_separated)
            return postcode, city, street, housenumber, conscriptionnumber
    else:
        return None, None, None, None, None

def extract_all_address_waxeye(clearable):
    clearable = clean_string(clearable)
    if clearable is not None and clearable != '':
        parsed_address = hu_address_parser.Parser().parse(clearable)
        address_dict = waxeye_process(parsed_address)
        postcode = address_dict.get('postcode')
        city = address_dict.get('cTown')
        housenumber = address_dict.get('houseNumber')
        street_name = address_dict.get('cStreet')
        street_type = address_dict.get('type')
        if street_name is not None and street_type is not None:
            street = f'{street_name} {street_type}'
        elif address_dict.get('cStreet') is not None:
            street = f'{street_name}'
        else:
            street = None
        conscriptionnumber = address_dict.get('conscriptionHrsz')
        return postcode, city, street, housenumber, conscriptionnumber
    else:
        return None, None, None, None, None


def extract_city_street_housenumber_address(clearable):
    if clearable is not None and clearable != '':
        clearable = clean_string(clearable)
        pc_match = PATTERN_CITY_ADDRESS.search(clearable)
        if pc_match is not None:
            city = pc_match.group(1)
            city = clean_string(city)
        else:
            city = None
        if len(clearable.split(',')) > 1:
            street, housenumber, conscriptionnumber = extract_street_housenumber_better_2(
                clearable.split(',')[1].strip())
            return city, street, housenumber, conscriptionnumber
        else:
            return city, None, None, None
    else:
        return None, None, None, None, None


def extract_street_housenumber_better_2(clearable: str) -> str:
    '''Try to separate street and house number from a Hungarian style address string

    :param clearable: An input string with Hungarian style address string
    return: Separated street and housenumber
    '''
    # Split and clean up street
    clearable = str(clearable)
    if clearable is not None and clearable.strip() != '':
        clearable = clean_string(str(clearable))
        # Remove bulding names
        clearable = clearable.replace(' - Savoya Park', '')
        clearable = clearable.replace('Park Center,', '')
        clearable = clearable.replace('Duna Center', '')
        clearable = clearable.replace('Family Center,', '')
        clearable = clearable.replace('Sostói ipari park, ', '')
        data = clearable.split('(')[0]
        cn_match_1 = PATTERN_CONSCRIPTIONNUMBER_1.search(data)
        cn_match_2 = PATTERN_CONSCRIPTIONNUMBER_2.search(data)
        if cn_match_1 is not None:
            conscriptionnumber = cn_match_1.group(2) if cn_match_1.group(2) is not None else None
            cnn_length = len(cn_match_1.group(0))
            logging.debug(
                'Matching conscription number with method 1: %s from %s', conscriptionnumber, clearable)
        elif cn_match_2 is not None:
            conscriptionnumber = cn_match_2.group(2) if cn_match_2.group(2) is not None else None
            cnn_length = len(cn_match_2.group(0))
            logging.debug(
                'Matching conscription number with method 2: %s from %s', conscriptionnumber, clearable)
        else:
            conscriptionnumber = None
            cnn_length = None
        # Try to match street
        street_corrected = clean_street(data)
        street_match = PATTERN_STREET_RICH.search(street_corrected)
        if street_match is None:
            logging.debug('Non matching street: %s', clearable)
            street, housenumber = None, None
        else:
            # Normalize street
            street = street_match.group(1)
            street_type = street_match.group(2)
            # Usually street_type is lower, but we got few exceptions
            if street_type not in ['Vám']:
                street_type = street_type.lower()
            street_length = len(street) + len(street_type)
            # Search for house number
            if cnn_length is not None:
                hn_match = PATTERN_HOUSENUMBER.search(street_corrected[street_length:-cnn_length])
            else:
                hn_match = PATTERN_HOUSENUMBER.search(street_corrected[street_length:])
            if hn_match is not None:
                # Split and clean up house number
                housenumber = hn_match.group(0)
                housenumber = housenumber.replace('.', '')
                housenumber = housenumber.replace('–', '-')
                housenumber = housenumber.upper()
            else:
                housenumber = None
        street = clean_string(street)
        housenumber = clean_string(housenumber)
        conscriptionnumber = clean_string(conscriptionnumber)
        if 'street_type' in locals():
            street_type = clean_string(street_type)
            return '{} {}'.format(street, street_type), housenumber, conscriptionnumber
        else:
            return street, housenumber, conscriptionnumber
    else:
        return None, None, None


def replace_html_newlines(clearable: str) -> str:
    repls = ('<br>', '; '), \
            ('</br>', '; '), \
            ('< br />', '; '), \
            ('<br />', '; '), \
            (' ;', ';')
    clearable = str(clearable)
    text = clean_string(clearable)
    if clearable is None:
        return None
    text = reduce(lambda a, kv: a.replace(*kv), repls, text)
    text = clean_string(text)
    return text


def extract_phone_number(data: str) -> str:
    '''Try to extract phone number

    :param a string contains not just phone number
    return: international format phone number string
    '''
    try:
        data = str(replace_html_newlines(data))
        if data is not None:
            fields = data.split(';')
            for f in fields:
                if 'Telefonszám' in f:
                    pn = f.split(':')[1]
                    try:
                        if '+36' not in pn:
                            pn = phonenumbers.parse(pn, 'HU')
                        else:
                            pn = phonenumbers.parse('+36 '.format(pn), 'HU')
                    except phonenumbers.phonenumberutil.NumberParseException:
                        logging.debug('This is string is cannot converted to phone number: %s ...', pn)
                        return None
                    return phonenumbers.format_number(pn, phonenumbers.PhoneNumberFormat.INTERNATIONAL)

    except Exception as e:
        logging.exception('Extracting phone number failed: {}'.format(e))
        logging.exception(traceback.print_exc())


def clean_city(clearable: str) -> str:
    '''Remove additional things from cityname

    :param clearable: Not clear cityname
    :return: Clear cityname
    '''
    clearable = clean_string(clearable)
    if clearable is None:
        return None

    city = clean_string(clearable)
    city = re.sub(PATTERN_CITY, '', city)
    repls = ('Mikolc', 'Miskolc'), ('Iinárcs', 'Inárcs')
    city = reduce(lambda a, kv: a.replace(*kv), repls, city)
    city = city.split('-')[0]
    city = city.split(',')[0]
    city = city.split('/')[0]
    city = city.split('/')[0]
    city = city.split('(')[0]
    city = city.split(' ')[0]
    city = clean_string(city)
    return city.title()


def clean_opening_hours(oh_from_to):
    oh_match = PATTERN_OPENING_HOURS.search(oh_from_to)
    if oh_match is not None:
        tmp = oh_match.group(0)
    else:
        # No match return None
        return None, None
    # Remove all whitespaces
    tmp = ''.join(tmp.split())
    # We expect exactly two parts, for example: 09:40
    if len(tmp.split('-')) == 2:
        tmf = tmp.split('-')[0].zfill(5)
        tmt = tmp.split('-')[1].zfill(5)
    else:
        tmf, tmt = None, None
    return tmf, tmt


def clean_opening_hours_2(oh):
    if oh == '-1':
        return None
    else:
        tmp = oh.strip().zfill(4)
        fmt = '{}:{}'.format(tmp[:2], tmp[-2:])
    return fmt


def clean_phone(phone):
    phone = clean_string(str(phone))
    if phone is None or phone == '':
        return None
    # Remove all whitespaces
    original = phone
    if '(' in phone:
        phone = phone.split('(')[0]
    phone = phone.replace('-', ' ')
    if ',' in phone:
        phone = phone.replace(',', ';')
    if ';' in phone:
        phone = phone.split(';')
    try:
        if type(phone) is list:
            for i in phone:
                pn = [phonenumbers.parse(i, 'HU') for i in phone]
        else:
            pn = []
            pn.append(phonenumbers.parse(phone, 'HU'))
    except phonenumbers.phonenumberutil.NumberParseException:
        logging.debug('This is string is cannot converted to phone number: %s ...', original)
        return None
    if pn is not None:
        return [phonenumbers.format_number(i, phonenumbers.PhoneNumberFormat.INTERNATIONAL) for i in pn]
    else:
        return None


def clean_phone_to_json(phone):
    phone = clean_string(phone)
    if phone is None:
        return None
    phone = clean_phone(phone)
    if phone is not None:
        return json.dumps(phone)
    else:
        return None


def clean_phone_to_str(phone):
    phone = clean_string(phone)
    if phone is None:
        return None
    cleaned = clean_phone(phone)
    if cleaned is not None:
        return ';'.join(cleaned)
    else:
        return None


def clean_email(email):
    email = clean_string(email)
    if email is None:
        return None
    email_parts = email.lower().split()
    if len(email_parts) == 0:
        return None
    email = ';'.join(email_parts)
    if ',' in email:
        email_list = email.split(',')
        email = ';'.join(email_list)
    if ';' in email:
        email_list = email.split(';')
        email = ';'.join(email_list)
    return email


def clean_string(clearable: str):
    '''
    Remove whitespaces, extra spaces from strings and surrounding whitespace characters
    :param clearable: String that has to clean
    :return: Cleaned string
    Returns None if the string is empty or contains only whitespace characters
    '''
    if clearable is None:
        return None
    if not isinstance(clearable, str):
        try:
            # logging.debug('Non string input (%s) trying to convert to string ...', clearable)
            clearable = str(clearable)
        except Exception as e:
            logging.error(e)
            logging.exception('Exception occurred')
            return None
    # Remove all whitespaces
    clearable = remove_whitespace(clearable, ' ')
    if clearable == '' or clearable.upper() in ['NONE', 'NAN', 'NULL', 'NULLNONE']:
        return None
    # Make list from words and join them with one space, removing double/multiple spaces
    clearable_parts = clearable.split()
    if len(clearable_parts) == 0:
        return None
    clearable = ' '.join(clearable_parts)
    clearable = clearable.strip()
    if clearable is not None and clearable != '' and clearable != ' ' and clearable.upper() not in ['NONE', 'NAN', 'NULL', 'NULLNONE']:
        return clearable
    else:
        return None

def clean_postcode(clearable: str):
    clearable = clean_string(clearable)
    if clearable is not None and (clearable == '' or clearable == '0'):
        return None
    return clearable

def clean_url(clearable):
    '''
    Remove extra slashes from URL strings and surrounding whitespace characters
    :param clearable: String that has to clean
    :return: Cleaned string
    '''
    clearable = clean_string(clearable)
    if clearable is None:
        return None
    url_match = PATTERN_URL_SLASH.sub('/', str(clearable))
    return url_match.lower().strip()


def clean_street(clearable):
    '''

    :param clearable:
    :return:
    '''
    clearable = str(clearable)
    street = clean_string(clearable)
    if clearable is None:
        return None
    if clearable == '':
        return ''
    repls = ('Nyúl 82. sz. főút', 'Kossuth Lajos út'), \
            ('Nyúl  82. sz. főút', '82' + SZFKL), \
            ('Budafoki út, 6-os sz. főút', '6' + SZFKL), \
            ('. Sz. Főút felső', SZFKL), \
            ('. számú - Némedi út sarok', SZFKL), \
            ('076/15. hrsz 86. számú főút mellett', '86' + SZFKL), \
            ('50.sz.út jobb oldal', '50' + SZFKL), \
            ('. sz. fkl.út', SZFKL), \
            ('.sz. fkl. út', SZFKL), \
            ('-es sz. főút', SZFKL), \
            ('. sz. főút', SZFKL), \
            ('.sz.fkl.', '. számú főközlekedési'), \
            ('. sz. fkl.', '. számú főközlekedési'), \
            ('. számú fkl. út', SZFKL), \
            ('. Sz. főút', SZFKL), \
            ('. számú főút', SZFKL), \
            ('. főút', SZFKL), \
            ('. sz út', SZFKL), \
            (' sz. főút', SZFKL), \
            ('-es fő út', SZFKL), \
            ('-es főút', SZFKL), \
            (' - es út', SZFKL), \
            ('-es út', SZFKL), \
            ('-as fő út', SZFKL), \
            ('-as főút', SZFKL), \
            (' - as út', SZFKL), \
            ('-as út', SZFKL), \
            ('-ös fő út', SZFKL), \
            ('-ös főút', SZFKL), \
            (' - ös út', SZFKL), \
            ('-ös út', SZFKL), \
            ('Omsz park', 'Omszk park'), \
            ('01.máj.', 'Május 1.'), \
            ('15.márc.', 'Március 15.'), \
            ('Ady E.', 'Ady Endre'), \
            ('Áchim A.', 'Áchim András'), \
            ('Bajcsy-Zs. E.', 'Bajcsy-Zsilinszky Endre'), \
            ('Bajcsy-Zs. E. u.', 'Bajcsy-Zsilinszky Endre utca'), \
            ('Bajcsy-Zs. u.', 'Bajcsy-Zsilinszky utca'), \
            ('Bajcsy Zs.u.', 'Bajcsy-Zsilinszky utca'), \
            ('Bajcsy-Zs. u.', 'Bajcsy-Zsilinszky utca'), \
            ('Bajcsy Zs. u.', 'Bajcsy-Zsilinszky utca'), \
            ('Bajcsy-Zs.', 'Bajcsy-Zsilinszky'), \
            ('Bajcsy Zs.', 'Bajcsy-Zsilinszky'), \
            ('Bartók B.', 'Bartók Béla'), \
            ('Baross G.', 'Baross Gábor'), \
            ('BERCSÉNYI U.', 'Bercsényi Miklós utca'), \
            ('Berzsenyi D.', 'Berzsenyi Dániel'), \
            ('Borics P.', 'Borics Pál'), \
            ('Corvin J.', 'Corvin'), \
            ('Dózsa Gy.u.', 'Dózsa György utca'), \
            ('Dózsa Gy.', 'Dózsa György'), \
            ('dr. Géfin Lajos', 'Dr. Géfin Lajos'), \
            ('Erkel F.', 'Erkel Ferenc'), \
            ('Hegedű/(Király)', 'Hegedű'), \
            ('Hevesi S.', 'Hevesi Sándor'), \
            ('Hunyadi J.', 'Hunyadi János'), \
            ('Ii. Rákóczi Ferenc', 'II. Rákóczi Ferenc'), \
            ('Innovációs kp. Fő út', 'Fő út'), \
            ('Ix. körzet', 'IX. körzet'), \
            ('Kölcsey F.', 'Kölcsey Ferenc'), \
            ('Kiss J.', 'Kiss József'), \
            ('Nagy L. király', 'Nagy Lajos király'), \
            ('Kaszás u. 2.-Dózsa György út', 'Dózsa György út'), \
            ('Váci út 117-119. „A” épület', 'Váci út'), \
            ('56-Osok tere', 'Ötvenhatosok tere'), \
            ('11-es út', '11. számú főközlekedési út'), \
            ('11-es Huszár út', 'Huszár út'), \
            ('Kölcsey-Pozsonyi út sarok', 'Kölcsey Ferenc utca '), \
            ('Március 15-e', 'Március 15.'), \
            ('Tiszavasvári út - Alkotás u sarok', 'Tiszavasvári út'), \
            ('Tiszavasvári út- Alkotás út sarok', 'Tiszavasvári út'), \
            ('Hőforrás-Rákóczi utca', 'Rákóczi utca'), \
            ('Kiss Tábornok - Kandó Kálmán utca sarok', 'Kiss Tábornok utca'), \
            ('Soroksári út - Határ út sarok', 'Soroksári út'), \
            ('Szentendrei- Czetz János utca sarok', 'Szentendrei út'), \
            ('Külső - Kádártai utca', 'Külső-Kádártai utca'), \
            ('Károlyi út - Ságvári út', 'Károlyi Mihály utca'), \
            ('Szlovák út - Csömöri út sarok', 'Szlovák út'), \
            ('Maglódi út - Jászberényi út sarok', 'Maglódi út'), \
            ('Dobogókői út- Kesztölci út sarok', 'Dobogókői út'), \
            ('DR. KOCH L. UTCA', 'Dr. Koch László utca'), \
            ('DR KOCH L.', 'Dr. Koch László'), \
            ('Koch L.u.', 'Dr. Koch László utca'), \
            ('Kiss J. ', 'Kiss József'), \
            ('Kossuth L.u.', 'Kossuth Lajos utca '), \
            ('Kossuth L.', 'Kossuth Lajos'), \
            ('Kossuth F. u', 'Kossuth Ferenc utca'), \
            ('Kossuth F.', 'Kossuth Ferenc'), \
            ('Korányi F.', 'Korányi Frigyes'), \
            ('Kőrösi Csoma S.', 'Kőrösi Csoma Sándor'), \
            ('Páter K.', 'Páter Károly'), \
            ('Petőfi S.', 'Petőfi Sándor'), \
            ('Somogyi B.', 'Somogyi Béla'), \
            ('Szondy', 'Szondi'), \
            ('Szt.István', 'Szent István'), \
            ('szt.istván', 'Szent István'), \
            ('Táncsics M.', 'Táncsics Mihály'), \
            ('Vass J.', 'Vass János'), \
            ('Vámház.', 'Vámház'), \
            ('Várkörút .', 'Várkörút'), \
            ('Vásárhelyi P.', 'Vásárhelyi Pál'), \
            ('Vi. utca', 'VI. utca'), \
            ('XXI. II. Rákóczi Ferenc', 'II. Rákóczi Ferenc'), \
            ('Zsolnay V.', 'Zsolnay Vilmos'), \
            ('Radnóti M.', 'Radnóti Miklós'), \
            ('Fehérvári út (Andor u. 1.)', 'Fehérvári'), \
            ('Szent István kir.', 'Szent István király'), \
            ('Dr Batthyány S. László', 'Dr. Batthyány-Strattmann László'), \
            ('Bacsinszky A.', 'Bacsinszky András'), \
            ('Fáy A.', 'Fáy András'), \
            ('József a.', 'József Attila'), \
            ('Juhász Gy. ', 'Juhász Gyula'), \
            ('Hock j.', 'Hock János'), \
            ('Vak B.', 'Vak Bottyán'), \
            ('Arany J.', 'Arany János'), \
            ('Könyves K.', 'Könyves Kálmán'), \
            ('Szilágyi E.', 'Szilágyi Erzsébet'), \
            ('Liszt F.', 'Liszt Ferenc'), \
            ('Bethlen G.', 'Bethlen Gábor'), \
            ('Gazdag E.', 'Gazdag Erzsi'), \
            ('Hátsókapu.', 'Hátsókapu'), \
            ('Herman O.', 'Herman Ottó'), \
            ('József A.', 'József Attila'), \
            ('Kazinczy F.', 'Kazinczy Ferenc'), \
            ('Király J.', 'Király Jenő'), \
            ('Királyhidai utca', 'Királyhidai út'), \
            ('Lackner K.', 'Lackner Kristóf'), \
            ('Mécs L.', 'Mécs László'), \
            ('Nagyváthy J.', 'Nagyváthy János'), \
            ('Szent I. kir.', 'Szent István király'), \
            ('Szigethy A. u.', 'Szigethy Attila út'), \
            ('Rákóczi F.', 'Rákóczi Ferenc'), \
            ('Jókai M.', 'Jókai Mór'), \
            ('Szabó D.', 'Szabó Dezső'), \
            ('Kossuth F.', 'Kossuth F.'), \
            ('Móricz Zs.', 'Móricz Zsigmond'), \
            ('Hunyadi J ', 'Hunyadi János'), \
            ('Szilágyi E ', 'Szilágyi Erzsébet fasor'), \
            ('Erzsébet Királyné út', 'Erzsébet királyné útja'), \
            ('Mammut', ''), \
            ('Szt. ', 'Szent '), \
            (' u.', ' utca '), \
            (' U.', ' utca '), \
            ('.u.', ' utca '), \
            (' u ', ' utca '), \
            (' krt.', ' körút'), \
            (' Krt.', ' körút'), \
            (' KRT.', ' körút'), \
            (' ltp.', ' lakótelep'), \
            (' Ltp.', ' lakótelep'), \
            (' LTP.', ' lakótelep'), \
            (' ltp', ' lakótelep'), \
            (' sgt.', ' sugárút'), \
            ('^4. sz$', '4. számú főközlekedési')
    street = reduce(lambda a, kv: a.replace(*kv), repls, street)
    street = clean_string(street)
    return street


def clean_street_type(clearable):
    '''

    :param clearable:
    :return:
    '''

    street = clean_string(clearable)
    if street is None or clearable != '':
        return None
    street = street.replace('fkl. út', 'főközlekedési út')
    street = street.replace('főút', 'főközlekedési út')
    street = street.replace('ltp.', ' lakótelep')
    street = street.replace('LTP.', ' lakótelep')
    street = street.replace('pu.', 'pályaudvar')
    street = street.replace('út.', 'út')
    street = street.replace('u.', 'utca')
    street = street.replace('(nincs)', '')
    street = street.replace('.', '')
    street = clean_string(street)
    return street


def clean_branch(clearable):
    '''

    :param clearable:
    :return:
    '''
    if clearable is not None and clearable != '':
        branch = clean_string(str(clearable))
        if branch is not None:
            branch = branch.replace('sz.', 'számú')
        branch = clean_string(branch)
        return branch
    else:
        return None
