# A poi_matchmaker működés

## A program futtatása előtti teendők

Annak érdekében, hogy a poi_matchmaker képes legyen offline működésre, szükség van az OpenStreetMap adatbázis-mentés beimportálására a saját adatbázisba, az osm2pgsql program segítségével.

## A program beállításai

A program futtatásával kapcsolatos beállítások a [app.conf-template](https://github.com/KAMI911/osm_poi_matchmaker/blob/master/osm_poi_matchmaker/app.conf-template) fájlban találhatóak, amelyet átmásolva app.conf néven és a kívánt beállításokat szerkesztve elvégezhető a program testreszabása.

## Az adatbázis táblák létrehozása

Az adatbázis táblák automatikusan létrejönnek, amennyiben azok még nem léteznek.

## A program által beimportált adatok

A poi_matchmaker a posta által elérhetővé tett település (city tábla) és utcanév (street_type tábla) adatokat letölti és beimportálja a saját adatbázisába. Ezek után az alkalmazás letölti és beimportálja a [dataproviders](https://github.com/KAMI911/osm_poi_matchmaker/tree/master/osm_poi_matchmaker/dataproviders) mappában található adatforrások segítségével a tényleges POI adatokat. A külső forrásból érkező adatok feldolgozása különböző mértékű, van ahol már a teljes feldolgozás megoldott, míg máshol csak a címek és üzlettípusok adatok feldolgozása történik meg. Minden egyes cég(csoport) POI-ja és a hozzá tartozó POI típusok egyedi OpenStreetMap címkéket kaphatnak. POI alapadatként ezek a címkék, valamint a POI típusból származó címkék bekerülnek az adatbázisba (poi_common tábla és types metódus). Az egyedi POI-k hozzáadása ezután történik meg (poi_address tábla és process metódus). A felvett POI-k lényeges paraméterei külön-külön mezőben szerepelnek, némi ellenőrzéssel, és adminisztratív adatokkal.

Megjegyzés:
* A beimportált adatok irányítószámát az OpenStreetMap által biztosított irányítószám poligonbon veszi a program (alapértelmezés szerint, beállítható az app.conf `geo.prefer.osm.postcode=True` paraméter `False` értekre történő állításával).

# A program által végzett összerendelések

A külső adatok sikeres betöltése után a program megkísérli a POI típusnak és nevének megfelelő objektumokat megtalálni az OpenStreetMap adatbázisában. Mivel ez a művelet offline történik, a már letöltött és betöltött OpenStreetMap adatbázis-mentést szükséges éles import vagy végső előkészítés előtt újra letölteni és betölteni a program adatbázisába.

Az lekérdezés külön-külön lefut, mind az OpenStreetMap pontokra, vonalakra és kapcsolatokra is.

Az összerendelés menete:

1. Amennyiben van név megadva a dataprovider alapadatoknál (`poi_search_name` kulcs név a dictionary-ban) egy nagyobb keresési sugárban végzett keresés zajlik le a POI típusra jellemező fő OpenStreetMap címkék alapján és a POI nevében történő részkeresés szerint.

(POI_Base osztály, query_osm_shop_poi_gpd metódus)
(# Try to search OSM POI with same type, and name contains poi_search_name within the specified distance)

Keresési értékek:
```buffer = 50```

Az egyes POI típusokra jellemző keresési sugár az app.conf fájl alapján kerül meghatározásra:
```
geo.default.poi.distance=70
geo.amenity.atm.poi.distance=20
geo.shop.conveience.poi.distance=50
geo.amenity.post.office.poi.distance=260
```

Ilyenkor az alapértelmezetten megadott keresési sugár az alább szerint kerül megnövelésre:
```
buffer += 600
distance += 800
```

(Ezt a megoldást a T miatt használtam, külön külön minden POI típushoz érdemes lenne kivezetni a megfelelő értékeket.)

2. Amennyiben az előző keresés nem járt sikerrel, vagy eleve nem volt az adatforrásnál megadva  poi_search_name kulcs értéke, akkor egy kisebb keresési sugárban próbálja meg beazonosítani a program a OpenStreeMap adatbázisban a POI-t, POI típusra jellemező fő OpenStreetMap címkék alapján.

(POI_Base osztály, query_osm_shop_poi_gpd metódus)
(# Try to search OSM POI with same type, and name contains poi_search_name within the specified distance)

Keresési értékek:
```buffer = 50```

Az egyes POI típusokra jellemző keresési sugár az app.conf fájl alapján kerül meghatározásra:
```
geo.default.poi.distance=70
geo.amenity.atm.poi.distance=20
geo.shop.conveience.poi.distance=50
geo.amenity.post.office.poi.distance=260
```

