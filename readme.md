ebird2spatialite
----------------
A tool for extracting EBird data into a spatialite database for further
analysis.

Usage
-----

Select records within 50km of a location in Ottawa, Canada:
```
ebird2spatialite data/ebd_relJul-2020.txt.gz --near-location "POINT (-75.6996606 45.4248058)" --buffer 50000
```

Select only records of Brown Thrasher:
```
ebird2spatialite data/ebd_relJul-2020.txt.gz --near-location "POINT (-75.6996606 45.4248058)" --buffer 50000 --common-name-regex "Thrasher"
```

Select only records with the genus Tringa:
```
ebird2spatialite data/ebd_relJul-2020.txt.gz --near-location "POINT (-75.6996606 45.4248058)" --buffer 50000 --scientific-name-regex "Tringa"
```

Select records since the specified date:
```
ebird2spatialite data/ebd_relJul-2020.txt.gz --near-location "POINT (-75.6996606 45.4248058)" --buffer 50000 --since-date '2007-04-13'
```

Select records before the specified date:
```
ebird2spatialite data/ebd_relJul-2020.txt.gz --near-location "POINT (-75.6996606 45.4248058)" --buffer 50000 --before-date '2007-04-13'
```

See Also
--------
The Cornell Lab of Ornithlogy provides
[Auk](https://cornelllabofornithology.github.io/auk/) a tool for extracting
EBird data into R data frames. Some differences with Auk:
* ebird2spatialite creates spatialite databases which can be used in tools
other than R.
* ebird2spatialite works on compressed data and so requires less space.
* Auk provides more ways of filtering data during import.

TODO
----
* It would be nice if --near-location accepted arbitrary geometries, but the structure of
the wkt library makes this awkward.
* Add --within option, which will select records contained within a polygon.
* Filter records in parallel.
