use chrono::NaiveDate;
use clap::{App, Arg};
use flate2::read::GzDecoder;
use geo::algorithm::haversine_distance::HaversineDistance;
use geo::{point, Point};
use regex::Regex;
use rusqlite::{params, Connection, LoadExtensionGuard, NO_PARAMS};
use serde::Deserialize;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{self};
use std::path::Path;

#[derive(Debug, Deserialize)]
struct EBirdRecord {
    #[serde(rename = "GLOBAL UNIQUE IDENTIFIER")]
    guid: String,
    #[serde(rename = "COMMON NAME")]
    common_name: String,
    #[serde(rename = "SCIENTIFIC NAME")]
    scientific_name: String,
    #[serde(rename = "OBSERVATION COUNT")]
    observation_count: String, // String because 'X' is used for count not specified
    #[serde(rename = "BREEDING BIRD ATLAS CODE")]
    breeding_bird_atlas_code: String,
    #[serde(rename = "BREEDING BIRD ATLAS CATEGORY")]
    breeding_bird_atlas_category: String,
    #[serde(rename = "AGE/SEX")]
    age_sex: String,
    #[serde(rename = "LATITUDE")]
    latitude: f64,
    #[serde(rename = "LONGITUDE")]
    longitude: f64,
    #[serde(rename = "OBSERVATION DATE")]
    obs_date: String,
    #[serde(rename = "TIME OBSERVATIONS STARTED")]
    time_obs_started: String,
    #[serde(rename = "OBSERVER ID")]
    obs_id: String,
    #[serde(rename = "SAMPLING EVENT IDENTIFIER")]
    sampling_event_id: String,
    #[serde(rename = "PROTOCOL TYPE")]
    protocol_type: String,
    #[serde(rename = "DURATION MINUTES")]
    duration_min: Option<i64>,
    #[serde(rename = "EFFORT DISTANCE KM")]
    effort_distance_km: Option<f64>,
    #[serde(rename = "NUMBER OBSERVERS")]
    number_observers: Option<i64>,
    #[serde(rename = "ALL SPECIES REPORTED")]
    all_species_reported: i64,
    #[serde(rename = "APPROVED")]
    approved: i64,
    #[serde(rename = "SPECIES COMMENTS")]
    species_comments: String,
}

fn initialize_database(conn: &rusqlite::Connection) -> rusqlite::Result<usize> {
    let _guard = LoadExtensionGuard::new(conn)?;
    conn.load_extension(Path::new("mod_spatialite.so"), None)?;

    conn.execute("DROP TABLE IF EXISTS ebird", params![])?;

    let mut stmt = conn.prepare("SELECT InitSpatialMetaData(1)")?;
    stmt.exists(NO_PARAMS)?;

    conn.execute(
        "CREATE TABLE ebird (
                id                              INTEGER PRIMARY KEY,
                guid                            TEXT,
                common_name                     TEXT,
                scientific_name                 TEXT,
                observation_count               TEXT,
                breeding_bird_atlas_code        TEXT,
                breeding_bird_atlas_category    TEXT,
                obs_date                        TEXT,
                time_obs_started                TEXT,
                obs_id                          TEXT,
                sampling_event_id               TEXT,
                protocol_type                   TEXT,
                duration_min                    INTEGER,
                effort_distance_km              REAL,
                number_observers                INTEGER,
                all_species_reported            INTEGER,
                approved                        INTEGER,
                species_comments                TEXT)",
        params![],
    )?;

    let mut stmt =
        conn.prepare("SELECT AddGeometryColumn('ebird', 'location', 4326, 'POINT', 'XY')")?;
    stmt.exists(NO_PARAMS)?;

    Ok(0)
}

fn insert_record(conn: &rusqlite::Connection, rec: &EBirdRecord) -> rusqlite::Result<usize> {
    conn.execute(
        "INSERT INTO ebird (guid, common_name, scientific_name, observation_count,
                            breeding_bird_atlas_code, breeding_bird_atlas_category,
                            location, obs_date, time_obs_started, obs_id,
                            sampling_event_id, protocol_type, duration_min, effort_distance_km,
                            number_observers, all_species_reported, approved,
                            species_comments)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, MakePoint(?7, ?8, 4326), ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
        params![
            rec.guid,
            rec.common_name,
            rec.scientific_name,
            rec.observation_count,
            rec.breeding_bird_atlas_code,
            rec.breeding_bird_atlas_category,
            rec.longitude,
            rec.latitude,
            rec.obs_date,
            rec.time_obs_started,
            rec.obs_id,
            rec.sampling_event_id,
            rec.protocol_type,
            rec.duration_min,
            rec.effort_distance_km,
            rec.number_observers,
            rec.all_species_reported,
            rec.approved,
            rec.species_comments
        ],
    )
}

fn main() -> io::Result<()> {
    let matches = App::new("ebird2spatialite")
        .arg(
            Arg::with_name("INPUT")
                .required(true)
                .index(1)
                .help("path to ebird archive"),
        )
        .arg(
            Arg::with_name("before-date")
                .long("before-date")
                .takes_value(true)
                .help("Select records before the specified observation date"),
        )
        .arg(
            Arg::with_name("since-date")
                .long("since-date")
                .takes_value(true)
                .help("Select records since the specified observation date"),
        )
        .arg(
            Arg::with_name("near-location")
                .long("near-location")
                .takes_value(true)
                .help("Location around which to select records (as WKT point)"),
        )
        .arg(
            Arg::with_name("buffer")
                .long("buffer")
                .takes_value(true)
                .help("Buffer around near-location (in metres)"),
        )
        .arg(
            Arg::with_name("common-name-regex")
                .long("common-name-regex")
                .takes_value(true)
                .help("Select records matching the specified regex"),
        )
        .arg(
            Arg::with_name("scientific-name-regex")
                .long("scientific-name-regex")
                .takes_value(true)
                .help("Select records matching the specified regex"),
        )
        .arg(
            Arg::with_name("limit")
                .long("limit")
                .takes_value(true)
                .help("Limit the number of records extracted (for debugging)"),
        )
        .get_matches();

    let path = matches.value_of("INPUT").unwrap();
    let ebird_data = File::open(path)?;
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(GzDecoder::new(ebird_data));

    // Determine before date if specified
    let before_date = if let Some(text) = matches.value_of("before-date") {
        match NaiveDate::parse_from_str(text, "%Y-%m-%d") {
            Ok(date) => {
                println!("date {}", date);
                Some(date)
            }
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err));
            }
        }
    } else {
        None
    };

    // Determine since date if specified
    let since_date = if let Some(text) = matches.value_of("since-date") {
        match NaiveDate::parse_from_str(text, "%Y-%m-%d") {
            Ok(date) => {
                if let Some(before_date) = before_date {
                    if before_date > date {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Before date is after since date",
                        ));
                    }
                }
                Some(date)
            }
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err));
            }
        }
    } else {
        None
    };

    // Determine near point, if specified.
    let near: Option<geo::Point<f64>> = match matches.value_of("near-location") {
        Some(text) => match wkt::Wkt::<f64>::from_str(text) {
            Ok(wkt) => {
                if wkt.items.len() == 1 {
                    match wkt::conversion::try_into_geometry(&wkt.items[0]) {
                        Ok(as_geometry) => Point::try_from(as_geometry).ok(),
                        Err(_) => {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Invalid near location geometry",
                            ));
                        }
                    }
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Multiple near locations are not supported",
                    ));
                }
            }
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err));
            }
        },
        None => None,
    };

    // Determine buffer for use with near location. It defaults to 1000m,
    // unless an invalid value was specified.
    let buffer = if let Some(text) = matches.value_of("buffer") {
        match text.parse::<f64>() {
            Ok(buffer) => buffer,
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err));
            }
        }
    } else {
        1000.
    };

    // Determine common name regex if specified
    let common_name_regex = if let Some(text) = matches.value_of("common-name-regex") {
        match Regex::new(text) {
            Ok(regex) => Some(regex),
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err));
            }
        }
    } else {
        None
    };

    // Determine scientific name regex if specified
    let scientific_name_regex = if let Some(text) = matches.value_of("scientific-name-regex") {
        match Regex::new(text) {
            Ok(regex) => Some(regex),
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err));
            }
        }
    } else {
        None
    };

    let limit = match matches.value_of("limit") {
        Some(text) => match text.parse::<usize>() {
            Ok(limit) => limit,
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err));
            }
        },
        None => usize::max_value(),
    };

    let mut conn = Connection::open("ebird.sqlite").unwrap();
    initialize_database(&conn).unwrap();
    let tx = conn.transaction().unwrap();

    reader
        .deserialize()
        .take(limit)
        .filter_map(|deserialized| match deserialized {
            Ok(deserialized) => {
                let record: EBirdRecord = deserialized;
                Some(record)
            }
            _ => None,
        })
        .filter(|record| {
            if let Some(before_date) = &before_date {
                match NaiveDate::parse_from_str(&record.obs_date, "%Y-%m-%d") {
                    Ok(other) => other <= *before_date,
                    _ => true,
                }
            } else {
                true
            }
        })
        .filter(|record| {
            if let Some(since_date) = &since_date {
                match NaiveDate::parse_from_str(&record.obs_date, "%Y-%m-%d") {
                    Ok(other) => other >= *since_date,
                    _ => true,
                }
            } else {
                true
            }
        })
        .filter(|record| {
            if let Some(near) = near {
                let other = point!(x: record.longitude, y: record.latitude);
                near.haversine_distance(&other) < buffer
            } else {
                true
            }
        })
        .filter(|record| {
            if let Some(common_name_regex) = &common_name_regex {
                common_name_regex.is_match(&record.common_name)
            } else {
                true
            }
        })
        .filter(|record| {
            if let Some(scientific_name_regex) = &scientific_name_regex {
                scientific_name_regex.is_match(&record.scientific_name)
            } else {
                true
            }
        })
        .for_each(|record| {
            if let Err(err) = insert_record(&tx, &record) {
                println!("could not insert record: {}", err);
            }
        });
    if let Err(err) = tx.commit() {
        println!("error on commit transaction: {}", err);
    }

    Ok(())
}
