use std::cmp::Ordering;

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue, ZAddCond, ZAddOptions, ZSetObject};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::{
        Response,
        zset::basic::{
            arg_bytes, ensure_zset_type_or_missing, formatted_score_value, parse_compact,
        },
    },
    store::{SetOptions, Store},
};

const EARTH_RADIUS_M: f64 = 6_372_797.560_856;
const GEO_LAT_MIN: f64 = -85.051_128_78;
const GEO_LAT_MAX: f64 = 85.051_128_78;
const GEO_LON_MIN: f64 = -180.0;
const GEO_LON_MAX: f64 = 180.0;
const GEO_STEP_BITS: u32 = 26;
const GEO_GRID_SCALE: f64 = (1_u64 << GEO_STEP_BITS) as f64;
const GEO_GRID_MAX: u32 = (1_u32 << GEO_STEP_BITS) - 1;
const GEOHASH_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

#[derive(Clone, Copy, PartialEq, Eq)]
enum GeoOrder {
    None,
    Asc,
    Desc,
}

#[derive(Clone, Copy)]
enum GeoShape {
    Radius { radius_m: f64 },
    Box { width_m: f64, height_m: f64 },
}

#[derive(Clone, Copy)]
struct GeoUnit {
    meters: f64,
}

#[derive(Clone)]
struct GeoEntry {
    member: CompactString,
    hash: u64,
    longitude: f64,
    latitude: f64,
    distance_m: f64,
}

struct GeoSearchOptions {
    withcoord: bool,
    withdist: bool,
    withhash: bool,
    count: Option<usize>,
    any: bool,
    order: GeoOrder,
    store: Option<CompactString>,
    storedist: Option<CompactString>,
}

impl GeoSearchOptions {
    fn store_target(&self) -> Option<&CompactString> {
        self.storedist.as_ref().or(self.store.as_ref())
    }
}

#[inline]
pub fn geoadd(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'geoadd' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let parsed = parse_geoadd_args(&args[1..])?;
    let zset = store.get_or_create_zset(parse_compact(key));
    let mut total = 0_i64;

    for item in parsed.items {
        validate_coordinates(item.longitude_raw, item.latitude_raw)?;
        let longitude = parse_f64(item.longitude_raw)?;
        let latitude = parse_f64(item.latitude_raw)?;
        let score = encode_geoscore(longitude, latitude) as f64;
        let result = zset.add(
            score,
            parse_compact(item.member_raw),
            ZAddOptions {
                condition: parsed.condition,
                ch: parsed.ch,
                ..Default::default()
            },
        );
        total += if parsed.ch {
            result.changed as i64
        } else {
            result.added as i64
        };
    }

    Ok(Response::Integer(total))
}

#[inline]
pub fn geodist(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 && args.len() != 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'geodist' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let unit = if args.len() == 4 {
        parse_unit(arg_bytes(&args[3])?)?
    } else {
        GeoUnit { meters: 1.0 }
    };
    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Value(None));
    };

    let Some((lon1, lat1)) = member_coords(zset, arg_bytes(&args[1])?) else {
        return Ok(Response::Value(None));
    };
    let Some((lon2, lat2)) = member_coords(zset, arg_bytes(&args[2])?) else {
        return Ok(Response::Value(None));
    };

    let distance = haversine_distance(lon1, lat1, lon2, lat2) / unit.meters;
    Ok(Response::Value(Some(format_geo_distance(distance))))
}

#[inline]
pub fn geohash(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'geohash' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Array(Box::default()));
    };

    if args.len() == 1 {
        return Ok(Response::Array(Box::default()));
    }

    let members: Vec<CompactString> = {
        let mut members = Vec::with_capacity(args.len() - 1);
        for frame in &args[1..] {
            members.push(parse_compact(arg_bytes(frame)?));
        }
        members
    };

    let mut out = SmallVec::<[Response; 16]>::new();
    for member in members {
        let value = zset.score(member.as_bytes()).map(|score| {
            let (longitude, latitude) = decode_geoscore(score_to_hash(score));
            SenkoValue::Raw(Bytes::from(encode_geohash_string(longitude, latitude)))
        });
        out.push(Response::Value(value));
    }
    Ok(Response::Array(Box::new(out)))
}

#[inline]
pub fn geopos(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'geopos' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Array(Box::default()));
    };

    if args.len() == 1 {
        return Ok(Response::Array(Box::default()));
    }

    let members: Vec<CompactString> = {
        let mut members = Vec::with_capacity(args.len() - 1);
        for frame in &args[1..] {
            members.push(parse_compact(arg_bytes(frame)?));
        }
        members
    };

    let mut out = SmallVec::<[Response; 16]>::new();
    for member in members {
        let value = zset.score(member.as_bytes()).map(|score| {
            let (lon, lat) = decode_geoscore(score_to_hash(score));
            coords_response(lon, lat)
        });
        out.push(value.unwrap_or(Response::Value(None)));
    }
    Ok(Response::Array(Box::new(out)))
}

#[inline]
pub fn georadius(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    georadius_impl(store, args, false, "georadius")
}

#[inline]
pub fn georadius_ro(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    georadius_impl(store, args, true, "georadius_ro")
}

#[inline]
pub fn georadiusbymember(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    georadiusbymember_impl(store, args, false, "georadiusbymember")
}

#[inline]
pub fn georadiusbymember_ro(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    georadiusbymember_impl(store, args, true, "georadiusbymember_ro")
}

#[inline]
pub fn geosearch(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 5 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'geosearch' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let parsed = parse_geosearch_args(&args[1..], false)?;

    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Array(Box::default()));
    };
    let (center_lon, center_lat) = parsed.origin.resolve(zset)?;
    let entries = collect_geo_results(
        zset,
        center_lon,
        center_lat,
        parsed.shape,
        parsed.unit,
        &parsed.options,
    );
    format_geo_results(store, entries, parsed.options)
}

#[inline]
pub fn geosearchstore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 6 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'geosearchstore' command",
        ));
    }

    let dst = parse_compact(arg_bytes(&args[0])?);
    let src = arg_bytes(&args[1])?;
    ensure_zset_type_or_missing(store, src)?;
    let parsed = parse_geosearch_args(&args[2..], true)?;

    let Some(zset) = store.get_zset(src) else {
        let _ = store.delete(dst.as_bytes());
        return Ok(Response::Integer(0));
    };
    let (center_lon, center_lat) = parsed.origin.resolve(zset)?;
    let entries = collect_geo_results(
        zset,
        center_lon,
        center_lat,
        parsed.shape,
        parsed.unit,
        &parsed.options,
    );
    store_geo_results(store, dst, &entries, parsed.options.storedist.is_some())
}

fn georadius_impl(
    store: &mut Store,
    args: &[Frame<'_>],
    readonly: bool,
    command: &'static str,
) -> SenkoResult<Response> {
    if args.len() < 5 {
        return Err(SenkoError::Protocol(match command {
            "georadius_ro" => "wrong number of arguments for 'georadius_ro' command",
            _ => "wrong number of arguments for 'georadius' command",
        }));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    validate_coordinates(arg_bytes(&args[1])?, arg_bytes(&args[2])?)?;
    let longitude = parse_f64(arg_bytes(&args[1])?)?;
    let latitude = parse_f64(arg_bytes(&args[2])?)?;
    let radius_raw = parse_f64(arg_bytes(&args[3])?)?;
    if radius_raw < 0.0 {
        return Err(SenkoError::Protocol("ERR radius cannot be negative"));
    }
    let unit = parse_unit(arg_bytes(&args[4])?)?;
    let options = parse_geo_search_options(&args[5..], readonly, command)?;

    let Some(zset) = store.get_zset(key) else {
        return Ok(if options.store_target().is_some() {
            if let Some(target) = options.store_target() {
                let _ = store.delete(target.as_bytes());
            }
            Response::Integer(0)
        } else {
            Response::Array(Box::default())
        });
    };

    let entries = collect_geo_results(
        zset,
        longitude,
        latitude,
        GeoShape::Radius {
            radius_m: radius_raw * unit.meters,
        },
        unit,
        &options,
    );
    format_geo_results(store, entries, options)
}

fn georadiusbymember_impl(
    store: &mut Store,
    args: &[Frame<'_>],
    readonly: bool,
    command: &'static str,
) -> SenkoResult<Response> {
    if args.len() < 4 {
        return Err(SenkoError::Protocol(match command {
            "georadiusbymember_ro" => {
                "wrong number of arguments for 'georadiusbymember_ro' command"
            }
            _ => "wrong number of arguments for 'georadiusbymember' command",
        }));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let radius_raw = parse_f64(arg_bytes(&args[2])?)?;
    if radius_raw < 0.0 {
        return Err(SenkoError::Protocol("ERR radius cannot be negative"));
    }
    let unit = parse_unit(arg_bytes(&args[3])?)?;
    let options = parse_geo_search_options(&args[4..], readonly, command)?;

    let Some(zset) = store.get_zset(key) else {
        return Ok(if options.store_target().is_some() {
            if let Some(target) = options.store_target() {
                let _ = store.delete(target.as_bytes());
            }
            Response::Integer(0)
        } else {
            Response::Array(Box::default())
        });
    };
    let Some((longitude, latitude)) = member_coords(zset, arg_bytes(&args[1])?) else {
        return Err(SenkoError::Protocol(
            "ERR could not decode requested zset member",
        ));
    };

    let entries = collect_geo_results(
        zset,
        longitude,
        latitude,
        GeoShape::Radius {
            radius_m: radius_raw * unit.meters,
        },
        unit,
        &options,
    );
    format_geo_results(store, entries, options)
}

struct ParsedGeoAdd<'a> {
    condition: ZAddCond,
    ch: bool,
    items: Vec<GeoAddItem<'a>>,
}

struct GeoAddItem<'a> {
    longitude_raw: &'a [u8],
    latitude_raw: &'a [u8],
    member_raw: &'a [u8],
}

fn parse_geoadd_args<'a>(args: &'a [Frame<'a>]) -> SenkoResult<ParsedGeoAdd<'a>> {
    let mut index = 0;
    let mut condition = ZAddCond::Always;
    let mut ch = false;

    while index < args.len() {
        let raw = arg_bytes(&args[index])?;
        if raw.eq_ignore_ascii_case(b"nx") {
            if !matches!(condition, ZAddCond::Always) {
                return Err(SenkoError::Protocol(
                    "ERR XX and NX options at the same time are not compatible",
                ));
            }
            condition = ZAddCond::NX;
            index += 1;
        } else if raw.eq_ignore_ascii_case(b"xx") {
            if !matches!(condition, ZAddCond::Always) {
                return Err(SenkoError::Protocol(
                    "ERR XX and NX options at the same time are not compatible",
                ));
            }
            condition = ZAddCond::XX;
            index += 1;
        } else if raw.eq_ignore_ascii_case(b"ch") {
            ch = true;
            index += 1;
        } else {
            break;
        }
    }

    let remaining = &args[index..];
    if remaining.len() < 3 || remaining.len() % 3 != 0 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'geoadd' command",
        ));
    }

    let mut items = Vec::with_capacity(remaining.len() / 3);
    let mut cursor = 0;
    while cursor < remaining.len() {
        items.push(GeoAddItem {
            longitude_raw: arg_bytes(&remaining[cursor])?,
            latitude_raw: arg_bytes(&remaining[cursor + 1])?,
            member_raw: arg_bytes(&remaining[cursor + 2])?,
        });
        cursor += 3;
    }

    Ok(ParsedGeoAdd {
        condition,
        ch,
        items,
    })
}

enum GeoOrigin {
    FromLonLat(f64, f64),
    FromMember(CompactString),
}

impl GeoOrigin {
    fn resolve(&self, zset: &ZSetObject) -> SenkoResult<(f64, f64)> {
        match self {
            Self::FromLonLat(lon, lat) => Ok((*lon, *lat)),
            Self::FromMember(member) => member_coords(zset, member.as_bytes()).ok_or(
                SenkoError::Protocol("ERR could not decode requested zset member"),
            ),
        }
    }
}

struct ParsedGeoSearch {
    origin: GeoOrigin,
    shape: GeoShape,
    unit: GeoUnit,
    options: GeoSearchOptions,
}

fn parse_geosearch_args(args: &[Frame<'_>], store_only: bool) -> SenkoResult<ParsedGeoSearch> {
    let mut index = 0;
    let mut origin = None;
    let mut shape = None;
    let mut unit = None;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if token.eq_ignore_ascii_case(b"frommember") {
            if origin.is_some() || index + 1 >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            origin = Some(GeoOrigin::FromMember(parse_compact(arg_bytes(
                &args[index + 1],
            )?)));
            index += 2;
        } else if token.eq_ignore_ascii_case(b"fromlonlat") {
            if origin.is_some() || index + 2 >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            let lon_raw = arg_bytes(&args[index + 1])?;
            let lat_raw = arg_bytes(&args[index + 2])?;
            validate_coordinates(lon_raw, lat_raw)?;
            origin = Some(GeoOrigin::FromLonLat(
                parse_f64(lon_raw)?,
                parse_f64(lat_raw)?,
            ));
            index += 3;
        } else if token.eq_ignore_ascii_case(b"byradius") {
            if shape.is_some() || index + 2 >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            let radius = parse_f64(arg_bytes(&args[index + 1])?)?;
            if radius < 0.0 {
                return Err(SenkoError::Protocol("ERR radius cannot be negative"));
            }
            let parsed_unit = parse_unit(arg_bytes(&args[index + 2])?)?;
            unit = Some(parsed_unit);
            shape = Some(GeoShape::Radius {
                radius_m: radius * parsed_unit.meters,
            });
            index += 3;
        } else if token.eq_ignore_ascii_case(b"bybox") {
            if shape.is_some() || index + 3 >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            let width = parse_f64(arg_bytes(&args[index + 1])?)?;
            let height = parse_f64(arg_bytes(&args[index + 2])?)?;
            if width < 0.0 || height < 0.0 {
                return Err(SenkoError::Protocol(
                    "ERR box width/height cannot be negative",
                ));
            }
            let parsed_unit = parse_unit(arg_bytes(&args[index + 3])?)?;
            unit = Some(parsed_unit);
            shape = Some(GeoShape::Box {
                width_m: width * parsed_unit.meters,
                height_m: height * parsed_unit.meters,
            });
            index += 4;
        } else {
            break;
        }
    }

    let Some(origin) = origin else {
        return Err(SenkoError::Protocol(
            "ERR exactly one of FROMMEMBER or FROMLONLAT arguments need to be specified",
        ));
    };
    let Some(shape) = shape else {
        return Err(SenkoError::Protocol(
            "ERR exactly one of BYRADIUS and BYBOX arguments need to be specified",
        ));
    };

    let options = parse_geo_search_options(&args[index..], store_only, "geosearch")?;
    Ok(ParsedGeoSearch {
        origin,
        shape,
        unit: unit.expect("shape parsing sets unit"),
        options,
    })
}

fn parse_geo_search_options(
    tail: &[Frame<'_>],
    store_only: bool,
    command: &'static str,
) -> SenkoResult<GeoSearchOptions> {
    let mut index = 0;
    let mut withcoord = false;
    let mut withdist = false;
    let mut withhash = false;
    let mut count = None;
    let mut any = false;
    let mut order = GeoOrder::None;
    let mut store = None;
    let mut storedist = None;

    while index < tail.len() {
        let token = arg_bytes(&tail[index])?;
        if token.eq_ignore_ascii_case(b"withcoord") {
            if store_only {
                return Err(SenkoError::Protocol("syntax error"));
            }
            withcoord = true;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"withdist") {
            if store_only {
                return Err(SenkoError::Protocol("syntax error"));
            }
            withdist = true;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"withhash") {
            if store_only {
                return Err(SenkoError::Protocol("syntax error"));
            }
            withhash = true;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"count") {
            if index + 1 >= tail.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let parsed = parse_non_negative_usize(arg_bytes(&tail[index + 1])?)?;
            count = Some(parsed);
            index += 2;
            if index < tail.len() && arg_bytes(&tail[index])?.eq_ignore_ascii_case(b"any") {
                any = true;
                index += 1;
            }
        } else if token.eq_ignore_ascii_case(b"any") {
            return Err(SenkoError::Protocol(
                "ERR ANY option requires COUNT option",
            ));
        } else if token.eq_ignore_ascii_case(b"asc") {
            if !matches!(order, GeoOrder::None) {
                return Err(SenkoError::Protocol("syntax error"));
            }
            order = GeoOrder::Asc;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"desc") {
            if !matches!(order, GeoOrder::None) {
                return Err(SenkoError::Protocol("syntax error"));
            }
            order = GeoOrder::Desc;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"store") {
            if store_only || store.is_some() || storedist.is_some() || index + 1 >= tail.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            store = Some(parse_compact(arg_bytes(&tail[index + 1])?));
            index += 2;
        } else if token.eq_ignore_ascii_case(b"storedist") {
            if command == "geosearch" && store_only {
                if store.is_some() || storedist.is_some() {
                    return Err(SenkoError::Protocol("syntax error"));
                }
                storedist = Some(CompactString::new(""));
                index += 1;
            } else if store_only
                || store.is_some()
                || storedist.is_some()
                || index + 1 >= tail.len()
            {
                return Err(SenkoError::Protocol("syntax error"));
            } else {
                storedist = Some(parse_compact(arg_bytes(&tail[index + 1])?));
                index += 2;
            }
        } else {
            return Err(SenkoError::Protocol("syntax error"));
        }
    }

    if !store_only && store.is_some() && (withcoord || withdist || withhash) {
        return Err(SenkoError::Protocol(
            "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options",
        ));
    }
    if any && count.is_none() {
        return Err(SenkoError::Protocol(
            "ERR ANY option requires COUNT option",
        ));
    }

    Ok(GeoSearchOptions {
        withcoord,
        withdist,
        withhash,
        count,
        any,
        order,
        store,
        storedist,
    })
}

fn collect_geo_results(
    zset: &ZSetObject,
    center_lon: f64,
    center_lat: f64,
    shape: GeoShape,
    unit: GeoUnit,
    options: &GeoSearchOptions,
) -> Vec<GeoEntry> {
    let mut entries = Vec::new();
    for (score, member) in zset.range_by_rank(0, -1, false, None) {
        let hash = score_to_hash(score);
        let (longitude, latitude) = decode_geoscore(hash);
        let distance_m = haversine_distance(center_lon, center_lat, longitude, latitude);
        let matches = match shape {
            GeoShape::Radius { radius_m } => distance_m <= radius_m,
            GeoShape::Box { width_m, height_m } => point_in_box(
                center_lon, center_lat, longitude, latitude, width_m, height_m,
            ),
        };
        if matches {
            entries.push(GeoEntry {
                member,
                hash,
                longitude,
                latitude,
                distance_m: distance_m / unit.meters,
            });
            if options.any
                && matches!(options.order, GeoOrder::None)
                && options.count.is_some_and(|count| entries.len() >= count)
            {
                break;
            }
        }
    }

    match options.order {
        GeoOrder::Asc => entries.sort_by(compare_geo_entry_asc),
        GeoOrder::Desc => entries.sort_by(compare_geo_entry_desc),
        GeoOrder::None => {}
    }

    if let Some(limit) = options.count {
        entries.truncate(limit);
    }
    entries
}

fn format_geo_results(
    store: &mut Store,
    entries: Vec<GeoEntry>,
    options: GeoSearchOptions,
) -> SenkoResult<Response> {
    if let Some(target) = options.store_target() {
        return store_geo_results(store, target.clone(), &entries, options.storedist.is_some());
    }

    let mut out = SmallVec::<[Response; 16]>::new();
    for entry in entries {
        if !(options.withcoord || options.withdist || options.withhash) {
            out.push(Response::Value(Some(SenkoValue::Raw(
                Bytes::copy_from_slice(entry.member.as_bytes()),
            ))));
            continue;
        }

        let mut nested = SmallVec::<[Response; 16]>::new();
        nested.push(Response::Value(Some(SenkoValue::Raw(
            Bytes::copy_from_slice(entry.member.as_bytes()),
        ))));
        if options.withdist {
            nested.push(Response::Value(Some(format_geo_distance(entry.distance_m))));
        }
        if options.withhash {
            nested.push(Response::Integer(entry.hash as i64));
        }
        if options.withcoord {
            nested.push(coords_response(entry.longitude, entry.latitude));
        }
        out.push(Response::Array(Box::new(nested)));
    }
    Ok(Response::Array(Box::new(out)))
}

fn store_geo_results(
    store: &mut Store,
    target: CompactString,
    entries: &[GeoEntry],
    store_dist: bool,
) -> SenkoResult<Response> {
    if entries.is_empty() {
        let _ = store.delete(target.as_bytes());
        return Ok(Response::Integer(0));
    }

    let mut out = ZSetObject::default();
    for entry in entries {
        let score = if store_dist {
            entry.distance_m
        } else {
            entry.hash as f64
        };
        let _ = out.add(score, entry.member.clone(), Default::default());
    }

    let _ = store.set(
        target,
        SenkoValue::ZSet(Box::new(out)),
        SetOptions::default(),
    );
    Ok(Response::Integer(entries.len() as i64))
}

fn member_coords(zset: &ZSetObject, member: &[u8]) -> Option<(f64, f64)> {
    zset.score(member).map(score_to_hash).map(decode_geoscore)
}

fn format_geo_distance(distance: f64) -> SenkoValue {
    SenkoValue::Raw(Bytes::from(format!("{distance:.4}")))
}

fn coords_response(longitude: f64, latitude: f64) -> Response {
    let mut out = SmallVec::<[Response; 16]>::new();
    out.push(Response::Value(Some(formatted_score_value(longitude))));
    out.push(Response::Value(Some(formatted_score_value(latitude))));
    Response::Array(Box::new(out))
}

fn compare_geo_entry_asc(left: &GeoEntry, right: &GeoEntry) -> Ordering {
    left.distance_m
        .partial_cmp(&right.distance_m)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.member.as_bytes().cmp(right.member.as_bytes()))
}

fn compare_geo_entry_desc(left: &GeoEntry, right: &GeoEntry) -> Ordering {
    right
        .distance_m
        .partial_cmp(&left.distance_m)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.member.as_bytes().cmp(right.member.as_bytes()))
}

fn validate_coordinates(longitude_raw: &[u8], latitude_raw: &[u8]) -> SenkoResult<()> {
    let longitude = parse_f64(longitude_raw)?;
    let latitude = parse_f64(latitude_raw)?;
    if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&longitude)
        || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&latitude)
    {
        let lon = String::from_utf8_lossy(longitude_raw);
        let lat = String::from_utf8_lossy(latitude_raw);
        return Err(SenkoError::ProtocolMessage(CompactString::from(format!(
            "ERR invalid longitude,latitude pair {lon},{lat}"
        ))));
    }
    Ok(())
}

fn parse_unit(raw: &[u8]) -> SenkoResult<GeoUnit> {
    if raw.eq_ignore_ascii_case(b"m") {
        Ok(GeoUnit { meters: 1.0 })
    } else if raw.eq_ignore_ascii_case(b"km") {
        Ok(GeoUnit { meters: 1_000.0 })
    } else if raw.eq_ignore_ascii_case(b"ft") {
        Ok(GeoUnit { meters: 0.3048 })
    } else if raw.eq_ignore_ascii_case(b"mi") {
        Ok(GeoUnit { meters: 1_609.344 })
    } else {
        Err(SenkoError::Protocol(
            "ERR unsupported unit provided. please use M, KM, FT, MI",
        ))
    }
}

fn parse_f64(raw: &[u8]) -> SenkoResult<f64> {
    fast_float::parse::<f64, _>(raw)
        .map_err(|_| SenkoError::Protocol("ERR value is not a valid float"))
}

fn parse_non_negative_usize(raw: &[u8]) -> SenkoResult<usize> {
    let text = std::str::from_utf8(raw).map_err(|_| SenkoError::Protocol("syntax error"))?;
    let value: i64 = text
        .parse()
        .map_err(|_| SenkoError::Protocol("syntax error"))?;
    usize::try_from(value).map_err(|_| SenkoError::Protocol("syntax error"))
}

fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lon1r = lon1.to_radians();
    let lat1r = lat1.to_radians();
    let lon2r = lon2.to_radians();
    let lat2r = lat2.to_radians();
    let v = ((lon2r - lon1r) / 2.0).sin();
    let u = ((lat2r - lat1r) / 2.0).sin();
    2.0 * EARTH_RADIUS_M * (u * u + lat1r.cos() * lat2r.cos() * v * v).sqrt().asin()
}

fn point_in_box(
    center_lon: f64,
    center_lat: f64,
    point_lon: f64,
    point_lat: f64,
    width_m: f64,
    height_m: f64,
) -> bool {
    let half_width = width_m / 2.0;
    let half_height = height_m / 2.0;
    let lat_distance = haversine_distance(point_lon, point_lat, point_lon, center_lat);
    if lat_distance > half_height {
        return false;
    }

    let lon_delta = wrapped_lon_delta(point_lon, center_lon);
    let adjusted_lon = center_lon + lon_delta;
    let lon_distance = haversine_distance(point_lon, point_lat, adjusted_lon, point_lat);
    lon_distance <= half_width
}

fn wrapped_lon_delta(lon: f64, center_lon: f64) -> f64 {
    let mut delta = lon - center_lon;
    while delta > 180.0 {
        delta -= 360.0;
    }
    while delta < -180.0 {
        delta += 360.0;
    }
    delta
}

fn encode_geoscore(longitude: f64, latitude: f64) -> u64 {
    let lon_norm = ((longitude - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN)).clamp(0.0, 1.0);
    let lat_norm = ((latitude - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN)).clamp(0.0, 1.0);
    let lon_bits = ((lon_norm * GEO_GRID_SCALE).floor() as u32).min(GEO_GRID_MAX);
    let lat_bits = ((lat_norm * GEO_GRID_SCALE).floor() as u32).min(GEO_GRID_MAX);
    interleave_bits(lon_bits, lat_bits)
}

fn decode_geoscore(hash: u64) -> (f64, f64) {
    let (lon_bits, lat_bits) = deinterleave_bits(hash);
    let longitude =
        GEO_LON_MIN + ((lon_bits as f64 + 0.5) / GEO_GRID_SCALE) * (GEO_LON_MAX - GEO_LON_MIN);
    let latitude =
        GEO_LAT_MIN + ((lat_bits as f64 + 0.5) / GEO_GRID_SCALE) * (GEO_LAT_MAX - GEO_LAT_MIN);
    (longitude, latitude)
}

fn interleave_bits(lon_bits: u32, lat_bits: u32) -> u64 {
    let mut out = 0_u64;
    for bit in (0..GEO_STEP_BITS).rev() {
        out = (out << 1) | u64::from((lon_bits >> bit) & 1);
        out = (out << 1) | u64::from((lat_bits >> bit) & 1);
    }
    out
}

fn deinterleave_bits(hash: u64) -> (u32, u32) {
    let mut lon = 0_u32;
    let mut lat = 0_u32;
    for bit in 0..GEO_STEP_BITS {
        let shift = (GEO_STEP_BITS - 1 - bit) * 2;
        lon = (lon << 1) | (((hash >> (shift + 1)) & 1) as u32);
        lat = (lat << 1) | (((hash >> shift) & 1) as u32);
    }
    (lon, lat)
}

fn score_to_hash(score: f64) -> u64 {
    if score.is_sign_negative() {
        0
    } else {
        score.round() as u64
    }
}

fn encode_geohash_string(longitude: f64, latitude: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    let mut is_even = true;
    let mut bit = 0_u8;
    let mut ch = 0_u8;
    let mut lon_range = [GEO_LON_MIN, GEO_LON_MAX];
    let mut lat_range = [-90.0, 90.0];

    while out.len() < 11 {
        if is_even {
            let mid = (lon_range[0] + lon_range[1]) / 2.0;
            if longitude >= mid {
                ch = (ch << 1) | 1;
                lon_range[0] = mid;
            } else {
                ch <<= 1;
                lon_range[1] = mid;
            }
        } else {
            let mid = (lat_range[0] + lat_range[1]) / 2.0;
            if latitude >= mid {
                ch = (ch << 1) | 1;
                lat_range[0] = mid;
            } else {
                ch <<= 1;
                lat_range[1] = mid;
            }
        }
        is_even = !is_even;
        bit += 1;
        if bit == 5 {
            out.push(GEOHASH_ALPHABET[ch as usize]);
            bit = 0;
            ch = 0;
        }
    }
    out[10] = b'0';
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{commands::Response, store::Store};

    fn bs(bytes: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(bytes)
    }

    #[test]
    fn geoadd_and_geopos_roundtrip() {
        let mut store = Store::default();
        let response = geoadd(
            &mut store,
            &[
                bs(b"places"),
                bs(b"13.361389"),
                bs(b"38.115556"),
                bs(b"Palermo"),
            ],
        )
        .unwrap();
        assert_eq!(response, Response::Integer(1));

        let response = geopos(&mut store, &[bs(b"places"), bs(b"Palermo")]).unwrap();
        let Response::Array(values) = response else {
            panic!("expected array");
        };
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn geodist_returns_expected_km_range() {
        let mut store = Store::default();
        let _ = geoadd(
            &mut store,
            &[
                bs(b"places"),
                bs(b"13.361389"),
                bs(b"38.115556"),
                bs(b"Palermo"),
                bs(b"15.087269"),
                bs(b"37.502669"),
                bs(b"Catania"),
            ],
        )
        .unwrap();

        let response = geodist(
            &mut store,
            &[bs(b"places"), bs(b"Palermo"), bs(b"Catania"), bs(b"km")],
        )
        .unwrap();
        let Response::Value(Some(SenkoValue::Raw(value))) = response else {
            panic!("expected km value");
        };
        let distance: f64 = std::str::from_utf8(value.as_ref())
            .unwrap()
            .parse()
            .unwrap();
        assert!((distance - 166.0).abs() < 2.0);
    }

    #[test]
    fn geodist_same_member_uses_fixed_precision() {
        let mut store = Store::default();
        let _ = geoadd(
            &mut store,
            &[
                bs(b"places"),
                bs(b"13.361389"),
                bs(b"38.115556"),
                bs(b"Palermo"),
            ],
        )
        .unwrap();

        let response =
            geodist(&mut store, &[bs(b"places"), bs(b"Palermo"), bs(b"Palermo")]).unwrap();
        let Response::Value(Some(SenkoValue::Raw(value))) = response else {
            panic!("expected value");
        };
        assert_eq!(value.as_ref(), b"0.0000");
    }

    #[test]
    fn georadius_with_store_creates_destination_zset() {
        let mut store = Store::default();
        let _ = geoadd(
            &mut store,
            &[
                bs(b"places"),
                bs(b"13.361389"),
                bs(b"38.115556"),
                bs(b"Palermo"),
                bs(b"15.087269"),
                bs(b"37.502669"),
                bs(b"Catania"),
            ],
        )
        .unwrap();

        let response = georadius(
            &mut store,
            &[
                bs(b"places"),
                bs(b"15"),
                bs(b"37"),
                bs(b"200"),
                bs(b"km"),
                bs(b"STORE"),
                bs(b"nearby"),
            ],
        )
        .unwrap();
        assert_eq!(response, Response::Integer(2));
        assert_eq!(store.get_zset(b"nearby").map(ZSetObject::len), Some(2));
    }

    #[test]
    fn geosearch_box_finds_members() {
        let mut store = Store::default();
        let _ = geoadd(
            &mut store,
            &[
                bs(b"places"),
                bs(b"-73.9733487"),
                bs(b"40.7648057"),
                bs(b"park"),
                bs(b"-73.9903085"),
                bs(b"40.7362513"),
                bs(b"square"),
            ],
        )
        .unwrap();

        let response = geosearch(
            &mut store,
            &[
                bs(b"places"),
                bs(b"FROMLONLAT"),
                bs(b"-73.9798091"),
                bs(b"40.7598464"),
                bs(b"BYBOX"),
                bs(b"6"),
                bs(b"6"),
                bs(b"km"),
                bs(b"ASC"),
            ],
        )
        .unwrap();
        let Response::Array(values) = response else {
            panic!("expected array");
        };
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn geohash_and_geopos_with_only_key_return_empty_array() {
        let mut store = Store::default();
        let _ = geoadd(
            &mut store,
            &[
                bs(b"places"),
                bs(b"10"),
                bs(b"20"),
                bs(b"a"),
                bs(b"30"),
                bs(b"40"),
                bs(b"b"),
            ],
        )
        .unwrap();

        let Response::Array(hashes) = geohash(&mut store, &[bs(b"places")]).unwrap() else {
            panic!("expected array");
        };
        assert!(hashes.is_empty());

        let Response::Array(positions) = geopos(&mut store, &[bs(b"places")]).unwrap() else {
            panic!("expected array");
        };
        assert!(positions.is_empty());
    }

    #[test]
    fn georadius_any_requires_count() {
        let mut store = Store::default();
        let err = georadius(
            &mut store,
            &[
                bs(b"places"),
                bs(b"10"),
                bs(b"20"),
                bs(b"1"),
                bs(b"km"),
                bs(b"ANY"),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("ANY option requires COUNT option"));
    }

    #[test]
    fn geohash_matches_reference_example() {
        let mut store = Store::default();
        let _ = geoadd(
            &mut store,
            &[bs(b"points"), bs(b"-5.6"), bs(b"42.6"), bs(b"test")],
        )
        .unwrap();

        let response = geohash(&mut store, &[bs(b"points"), bs(b"test")]).unwrap();
        let Response::Array(values) = response else {
            panic!("expected array");
        };
        let Response::Value(Some(SenkoValue::Raw(value))) = &values[0] else {
            panic!("expected geohash");
        };
        assert_eq!(value.as_ref(), b"ezs42e44yx0");
    }

    #[test]
    fn geosearch_frommember_missing_member_errors() {
        let mut store = Store::default();
        let _ = geoadd(&mut store, &[bs(b"places"), bs(b"10"), bs(b"20"), bs(b"a")]).unwrap();

        let err = geosearch(
            &mut store,
            &[
                bs(b"places"),
                bs(b"FROMMEMBER"),
                bs(b"missing"),
                bs(b"BYBOX"),
                bs(b"1"),
                bs(b"1"),
                bs(b"km"),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("could not decode requested zset member")
        );
    }
}
