//! `chronoctl` — offline inspection and verification of `*.cdb` files.
//!
//! Subcommands:
//! * `inspect <file>` — superblock facts plus a validated scan summary.
//! * `verify <file> [--repair]` — full structural validation; `--repair`
//!   truncates a torn tail back to the last valid page boundary (SPEC §5.2
//!   step 3).
//! * `recover <file>` — the boot path: validates, repairs a torn tail,
//!   rebuilds the sparse index and resumes the engine (SPEC §5.2), then
//!   reports what was recovered and closes the file.
//! * `tail <file> [count]` — prints the most recent records (default 10).
//! * `query <file> --id <uuid|hex|dec> [--as-of <rfc3339|unix-ns>]` —
//!   resolves one entity as of a timestamp (default: latest) by scanning
//!   the file; tombstone semantics match the engine's `get_as_of`.

use std::collections::VecDeque;
use std::env;
use std::path::Path;
use std::process::ExitCode;

use chronodb::engine::{Engine, EngineOptions, OpenOptions};
use chronodb::error::Result;
use chronodb::storage::file::DbFile;
use chronodb::storage::scanner::RecordView;

const DEFAULT_TAIL: usize = 10;

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("inspect") => cmd_inspect(&args[1..]),
        Some("verify") => cmd_verify(&args[1..]),
        Some("recover") => cmd_recover(&args[1..]),
        Some("tail") => cmd_tail(&args[1..]),
        Some("query") => cmd_query(&args[1..]),
        Some(other) => {
            eprintln!("chronoctl: unknown subcommand `{other}`");
            usage()
        }
        None => usage(),
    }
}

fn usage() -> ExitCode {
    eprintln!(
        "usage:\n  \
         chronoctl inspect <file>\n  \
         chronoctl verify  <file> [--repair]\n  \
         chronoctl recover <file>\n  \
         chronoctl tail    <file> [count]\n  \
         chronoctl query   <file> --id <uuid|hex|dec> [--as-of <rfc3339|unix-ns>]"
    );
    ExitCode::from(2)
}

fn cmd_inspect(args: &[String]) -> ExitCode {
    let Some(path) = args.first() else {
        return usage();
    };
    match inspect(Path::new(path)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("chronoctl: {e}");
            ExitCode::FAILURE
        }
    }
}

fn inspect(path: &Path) -> Result<()> {
    let (db, sb) = DbFile::open_read_only(path)?;
    println!("file            : {}", db.path().display());
    println!("bytes           : {}", db.len()?);
    println!("alloc mode      : {:?}", db.alloc_mode());
    println!("cluster id      : {:#034x}", sb.cluster_id());
    println!("format version  : {}", sb.version());
    println!("page size       : {}", sb.page_size());
    println!("created at (ns) : {}", sb.created_at_ns());
    println!("superblock flags: {:#010x}", sb.flags());
    println!("strict O_DIRECT : {}", sb.strict_direct_io());

    let mut records = 0u64;
    let mut tombstones = 0u64;
    let mut min_ts = u64::MAX;
    let mut max_ts = 0u64;
    let report = chronodb::storage::scanner::scan_and_validate(&db, false, |rec| {
        records += 1;
        if rec.is_tombstone() {
            tombstones += 1;
        }
        min_ts = min_ts.min(rec.timestamp_ns());
        max_ts = max_ts.max(rec.timestamp_ns());
        Ok(())
    })?;

    println!("data pages      : {}", report.pages.len() as u64);
    println!("free tail pages : {}", report.free_tail_pages);
    println!("records         : {records} ({tombstones} tombstones)");
    match report.min_lsn {
        Some(min) => println!(
            "lsn range       : {min}..={}",
            report.max_lsn.unwrap_or(min)
        ),
        None => println!("lsn range       : (empty)"),
    }
    if records > 0 {
        println!("timestamp range : {min_ts}..={max_ts}");
    }
    println!("status          : ok");
    Ok(())
}

fn cmd_verify(args: &[String]) -> ExitCode {
    let mut repair = false;
    let mut path: Option<&String> = None;
    for a in args {
        match a.as_str() {
            "--repair" => repair = true,
            _ => {
                if path.is_some() {
                    return usage();
                }
                path = Some(a);
            }
        }
    }
    let Some(path) = path else {
        return usage();
    };
    match verify(Path::new(path), repair) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("chronoctl: {e}");
            ExitCode::FAILURE
        }
    }
}

fn verify(path: &Path, repair: bool) -> Result<()> {
    let db = if repair {
        DbFile::open(path)?.0
    } else {
        DbFile::open_read_only(path)?.0
    };
    let mut records = 0u64;
    let report = chronodb::storage::scanner::scan_and_validate(&db, repair, |_rec| {
        records += 1;
        Ok(())
    })?;
    match report.truncated_bytes {
        Some(bytes) => {
            println!(
                "{}: repaired — truncated {bytes} bytes of torn tail; {records} records in \
                 {} pages",
                db.path().display(),
                report.pages.len()
            );
        }
        None => {
            println!(
                "{}: ok — {records} records, {} data pages, {} free tail pages",
                db.path().display(),
                report.pages.len(),
                report.free_tail_pages
            );
        }
    }
    Ok(())
}

fn cmd_recover(args: &[String]) -> ExitCode {
    let Some(path) = args.first() else {
        return usage();
    };
    match recover(Path::new(path)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("chronoctl: {e}");
            ExitCode::FAILURE
        }
    }
}

/// The boot path of SPEC §5.2: validate, truncate a torn tail, rebuild the
/// sparse index and resume the engine, then close it again.
fn recover(path: &Path) -> Result<()> {
    let (engine, report) =
        Engine::open_with(path, EngineOptions::default(), OpenOptions::default())?;
    match report.truncated_bytes {
        0 => println!("RECOVER: clean file — no truncation needed"),
        n => println!("RECOVER: Truncated {n} corrupt uncommitted bytes at file tail"),
    }
    println!("file            : {}", path.display());
    println!("records         : {}", report.record_count);
    println!("data pages      : {}", report.data_pages);
    println!("free tail pages : {}", report.free_tail_pages);
    println!(
        "resume lsn      : {}",
        if report.last_lsn == 0 {
            "(empty)".to_string()
        } else {
            (report.last_lsn + 1).to_string()
        }
    );
    println!(
        "resume ts (ns)  : {}",
        if report.last_ts == 0 {
            "(empty)".to_string()
        } else {
            (report.last_ts + 1).to_string()
        }
    );
    let stats = engine.close(true)?;
    println!(
        "closed          : committed_lsn={} durable_lsn={} pages={}",
        stats.committed_lsn, stats.durable_lsn, stats.pages_flushed
    );
    Ok(())
}

struct TailRow {
    lsn: u64,
    entity_id: u128,
    timestamp_ns: u64,
    payload_len: usize,
    tombstone: bool,
}

fn cmd_tail(args: &[String]) -> ExitCode {
    let Some(path) = args.first() else {
        return usage();
    };
    let count = match args.get(1) {
        Some(s) => match s.parse::<usize>() {
            Ok(n) if n > 0 => n,
            _ => {
                eprintln!("chronoctl: `{}` is not a positive count", s);
                return usage();
            }
        },
        None => DEFAULT_TAIL,
    };
    match tail(Path::new(path), count) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("chronoctl: {e}");
            ExitCode::FAILURE
        }
    }
}

fn tail(path: &Path, count: usize) -> Result<()> {
    let db = DbFile::open_read_only(path)?.0;
    let mut ring: VecDeque<TailRow> = VecDeque::with_capacity(count + 1);
    chronodb::storage::scanner::for_each_record(&db, false, |rec: RecordView<'_>| {
        if ring.len() == count {
            ring.pop_front();
        }
        ring.push_back(TailRow {
            lsn: rec.lsn(),
            entity_id: rec.entity_id(),
            timestamp_ns: rec.timestamp_ns(),
            payload_len: rec.payload.len(),
            tombstone: rec.is_tombstone(),
        });
        Ok(())
    })?;
    println!(
        "{:>10}  {:>34}  {:>20}  {:>8}  flags",
        "lsn", "entity_id", "timestamp_ns", "payload"
    );
    for row in ring {
        let flags = if row.tombstone { "tombstone" } else { "" };
        println!(
            "{:>10}  {:#034x}  {:>20}  {:>8}  {}",
            row.lsn, row.entity_id, row.timestamp_ns, row.payload_len, flags
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

struct QueryArgs {
    path: String,
    entity_id: u128,
    /// Inclusive upper bound in nanoseconds; `None` resolves the latest.
    as_of_ns: Option<u64>,
}

struct QueryHit {
    lsn: u64,
    timestamp_ns: u64,
    tombstone: bool,
    payload: Vec<u8>,
}

fn cmd_query(args: &[String]) -> ExitCode {
    let mut path: Option<&String> = None;
    let mut id: Option<u128> = None;
    let mut as_of: Option<u64> = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--id" | "--entity" => {
                let Some(value) = iter.next() else {
                    eprintln!("chronoctl: `{arg}` needs a value");
                    return usage();
                };
                match parse_entity_id(value) {
                    Some(parsed) => id = Some(parsed),
                    None => {
                        eprintln!("chronoctl: `{value}` is not a uuid/hex/decimal entity id");
                        return usage();
                    }
                }
            }
            "--as-of" | "--at" => {
                let Some(value) = iter.next() else {
                    eprintln!("chronoctl: `{arg}` needs a value");
                    return usage();
                };
                match parse_timestamp_ns(value) {
                    Some(parsed) => as_of = Some(parsed),
                    None => {
                        eprintln!(
                            "chronoctl: `{value}` is not an RFC 3339 timestamp or unix-ns value"
                        );
                        return usage();
                    }
                }
            }
            other if other.starts_with("--") => {
                eprintln!("chronoctl: unknown flag `{other}`");
                return usage();
            }
            _ => {
                if path.is_some() {
                    return usage();
                }
                path = Some(arg);
            }
        }
    }
    let (Some(path), Some(entity_id)) = (path, id) else {
        return usage();
    };
    let args = QueryArgs {
        path: path.clone(),
        entity_id,
        as_of_ns: as_of,
    };
    match query(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("chronoctl: {e}");
            ExitCode::FAILURE
        }
    }
}

fn query(args: &QueryArgs) -> Result<()> {
    let db = DbFile::open_read_only(Path::new(&args.path))?.0;
    let mut best: Option<QueryHit> = None;
    let mut considered = 0u64;
    chronodb::storage::scanner::scan_and_validate(&db, false, |rec: RecordView<'_>| {
        if rec.entity_id() != args.entity_id {
            return Ok(());
        }
        if let Some(limit) = args.as_of_ns {
            if rec.timestamp_ns() > limit {
                return Ok(());
            }
        }
        considered += 1;
        let key = (rec.timestamp_ns(), rec.lsn());
        let better = best
            .as_ref()
            .map_or(true, |b| key > (b.timestamp_ns, b.lsn));
        if better {
            best = Some(QueryHit {
                lsn: rec.lsn(),
                timestamp_ns: rec.timestamp_ns(),
                tombstone: rec.is_tombstone(),
                payload: rec.payload.to_vec(),
            });
        }
        Ok(())
    })?;

    println!("entity        : {:#034x}", args.entity_id);
    match args.as_of_ns {
        Some(limit) => println!("as-of (ns)    : {limit}"),
        None => println!("as-of (ns)    : latest"),
    }
    match best {
        None => {
            println!("state         : no record at or before the query time");
            Ok(())
        }
        Some(hit) if hit.tombstone => {
            println!("state         : deleted (tombstone)");
            println!("lsn           : {}", hit.lsn);
            println!("timestamp (ns): {}", hit.timestamp_ns);
            println!("versions seen : {considered}");
            Ok(())
        }
        Some(hit) => {
            println!("state         : present");
            println!("lsn           : {}", hit.lsn);
            println!("timestamp (ns): {}", hit.timestamp_ns);
            println!("payload (bytes): {}", hit.payload.len());
            println!("versions seen : {considered}");
            println!("payload       : {}", payload_preview(&hit.payload));
            Ok(())
        }
    }
}

/// UUID / hex / decimal entity id.
fn parse_entity_id(input: &str) -> Option<u128> {
    let trimmed = input.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        return u128::from_str_radix(hex, 16).ok();
    }
    let digits: String = trimmed.chars().filter(|c| *c != '-').collect();
    if trimmed.contains('-') {
        // UUID form: 32 hex digits once the dashes are gone.
        if digits.len() != 32 {
            return None;
        }
        return u128::from_str_radix(&digits, 16).ok();
    }
    if digits.chars().all(|c| c.is_ascii_digit()) && !digits.is_empty() {
        return digits.parse().ok();
    }
    u128::from_str_radix(&digits, 16).ok()
}

/// `unix nanoseconds` (all digits) or an RFC 3339 timestamp with an
/// optional fractional second and `Z` / `±HH:MM` offset.
fn parse_timestamp_ns(input: &str) -> Option<u64> {
    let trimmed = input.trim();
    if !trimmed.is_empty() && trimmed.chars().all(|c| c.is_ascii_digit()) {
        return trimmed.parse().ok();
    }

    let (date, rest) = trimmed.split_once(['T', 't'])?;
    let mut ymd = date.split('-');
    let year: i64 = ymd.next()?.parse().ok()?;
    let month: u32 = ymd.next()?.parse().ok()?;
    let day: u32 = ymd.next()?.parse().ok()?;
    if ymd.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let (time, offset_secs): (&str, i64) =
        if let Some(body) = rest.strip_suffix('Z').or_else(|| rest.strip_suffix('z')) {
            (body, 0)
        } else if let Some(pos) = rest.rfind(['+', '-']) {
            let (time, offset) = rest.split_at(pos);
            let sign: i64 = if offset.starts_with('+') { 1 } else { -1 };
            let mut hm = offset[1..].split(':');
            let hours: i64 = hm.next()?.parse().ok()?;
            let minutes: i64 = hm.next().unwrap_or("0").parse().ok()?;
            (time, sign * (hours * 3600 + minutes * 60))
        } else {
            (rest, 0)
        };

    let mut hms = time.split(':');
    let hours: u64 = hms.next()?.parse().ok()?;
    let minutes: u64 = hms.next()?.parse().ok()?;
    let (seconds_str, fraction) = match hms.next() {
        Some(s) => s
            .split_once('.')
            .map_or((s, ""), |(whole, frac)| (whole, frac)),
        None => ("0", ""),
    };
    let seconds: u64 = seconds_str.parse().ok()?;
    if hms.next().is_some() || hours > 23 || minutes > 59 || seconds > 59 {
        return None;
    }
    let mut fraction_ns: u64 = 0;
    if !fraction.is_empty() {
        if !fraction.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let mut padded = fraction[..fraction.len().min(9)].to_string();
        while padded.len() < 9 {
            padded.push('0');
        }
        fraction_ns = padded.parse().ok()?;
    }

    let days = days_from_civil(year, month, day);
    let secs =
        days * 86_400 + hours as i64 * 3_600 + minutes as i64 * 60 + seconds as i64 - offset_secs;
    if secs < 0 {
        return None;
    }
    Some(secs as u64 * 1_000_000_000 + fraction_ns)
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = ((month + 9) % 12) as i64; // Mar = 0 … Feb = 11
    let doy = (153 * mp + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Human-friendly payload rendering: quoted UTF-8, or a hex preview when
/// the payload is binary; both truncated.
fn payload_preview(payload: &[u8]) -> String {
    const MAX_CHARS: usize = 120;
    const MAX_HEX: usize = 64;
    match std::str::from_utf8(payload) {
        Ok(text) => {
            let mut preview: String = text.chars().take(MAX_CHARS).collect();
            if text.chars().nth(MAX_CHARS).is_some() {
                preview.push('…');
            }
            format!("{preview:?}")
        }
        Err(_) => {
            let hex: String = payload
                .iter()
                .take(MAX_HEX)
                .map(|b| format!("{b:02x}"))
                .collect();
            if payload.len() > MAX_HEX {
                format!("hex:{hex}… ({} bytes)", payload.len())
            } else {
                format!("hex:{hex}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_ids_parse_in_all_accepted_forms() {
        let expected: u128 = 0x018f_2d8b_7b00_7c2a_96e0_7984_f4f3_4f71;
        assert_eq!(
            parse_entity_id("018f2d8b-7b00-7c2a-96e0-7984f4f34f71"),
            Some(expected)
        );
        assert_eq!(
            parse_entity_id("0x018f2d8b7b007c2a96e07984f4f34f71"),
            Some(expected)
        );
        assert_eq!(parse_entity_id("42"), Some(42));
        assert_eq!(parse_entity_id("deadbeef"), Some(0xdead_beef));
        assert_eq!(parse_entity_id("not-an-id"), None);
    }

    #[test]
    fn timestamps_parse_as_ns_or_rfc3339() {
        assert_eq!(parse_timestamp_ns("0"), Some(0));
        assert_eq!(
            parse_timestamp_ns("1792317600000000000"),
            Some(1_792_317_600_000_000_000)
        );
        assert_eq!(parse_timestamp_ns("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_timestamp_ns("2026-10-18T10:00:00Z"),
            Some(1_792_317_600_000_000_000)
        );
        assert_eq!(
            parse_timestamp_ns("2026-10-18T10:00:00.5Z"),
            Some(1_792_317_600_500_000_000)
        );
        // Offsets are applied.
        assert_eq!(
            parse_timestamp_ns("2026-10-18T12:00:00+02:00"),
            Some(1_792_317_600_000_000_000)
        );
        assert_eq!(
            parse_timestamp_ns("2026-10-18T08:30:00-01:30"),
            Some(1_792_317_600_000_000_000)
        );
        // Malformed inputs are rejected.
        assert_eq!(parse_timestamp_ns("2026-13-18T10:00:00Z"), None);
        assert_eq!(parse_timestamp_ns("2026-10-18T25:00:00Z"), None);
        assert_eq!(parse_timestamp_ns("yesterday"), None);
    }

    #[test]
    fn civil_day_arithmetic_matches_known_epochs() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(days_from_civil(2026, 10, 18), 20_744);
    }

    #[test]
    fn payload_previews_cover_text_and_binary() {
        assert_eq!(payload_preview(b"hello"), "\"hello\"");
        assert_eq!(payload_preview(&[0x00, 0xFF]), "hex:00ff");
        let long = vec![b'x'; 200];
        let preview = payload_preview(&long);
        assert!(preview.contains('…') && preview.len() < 200);
    }
}
