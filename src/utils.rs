use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

/// Parse "WWW-Authenticate" header format
pub fn parse_www_authenticate(header: &str) -> Option<HashMap<String, String>> {
    let mut parts = header.splitn(2, " ");
    let _ = parts.next()?.to_string();
    let params = parts.next().unwrap_or("");

    let mut param_map = HashMap::new();
    for param in params.split(",") {
        let mut kv = param.trim().splitn(2, "=");
        if let (Some(key), Some(value)) = (kv.next(), kv.next()) {
            let value = value.trim_matches('"').to_string();
            param_map.insert(key.to_string(), value);
        }
    }

    Some(param_map)
}

/// Get current timestamp of source time
pub fn to_timestamp(source: SystemTime) -> Result<i64, anyhow::Error> {
    let now = source.duration_since(UNIX_EPOCH)?;

    let ts = i64::try_from(now.as_nanos())?;
    Ok(ts)
}

/// Get current timestamp
pub fn timestamp() -> Result<i64, anyhow::Error> {
    let now = SystemTime::now();
    to_timestamp(now)
}
