use bitcode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::IpAddr;

/// Network filtering and extension policy propagated through the master member record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    pub subnet_whitelist: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_ip_to_extended_ips: Option<BTreeMap<String, Vec<String>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IpCidr {
    pub(crate) addr: IpAddr,
    pub(crate) prefix: u8,
}

pub(crate) fn parse_ip_cidr(s: &str) -> Result<IpCidr, String> {
    let s = s.trim();
    let (ip_str, prefix_str) = s.split_once('/').ok_or_else(|| "missing '/'".to_string())?;
    let ip: IpAddr = ip_str
        .trim()
        .parse()
        .map_err(|e| format!("invalid ip '{}': {}", ip_str, e))?;
    let prefix: u8 = prefix_str
        .trim()
        .parse()
        .map_err(|e| format!("invalid prefix '{}': {}", prefix_str, e))?;

    match ip {
        IpAddr::V4(_) => {
            if prefix > 32 {
                return Err(format!("ipv4 prefix out of range: {}", prefix));
            }
        }
        IpAddr::V6(_) => {
            if prefix > 128 {
                return Err(format!("ipv6 prefix out of range: {}", prefix));
            }
        }
    }

    Ok(IpCidr { addr: ip, prefix })
}

pub(crate) fn ip_in_cidr(ip: IpAddr, cidr: IpCidr) -> bool {
    match (ip, cidr.addr) {
        (IpAddr::V4(ipv4), IpAddr::V4(net)) => {
            let ip_u32 = u32::from_be_bytes(ipv4.octets());
            let net_u32 = u32::from_be_bytes(net.octets());
            let mask = if cidr.prefix == 0 {
                0
            } else {
                u32::MAX << (32 - cidr.prefix)
            };
            (ip_u32 & mask) == (net_u32 & mask)
        }
        (IpAddr::V6(ipv6), IpAddr::V6(net)) => {
            let ip_u128 = u128::from_be_bytes(ipv6.octets());
            let net_u128 = u128::from_be_bytes(net.octets());
            let mask = if cidr.prefix == 0 {
                0
            } else {
                u128::MAX << (128 - cidr.prefix)
            };
            (ip_u128 & mask) == (net_u128 & mask)
        }
        _ => false,
    }
}

pub fn validate_ip_cidr(s: &str) -> Result<(), String> {
    parse_ip_cidr(s).map(|_| ())
}
