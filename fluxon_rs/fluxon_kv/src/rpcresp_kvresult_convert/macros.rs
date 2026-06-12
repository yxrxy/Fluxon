/// Define a group of errors with per-error codes, fields, and optional message templates.
///
/// Syntax:
/// define_err_group! {
///   config {
///     (1001, InvalidLogLevel { level: String }, msg: "Invalid log level: {level}"),
///     (1002, InvalidPort { port: u16 }, msg: "Invalid port: {port}"),
///     (1003, MissingField { name: String }) // no msg => JSON desc
///   }
/// }
#[macro_export]
macro_rules! define_err_group {
    ($group:ident { $( ( $code:expr, $name:ident { $( $field:ident : $fty:ty ),* $(,)? } $(, msg: $tmpl:literal )? ) ),+ $(,)? }) => {
        // No per-variant structs; use only the enum + code constants

        // Export codes_* constants
        ::paste::paste! {
            pub mod [<codes_ $group>] {
                $( pub const [<$group:snake:upper _ $name:snake:upper>]: u32 = $code; )+
            }
        }

        // Top-level group enum with helpers (generic serde encode/decode)
        ::paste::paste! {
            // Use adjacently-tagged representation to avoid ambiguity when deserializing variants
            // keeping compatibility and explicitness.
            #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::thiserror::Error)]
            // e.g. { "type": "OwnerStartTimeMismatch", "data": { "expected": 1, "got": 2 } }
            #[serde(tag = "type", content = "data")]
            pub enum [<$group:camel Error>] {
                $(
                    $( #[error($tmpl)] )?
                    $name { $( $field : $fty ),* }
                ),+
            }
            impl [<$group:camel Error>] {
                pub fn code(&self) -> u32 { match self { $( Self::$name { .. } => $code ),+ } }
                pub fn to_code_and_json(&self) -> (u32, String) {
                    let code = self.code();
                    // Serialize as adjacently-tagged JSON
                    let json = ::serde_json::to_string(self).unwrap();
                    (code, json)
                }
                pub fn from_code_and_json(code: u32, json: &str) -> Option<Self> {
                    // Decode directly into the enum via serde
                    let v: Self = ::serde_json::from_str(json).ok()?;
                    if v.code() == code { Some(v) } else { None }
                }
            }
        }
    };
}
