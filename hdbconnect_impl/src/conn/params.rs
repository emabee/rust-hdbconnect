pub mod connect_params;
pub mod connect_params_builder;
pub mod cp_url;
pub mod into_connect_params;
pub mod into_connect_params_builder;
pub(crate) mod tls;

#[derive(Debug, Clone, Default, Copy, Eq, PartialEq, Deserialize)]
pub(crate) enum Compression {
    Off,
    // Remote,
    #[default]
    Always,
}
