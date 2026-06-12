pub(crate) const CURRENT_PROVIDER_BOUNDARY_MODE: &str = "closed-sdk-consumer";

pub(crate) type ProviderRuntimeAnchor = fluxon_commu_closed_sdk_consumer::ClosedSdkRuntimeAnchor;

pub(crate) fn current_provider_runtime_anchor() -> ProviderRuntimeAnchor {
    fluxon_commu_closed_sdk_consumer::runtime_anchor()
}
