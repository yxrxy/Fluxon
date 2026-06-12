{ lib
, stdenvNoCC
, sourcePath
, baseSystemKey
, runtimeAbiKey
, buildAuthority
, fluxonCommuAuthority
, workspaceSeed
, vendorRuntime
, cxxpacked
}:

assert builtins.isAttrs buildAuthority;

stdenvNoCC.mkDerivation {
  pname = lib.strings.sanitizeDerivationName "fluxon-native-runtime-${runtimeAbiKey}";
  version = "0";
  src = sourcePath;

  dontConfigure = true;
  dontBuild = true;

  installPhase = ''
    mkdir -p "$out"
    cp -a "$src"/. "$out"/
    cat > "$out/.fluxon-authority.json" <<'EOF'
    ${builtins.toJSON {
      object_kind = "FluxonNativeRuntime";
      source_kind = "prebuilt_tree";
      base_system_key = baseSystemKey;
      runtime_abi_key = runtimeAbiKey;
      target_dir_name = "native_runtime";
      build_authority = buildAuthority;
      store_inputs = {
        fluxon_commu_authority = toString fluxonCommuAuthority;
        workspace_seed = toString workspaceSeed;
        vendor_runtime = toString vendorRuntime;
        cxxpacked = toString cxxpacked;
      };
    }}
    EOF
  '';

  meta = {
    description = "Packaged native runtime tree for Fluxon manylinux profiles";
    platforms = lib.platforms.linux;
  };
}
