{ lib, stdenvNoCC, sourcePath, runtimeAbiKey }:

stdenvNoCC.mkDerivation {
  pname = lib.strings.sanitizeDerivationName "fluxon-cxxpacked-${runtimeAbiKey}";
  version = "0";
  src = sourcePath;

  dontConfigure = true;
  dontBuild = true;

  installPhase = ''
    mkdir -p "$out"
    cp -a "$src"/. "$out"/
    cat > "$out/.fluxon-authority.json" <<'EOF'
    ${builtins.toJSON {
      object_kind = "FluxonCxxpacked";
      source_kind = "prebuilt_tree";
      runtime_abi_key = runtimeAbiKey;
      target_dir_name = "cxxpacked";
    }}
    EOF
  '';

  meta = {
    description = "Packaged cxxpacked runtime tree for Fluxon manylinux profiles";
    platforms = lib.platforms.linux;
  };
}
