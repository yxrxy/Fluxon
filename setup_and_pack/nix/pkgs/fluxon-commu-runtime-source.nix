{ lib, stdenvNoCC, fluxonCommuSource, crateVersion }:

stdenvNoCC.mkDerivation {
  pname = "fluxon-commu-runtime-source";
  version = crateVersion;
  src = fluxonCommuSource;

  dontConfigure = true;
  dontBuild = true;

  installPhase = ''
    mkdir -p "$out"
    cp -a "$src"/. "$out"/
    cat > "$out/.fluxon-authority.json" <<'EOF'
    ${builtins.toJSON {
      object_kind = "FluxonCommuAuthority";
      authority_kind = "runtime_source";
      crate_name = "fluxon_commu";
      crate_version = crateVersion;
    }}
    EOF
  '';

  meta = {
    description = "Runtime-source authority object for fluxon_commu";
    platforms = lib.platforms.linux;
  };
}
