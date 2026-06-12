{ lib, stdenvNoCC, workspaceSeedPath, projectRoot, transportBackend, runtimeAbiKey }:

stdenvNoCC.mkDerivation {
  pname = lib.strings.sanitizeDerivationName "fluxon-workspace-seed-${runtimeAbiKey}-${transportBackend}";
  version = "0";
  src = workspaceSeedPath;

  dontConfigure = true;
  dontBuild = true;

  installPhase = ''
    mkdir -p "$out"
    cp -a "$src"/. "$out"/
    cat > "$out/.fluxon-authority.json" <<'EOF'
    ${builtins.toJSON {
      object_kind = "FluxonWorkspaceSeed";
      source_kind = "workspace_snapshot";
      project_root = toString projectRoot;
      transport_backend = transportBackend;
      runtime_abi_key = runtimeAbiKey;
    }}
    EOF
  '';

  meta = {
    description = "Immutable workspace seed for Fluxon manylinux builds";
    platforms = lib.platforms.linux;
  };
}
