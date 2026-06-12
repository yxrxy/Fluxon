{ lib, stdenvNoCC, wheelPath, runtimeAbiKey, pythonAbiTag, transportBackend }:

let
  wheelFileName = builtins.baseNameOf (toString wheelPath);
in
stdenvNoCC.mkDerivation {
  pname = lib.strings.sanitizeDerivationName "fluxon-pyo3-wheel-${runtimeAbiKey}-${transportBackend}";
  version = "0";
  src = wheelPath;

  dontConfigure = true;
  dontBuild = true;

  installPhase = ''
    mkdir -p "$out"
    install -Dm644 "$src" "$out/${wheelFileName}"
    cat > "$out/.fluxon-authority.json" <<'EOF'
    ${builtins.toJSON {
      object_kind = "FluxonPyo3Wheel";
      runtime_abi_key = runtimeAbiKey;
      python_abi_tag = pythonAbiTag;
      transport_backend = transportBackend;
      wheel_file_name = wheelFileName;
    }}
    EOF
  '';

  meta = {
    description = "Packaged Fluxon PyO3 wheel artifact";
    platforms = lib.platforms.linux;
  };
}
