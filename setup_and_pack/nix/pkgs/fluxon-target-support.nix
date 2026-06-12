{ lib, stdenvNoCC, sourcePath, runtimeAbiKey, targetDirNames }:

assert builtins.isList targetDirNames;
assert targetDirNames != [ ];
assert builtins.all (name: builtins.isString name && name != "") targetDirNames;

stdenvNoCC.mkDerivation {
  pname = lib.strings.sanitizeDerivationName "fluxon-target-support-${runtimeAbiKey}";
  version = "0";
  src = sourcePath;

  dontConfigure = true;
  dontBuild = true;

  installPhase = ''
    mkdir -p "$out"
    for dir_name in ${lib.escapeShellArgs targetDirNames}; do
      if [ ! -d "$src/$dir_name" ]; then
        echo "missing required target support dir: $src/$dir_name" >&2
        exit 1
      fi
      cp -a "$src/$dir_name" "$out/$dir_name"
    done
    cat > "$out/.fluxon-authority.json" <<'EOF'
    ${builtins.toJSON {
      object_kind = "FluxonTargetSupport";
      source_kind = "prebuilt_tree";
      runtime_abi_key = runtimeAbiKey;
      target_dir_names = targetDirNames;
    }}
    EOF
  '';

  passthru = {
    inherit targetDirNames;
  };

  meta = {
    description = "Packaged target support runtime tree for Fluxon manylinux profiles";
    platforms = lib.platforms.linux;
  };
}
