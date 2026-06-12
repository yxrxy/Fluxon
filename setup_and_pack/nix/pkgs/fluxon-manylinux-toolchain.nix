{ lib
, runCommandNoCC
, baseSystemKey
, runtimeAbiKey
, manylinuxVersion
, pythonAbiTag
, runtimeImageRef
, containerImageDigest
}:

runCommandNoCC
  (lib.strings.sanitizeDerivationName "fluxon-manylinux-toolchain-${runtimeAbiKey}")
  { }
  ''
    mkdir -p "$out"
    cat > "$out/.fluxon-authority.json" <<'EOF'
    ${builtins.toJSON {
      object_kind = "FluxonManylinuxToolchain";
      base_system_key = baseSystemKey;
      runtime_abi_key = runtimeAbiKey;
      manylinux_version = manylinuxVersion;
      python_abi_tag = pythonAbiTag;
      runtime_image_ref = runtimeImageRef;
      container_image_digest = containerImageDigest;
      mount_contract = {
        nix_root = "/nix";
        workspace_root = "/workspace";
        release_root = "/release";
      };
    }}
    EOF
  ''
