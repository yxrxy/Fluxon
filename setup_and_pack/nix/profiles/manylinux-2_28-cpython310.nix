{ lib
, runCommandNoCC
, profileName
, assemblyName
, baseSystemKey
, runtimeAbiKey
, nativeInputObjectIds
, toolchain
, workspaceSeed
, targetSupport
, vendorRuntime
, nativeRuntime
, cxxpacked
, fluxonPyo3Wheel
}:

let
  targetSupportDirNames = targetSupport.targetDirNames;
  nativeInputsById = {
    vendorRuntime = vendorRuntime;
    nativeRuntime = nativeRuntime;
    cxxpacked = cxxpacked;
  };
  nativeObjectIdToDirName = {
    vendorRuntime = "vendor_runtime";
    nativeRuntime = "native_runtime";
    cxxpacked = "cxxpacked";
  };
  nativeDirNames =
    map
      (objectId:
        if builtins.hasAttr objectId nativeObjectIdToDirName then
          builtins.getAttr objectId nativeObjectIdToDirName
        else
          throw "unknown native profile object id: ${objectId}"
      )
      nativeInputObjectIds;
in
runCommandNoCC
  (lib.strings.sanitizeDerivationName "fluxon-manylinux-profile-${profileName}-${runtimeAbiKey}")
  { }
  ''
    mkdir -p "$out/profile/native" "$out/profile/target_support"
    ln -s ${toolchain} "$out/profile/toolchain"
    ln -s ${workspaceSeed} "$out/profile/workspace_seed"
    for dir_name in ${lib.escapeShellArgs targetSupportDirNames}; do
      ln -s "${targetSupport}/$dir_name" "$out/profile/target_support/$dir_name"
    done
    ${lib.concatMapStringsSep "\n" (
      objectId:
      let
        dirName =
          if builtins.hasAttr objectId nativeObjectIdToDirName then
            builtins.getAttr objectId nativeObjectIdToDirName
          else
            throw "unknown native profile object id: ${objectId}";
        storePath =
          if builtins.hasAttr objectId nativeInputsById then
            builtins.getAttr objectId nativeInputsById
          else
            throw "missing native profile input for object id: ${objectId}";
      in
      ''ln -s ${storePath} "$out/profile/native/${dirName}"''
    ) nativeInputObjectIds}
    ln -s ${fluxonPyo3Wheel} "$out/profile/fluxon_pyo3_wheel"
    cat > "$out/profile/manifest.json" <<'EOF'
    ${builtins.toJSON {
      object_kind = "FluxonManylinuxProfile";
      profile_name = profileName;
      assembly_name = assemblyName;
      base_system_key = baseSystemKey;
      runtime_abi_key = runtimeAbiKey;
      native_runtime_dir_names = nativeDirNames;
      target_support_dir_names = targetSupportDirNames;
      store_paths = {
        toolchain = toString toolchain;
        workspace_seed = toString workspaceSeed;
        target_support = toString targetSupport;
        native = builtins.listToAttrs (
          map
            (objectId: {
              name =
                if builtins.hasAttr objectId nativeObjectIdToDirName then
                  builtins.getAttr objectId nativeObjectIdToDirName
                else
                  throw "unknown native profile object id: ${objectId}";
              value =
                toString (
                  if builtins.hasAttr objectId nativeInputsById then
                    builtins.getAttr objectId nativeInputsById
                  else
                    throw "missing native profile input for object id: ${objectId}"
                );
            })
            nativeInputObjectIds
        );
        fluxon_pyo3_wheel = toString fluxonPyo3Wheel;
      };
    }}
    EOF
  ''
