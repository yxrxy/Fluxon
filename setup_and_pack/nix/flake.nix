{
  description = "Isolated scripts/nix experiments for Fluxon manylinux and Nix profile flows";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs =
    { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      mkFluxonLib =
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          mkFluxonWorkspaceSeed = args: pkgs.callPackage ./pkgs/fluxon-workspace-seed.nix args;
          mkFluxonManylinuxToolchain = args: pkgs.callPackage ./pkgs/fluxon-manylinux-toolchain.nix args;
          mkFluxonCommuRuntimeSource = args: pkgs.callPackage ./pkgs/fluxon-commu-runtime-source.nix args;
          mkFluxonTargetSupport = args: pkgs.callPackage ./pkgs/fluxon-target-support.nix args;
          mkFluxonVendorRuntime = args: pkgs.callPackage ./pkgs/fluxon-vendor-runtime.nix args;
          mkFluxonNativeRuntime = args: pkgs.callPackage ./pkgs/fluxon-native-runtime.nix args;
          mkFluxonCxxpacked = args: pkgs.callPackage ./pkgs/fluxon-cxxpacked.nix args;
          mkFluxonPyo3Wheel = args: pkgs.callPackage ./pkgs/fluxon-pyo3-wheel.nix args;
          mkManylinux228Cpython310Profile = args: pkgs.callPackage ./profiles/manylinux-2_28-cpython310.nix args;
        };
    in
    {
      lib = {
        supportedSystems = systems;
        forSystem = mkFluxonLib;
      };

      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          pythonEnv = pkgs.python312.withPackages (ps: [ ps.pyyaml ]);
        in
        {
          default = pkgs.mkShell {
            packages = [
              pythonEnv
              pkgs.coreutils
            ];
          };
        }
      );

      packages = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          architectureDoc = pkgs.writeTextDir "share/doc/fluxon-nix/ARCHITECTURE.md" (
            ''
              # Fluxon Nix

              This repository snapshot does not ship the internal architecture note.
            ''
          );
        in
        {
          default = architectureDoc;
          architecture_doc = architectureDoc;
        }
      );
    };
}
