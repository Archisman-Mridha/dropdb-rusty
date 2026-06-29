{
  inputs = {
    nixpkgs.url = "github:cachix/devenv-nixpkgs/rolling";
    systems.url = "github:nix-systems/default";
    devenv = {
      url = "github:cachix/devenv";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs =
    {
      self,
      nixpkgs,
      devenv,
      systems,
      ...
    }@inputs:
    let
      forEachSystem = nixpkgs.lib.genAttrs (import systems);
    in
    {
      devShells = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = devenv.lib.mkShell {
            inherit inputs pkgs;
            modules = [
              {
                # Most packages come pre-built with binaries provided by the official Nix binary
                # cache.
                # If you're modifying a package or using a package that's not built upstream, Nix
                # will build it from source instead of downloading a binary.
                # To prevent packages from being built more than once, devenv provides seamless
                # integration with binary caches hosted by Cachix.
                cachix.enable = true;

                languages.rust = {
                  enable = true;
                  toolchainFile = ./rust-toolchain.toml;

                  lsp.enable = false;
                };

                packages = with pkgs; [
                  protobuf
                ];
              }
            ];
          };
        }
      );
    };
}
