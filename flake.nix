# SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
#
# SPDX-License-Identifier: GPL-3.0-only

{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    pre-commit.url = "github:cachix/git-hooks.nix";
    pre-commit.inputs.nixpkgs.follows = "nixpkgs";
    advisory-db.url = "github:rustsec/advisory-db";
    advisory-db.flake = false;
    jailed-claude.url = "github:TimQuelch/jailed-claude";
    jailed-claude.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      flake-utils,
      crane,
      advisory-db,
      pre-commit,
      jailed-claude,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
          overlays = [
            jailed-claude.overlays.default
            (final: prev: { claude-code = jailed-claude.inputs.llm-agents.packages.${system}.claude-code; })
          ];
        };
        craneLib = crane.mkLib pkgs;
        src = craneLib.cleanCargoSource ./.;

        commonArgs = {
          inherit src;
          strictDeps = true;
          nativeBuildInputs = [ pkgs.pkg-config ];
          buildInputs = [ pkgs.duckdb ];
        };

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        aws-config-dump = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
            nativeBuildInputs = commonArgs.nativeBuildInputs ++ [
              pkgs.installShellFiles
              pkgs.makeBinaryWrapper
            ];
            postInstall = ''
              wrapProgram $out/bin/aws-config-dump \
                  --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.duckdb ]}

              installShellCompletion \
                  --cmd aws-config-dump \
                  --bash <(PATH=$out/bin COMPLETE=bash aws-config-dump) \
                  --zsh <(PATH=$out/bin COMPLETE=zsh aws-config-dump) \
                  --fish <(PATH=$out/bin COMPLETE=fish aws-config-dump)

              zsh_completion=$(find $out/share/zsh -type f)

              cat <<EOF >> $zsh_completion
              if [ "\$funcstack[1]" = "_aws-config-dump" ]; then
                  _clap_dynamic_completer_aws_config_dump
              fi
              EOF
            '';
          }
        );

        preCommit = pre-commit.lib.${system}.run {
          # Need to provide src if precommit is used as a check, however because it is only used for
          # devShell and git hooks we don't need to include it. Using an empty src inhibits
          # rebuilding the dev shell whenever any file changes
          src = builtins.emptyFile;
          hooks = {
            nixfmt.enable = true;
            rustfmt.enable = true;
            taplo.enable = true;
            clippy.enable = true;
            clippy.settings.extraArgs = "--fix --allow-dirty";
          };
        };
      in
      {
        packages.default = aws-config-dump;
        devShells.default =
          (craneLib.devShell {
            inherit (preCommit) shellHook;
            checks = self.checks.${system};
            packages =
              with pkgs;
              [
                bacon
                gcc
              ]
              ++ preCommit.enabledPackages;
          }).overrideAttrs
            (prevAttrs: {
              nativeBuildInputs = prevAttrs.nativeBuildInputs ++ [
                (jailed-claude.lib.makeJailedClaude {
                  inherit pkgs;
                  extraPkgs = prevAttrs.nativeBuildInputs;
                })
              ];
            });

        checks = {
          inherit aws-config-dump;
        }
        // nixpkgs.lib.mapAttrs' (k: v: nixpkgs.lib.nameValuePair "aws-config-dump-${k}" v) {
          clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets -- --deny warnings";
            }
          );

          test = craneLib.cargoNextest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoNextestExtraArgs = "--no-tests=warn";
            }
          );

          fmt = craneLib.cargoFmt { inherit src; };
          toml-fmt = craneLib.taploFmt (
            commonArgs // { src = pkgs.lib.sources.sourceFilesBySuffices src [ ".toml" ]; }
          );
          audit = craneLib.cargoAudit { inherit src advisory-db; };
          deny = craneLib.cargoDeny { inherit src; };
        };
      }
    );
}
