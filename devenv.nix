{ pkgs, ... }:

{
  # https://devenv.sh/basics/
  # env.GREET = "devenv";
  env.RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
  # https://devenv.sh/packages/
  packages = [ pkgs.git pkgs.redis pkgs.cargo-watch ];

  # enterShell = ''
    # hello
    # git --version
  # '';

  # https://devenv.sh/languages/
  languages.nix.enable = true;
  languages.rust.enable = true;
  # languages.rust.version = "stable";

  # https://devenv.sh/scripts/
  # scripts.hello.exec = "echo hello from $GREET";

  # https://devenv.sh/pre-commit-hooks/
  pre-commit.hooks.shellcheck.enable = true;
  pre-commit.hooks.rustfmt.enable = true;
  pre-commit.hooks.clippy.enable = true;
  pre-commit.hooks.cargo-check.enable = true;

  # https://devenv.sh/processes/
  processes.server.exec = "cargo watch -x 'run'";
}
