#!/bin/bash
set -e

case $1 in
  setup)
    rustup component add rustfmt --toolchain nightly
    rustup component add clippy --toolchain nightly
    pre-commit install
    echo "Ready to go..."
    ;;

  check)
    pre-commit run --all-files
    ;;

  *)
    echo "invalid argument, setup/check/release expected"
    exit 1
    ;;
esac
