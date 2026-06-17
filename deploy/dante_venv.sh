#!/usr/bin/env bash
# Dante 팩토리 — 가상환경 경로 단일 정의 (venv 우선, .venv 레거시 폴백)
# shellcheck disable=SC2034
# Usage: source deploy/dante_venv.sh; dante_resolve_python "$INSTALL_ROOT"
set -eu -o pipefail

dante_resolve_python() {
  local root="${1:?INSTALL_ROOT required}"
  if [[ -x "${root}/venv/bin/python" ]]; then
    echo "${root}/venv/bin/python"
  elif [[ -x "${root}/.venv/bin/python" ]]; then
    echo "${root}/.venv/bin/python"
  else
    return 1
  fi
}

dante_resolve_streamlit() {
  local root="${1:?INSTALL_ROOT required}"
  if [[ -x "${root}/venv/bin/streamlit" ]]; then
    echo "${root}/venv/bin/streamlit"
  elif [[ -x "${root}/.venv/bin/streamlit" ]]; then
    echo "${root}/.venv/bin/streamlit"
  else
    return 1
  fi
}
