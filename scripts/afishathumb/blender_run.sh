#!/usr/bin/env bash
# Run a Python script inside the locally-installed Blender 4.5 under a
# headless Xvfb display, with the extracted system libs visible to the
# dynamic linker. All arguments after the script path are forwarded to
# Blender. Forward script-level args to the Python script using `--`.
set -euo pipefail

BLENDER_HOME="${BLENDER_HOME:-/home/dev/.local/opt/blender-4.5.0-linux-x64}"
BLENDER_LIBS="${BLENDER_LIBS:-/home/dev/.local/blender-runtime-libs/usr/lib/x86_64-linux-gnu}"
BLENDER_BIN_EXTRA="${BLENDER_BIN_EXTRA:-/home/dev/.local/blender-runtime-libs/usr/bin}"

if [[ ! -x "${BLENDER_HOME}/blender" ]]; then
  echo "Blender not found at ${BLENDER_HOME}" >&2
  exit 1
fi

export LD_LIBRARY_PATH="${BLENDER_LIBS}:${LD_LIBRARY_PATH:-}"
export PATH="${BLENDER_BIN_EXTRA}:${PATH}"
export LIBGL_DRIVERS_PATH="${BLENDER_LIBS}/dri"
# Force the Mesa software rasterizer — EGL on this headless host is broken
# without a real GPU vendor driver, but `LIBGL_ALWAYS_SOFTWARE=1` over
# Xvfb's GLX is reliable.
export __GLX_VENDOR_LIBRARY_NAME=mesa
export LIBGL_ALWAYS_SOFTWARE=1
# Blender 4.x still needs a working GL context even in --background mode;
# run under a virtual framebuffer so EGL/GLX init succeeds on headless hosts.
exec xvfb-run -a -s "-screen 0 1920x1080x24" \
  "${BLENDER_HOME}/blender" --background --factory-startup \
  --gpu-backend opengl --python "$@"
