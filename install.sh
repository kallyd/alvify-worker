#!/usr/bin/env bash
# =============================================================================
#  Alvify Worker — VPS Installation Script
#  Compatible with: Ubuntu 20.04 / 22.04 / 24.04, Debian 11 / 12
#
#  Usage:
#    git clone https://github.com/kallyd/alvify-worker.git
#    cd alvify-worker
#    chmod +x install.sh
#    sudo ./install.sh
#
#  After installation, setup.sh runs automatically to configure credentials.
#  To re-run setup at any time:
#    sudo /opt/alvify-worker/setup.sh
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET} $*"; }
success() { echo -e "${GREEN}[OK]${RESET}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET} $*"; }
die()     { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "Run this script as root:  sudo $0"

INSTALL_DIR="/opt/alvify-worker"
SERVICE_USER="alvify"
SERVICE_NAME="alvify-worker"
PYTHON_MIN="3.11"
WORKER_SRC="$(cd "$(dirname "$0")" && pwd)"

echo -e "\n${BOLD}════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Alvify Worker — Installer${RESET}"
echo -e "${BOLD}════════════════════════════════════════════${RESET}\n"

# 1. OS check
info "Detecting OS..."
. /etc/os-release 2>/dev/null || true
OS_ID="${ID:-unknown}"
info "OS: ${PRETTY_NAME:-$OS_ID}"

case "$OS_ID" in
  ubuntu|debian|linuxmint|pop) PKG_MGR="apt" ;;
  centos|rhel|almalinux|rocky) PKG_MGR="yum" ;;
  fedora)                      PKG_MGR="dnf" ;;
  *) warn "Untested OS '$OS_ID' — will try apt" ; PKG_MGR="apt" ;;
esac

# 2. System dependencies
info "Installing system dependencies..."
if [[ "$PKG_MGR" == "apt" ]]; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq \
    curl wget git build-essential software-properties-common \
    libglib2.0-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libdbus-1-3 libxcb1 libxkbcommon0 libx11-6 \
    libxcomposite1 libxdamage1 libxext6 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libcairo2 libasound2 libxshmfence1 \
    fonts-liberation fonts-noto-color-emoji 2>/dev/null || true
else
  yum install -y curl wget git gcc 2>/dev/null || true
fi
success "System dependencies installed"

# 3. Python >= 3.11
info "Checking for Python >= ${PYTHON_MIN}..."

_find_python() {
  for py in python3.13 python3.12 python3.11 python3; do
    if command -v "$py" &>/dev/null; then
      if "$py" -c "import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)" 2>/dev/null; then
        echo "$py"; return 0
      fi
    fi
  done
  return 1
}

_install_python_apt() {
  for ver in 3.13 3.12 3.11; do
    if apt-cache show "python${ver}" &>/dev/null 2>&1; then
      info "Installing python${ver} from system repo..."
      apt-get install -y -qq "python${ver}" "python${ver}-venv" "python${ver}-dev"
      echo "python${ver}"; return 0
    fi
  done
  info "Adding deadsnakes PPA for Python 3.12..."
  apt-get install -y -qq software-properties-common
  add-apt-repository -y ppa:deadsnakes/ppa
  apt-get update -qq
  apt-get install -y -qq python3.12 python3.12-venv python3.12-dev
  echo "python3.12"; return 0
}

_install_python_yum() {
  local PKG_CMD="$1"
  for ver in 312 311; do
    if "$PKG_CMD" info "python${ver}" &>/dev/null 2>&1; then
      info "Installing python${ver} via ${PKG_CMD}..."
      "$PKG_CMD" install -y "python${ver}" "python${ver}-pip"
      echo "python$(echo "$ver" | sed 's/\(.\)/\1./')"; return 0
    fi
  done
  info "Compiling Python 3.12 from source..."
  yum groupinstall -y "Development Tools" 2>/dev/null || \
    "$PKG_CMD" install -y gcc gcc-c++ make openssl-devel bzip2-devel libffi-devel zlib-devel
  curl -sSf "https://www.python.org/ftp/python/3.12.9/Python-3.12.9.tgz" | tar -xz -C /tmp
  pushd /tmp/Python-3.12.9 > /dev/null
  ./configure --enable-optimizations --prefix=/usr/local -q
  make -j"$(nproc)" altinstall 2>&1 | tail -3
  popd > /dev/null
  rm -rf /tmp/Python-3.12.9
  echo "python3.12"; return 0
}

PYTHON_BIN=$(_find_python || true)
if [[ -z "$PYTHON_BIN" ]]; then
  warn "Python >= 3.11 not found — installing automatically..."
  case "$PKG_MGR" in
    apt) PYTHON_BIN=$(_install_python_apt) ;;
    dnf) PYTHON_BIN=$(_install_python_yum dnf) ;;
    yum) PYTHON_BIN=$(_install_python_yum yum) ;;
    *)   die "Cannot auto-install Python on this OS. Install Python >= 3.11 manually and re-run." ;;
  esac
  PYTHON_BIN=$(_find_python) || die "Python installation succeeded but binary not found in PATH."
fi
success "Python: $($PYTHON_BIN --version)"

# 4. Create system user
if ! id "$SERVICE_USER" &>/dev/null; then
  info "Creating system user '${SERVICE_USER}'..."
  useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
  success "User created"
else
  info "User '${SERVICE_USER}' already exists — skipping"
fi

# 5. Create install directory
info "Setting up install directory: ${INSTALL_DIR}"
mkdir -p "$INSTALL_DIR"

# 6. Copy worker source files
info "Copying worker source files..."
[[ -f "${WORKER_SRC}/main.py" ]]          || die "main.py not found in ${WORKER_SRC}"
[[ -f "${WORKER_SRC}/requirements.txt" ]] || die "requirements.txt not found in ${WORKER_SRC}"
[[ -d "${WORKER_SRC}/core" ]]             || die "core/ directory not found in ${WORKER_SRC}"

cp "${WORKER_SRC}/main.py"          "${INSTALL_DIR}/main.py"
cp "${WORKER_SRC}/requirements.txt" "${INSTALL_DIR}/requirements.txt"
rm -rf "${INSTALL_DIR}/core"
cp -r "${WORKER_SRC}/core" "${INSTALL_DIR}/core"
cp "${WORKER_SRC}/setup.sh" "${INSTALL_DIR}/setup.sh"
chmod +x "${INSTALL_DIR}/setup.sh"
success "Source files copied"

# 7. Python virtual environment
if [[ "$PKG_MGR" == "apt" ]]; then
  PY_VER=$("$PYTHON_BIN" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
  apt-get install -y -qq "python${PY_VER}-venv" "python${PY_VER}-dev" 2>/dev/null || true
fi

VENV_DIR="${INSTALL_DIR}/venv"
if [[ ! -d "$VENV_DIR" ]]; then
  info "Creating virtual environment..."
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

PIP="${VENV_DIR}/bin/pip"
if [[ ! -f "$PIP" ]]; then
  info "Bootstrapping pip..."
  curl -sSf https://bootstrap.pypa.io/get-pip.py | "${VENV_DIR}/bin/python" - --quiet
fi

info "Upgrading pip..."
"$PIP" install --quiet --upgrade pip
info "Installing Python dependencies..."
"$PIP" install --quiet -r "${INSTALL_DIR}/requirements.txt"
success "Python dependencies installed"

# 8. Playwright + Chromium
info "Installing Playwright and Chromium browser..."
PLAYWRIGHT_BROWSERS_PATH="/opt/alvify-worker/.playwright"
export PLAYWRIGHT_BROWSERS_PATH
"$PIP" install --quiet playwright
"${VENV_DIR}/bin/playwright" install chromium --with-deps 2>&1 | tail -5
success "Chromium installed"

# 9. Environment file
ENV_FILE="${INSTALL_DIR}/.env"
if [[ ! -f "$ENV_FILE" ]]; then
  info "Creating .env from template..."
  if [[ -f "${WORKER_SRC}/.env.example" ]]; then
    cp "${WORKER_SRC}/.env.example" "$ENV_FILE"
  else
    cat > "$ENV_FILE" <<'ENVEOF'
# Alvify Worker configuration
API_URL=https://workers.alvify.com.br
WORKER_ID=
WORKER_API_KEY=
MAX_CONCURRENCY=2
HEALTH_PORT=8001
VERSION=1.0.0
LOG_LEVEL=INFO
ENVEOF
  fi
  success ".env created at ${ENV_FILE}"
else
  info ".env already exists — not overwriting"
fi

# 10. Permissions
chown -R "${SERVICE_USER}:${SERVICE_USER}" "$INSTALL_DIR"
chmod 640 "$ENV_FILE"
success "Permissions set"

# 11. systemd service
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
info "Installing systemd service: ${SERVICE_NAME}..."

cat > "$SERVICE_FILE" <<SERVICEEOF
[Unit]
Description=Alvify Remote Scraping Worker
Documentation=https://alvify.com.br
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${ENV_FILE}
Environment=PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_BROWSERS_PATH}
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH=${INSTALL_DIR}
ExecStart=${VENV_DIR}/bin/python ${INSTALL_DIR}/main.py
Restart=on-failure
RestartSec=10s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}
LimitNOFILE=65536
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
SERVICEEOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
success "systemd service installed and enabled"

# 12. Log rotation
cat > "/etc/logrotate.d/${SERVICE_NAME}" <<LOGEOF
/var/log/${SERVICE_NAME}.log {
    daily
    rotate 7
    compress
    missingok
    notifempty
    copytruncate
}
LOGEOF

# 13. Summary
echo ""
echo -e "${BOLD}════════════════════════════════════════════${RESET}"
echo -e "${GREEN}${BOLD}  Instalação concluída!${RESET}"
echo -e "${BOLD}════════════════════════════════════════════${RESET}"
echo ""
echo -e "  Diretório : ${CYAN}${INSTALL_DIR}${RESET}"
echo -e "  Config    : ${CYAN}${ENV_FILE}${RESET}"
echo -e "  Serviço   : ${CYAN}${SERVICE_NAME}${RESET}"
echo ""

# 14. Launch interactive setup wizard
echo -e "${BOLD}Iniciando assistente de configuração...${RESET}"
echo ""
exec "${INSTALL_DIR}/setup.sh"
