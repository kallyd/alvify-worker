#!/usr/bin/env bash
# =============================================================================
#  Alvify Worker — Interactive Setup Wizard
#  Run after install.sh to configure credentials and verify connectivity.
#
#  Usage:
#    sudo ./setup.sh
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
DIM='\033[2m'

info()    { echo -e "${CYAN}▸${RESET} $*"; }
success() { echo -e "${GREEN}✔${RESET}  $*"; }
warn()    { echo -e "${YELLOW}⚠${RESET}  $*"; }
die()     { echo -e "\n${RED}✘  $*${RESET}\n" >&2; exit 1; }
hr()      { echo -e "${DIM}────────────────────────────────────────────────${RESET}"; }

[[ $EUID -eq 0 ]] || die "Run as root:  sudo $0"

INSTALL_DIR="/opt/alvify-worker"
ENV_FILE="${INSTALL_DIR}/.env"
SERVICE_NAME="alvify-worker"
VENV="${INSTALL_DIR}/venv"

[[ -f "$ENV_FILE" ]] || die "Worker not installed. Run install.sh first."

# ── Detect public IP ──────────────────────────────────────────────────────────
VPS_IP=$(curl -s --max-time 5 https://api.ipify.org 2>/dev/null \
      || curl -s --max-time 5 https://ifconfig.me 2>/dev/null \
      || hostname -I | awk '{print $1}')

# ── Read existing config ──────────────────────────────────────────────────────
_read_env() { grep -m1 "^${1}=" "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '"' || echo ""; }
API_URL=$(_read_env API_URL); API_URL="${API_URL:-https://workers.alvify.com.br}"
CUR_WORKER_ID=$(_read_env WORKER_ID)
CUR_API_KEY=$(_read_env WORKER_API_KEY)
HEALTH_PORT=$(_read_env HEALTH_PORT); HEALTH_PORT="${HEALTH_PORT:-8001}"

clear
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║        Alvify Worker — Configuração Inicial       ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════╝${RESET}"
echo ""
echo -e "  ${BOLD}IP deste servidor:${RESET}  ${CYAN}${VPS_IP}${RESET}"
echo -e "  ${BOLD}Porta de saúde  :${RESET}  ${CYAN}:${HEALTH_PORT}${RESET}"
echo -e "  ${BOLD}API URL         :${RESET}  ${CYAN}${API_URL}${RESET}"
echo ""
hr
echo ""
echo -e "${BOLD}Como criar este worker no painel Alvify:${RESET}"
echo ""
echo -e "  ${YELLOW}1.${RESET} Acesse  ${CYAN}https://alvify.com.br${RESET}  → Admin → Workers"
echo -e "  ${YELLOW}2.${RESET} Clique em ${BOLD}\"+ Novo Worker\"${RESET}"
echo -e "  ${YELLOW}3.${RESET} Preencha os campos:"
echo -e "       ${DIM}Nome  :${RESET} qualquer nome (ex: vps-01)"
echo -e "       ${DIM}Host  :${RESET} ${CYAN}${VPS_IP}${RESET}"
echo -e "       ${DIM}Porta :${RESET} ${CYAN}${HEALTH_PORT}${RESET}"
echo -e "  ${YELLOW}4.${RESET} Clique em ${BOLD}Criar${RESET} — copie o ${BOLD}Worker ID${RESET} e a ${BOLD}API Key${RESET} gerados"
echo -e "       ${RED}(a API Key é mostrada uma única vez)${RESET}"
echo ""
hr
echo ""

# ── Prompt for credentials ────────────────────────────────────────────────────
if [[ -n "$CUR_WORKER_ID" && -n "$CUR_API_KEY" ]]; then
  echo -e "${GREEN}Credenciais já configuradas.${RESET}"
  echo -e "  Worker ID : ${CYAN}${CUR_WORKER_ID}${RESET}"
  echo -e "  API Key   : ${CYAN}${CUR_API_KEY:0:8}…${RESET}"
  echo ""
  read -rp "$(echo -e "${YELLOW}Deseja atualizar as credenciais? (s/N):${RESET} ")" UPDATE_CREDS
  UPDATE_CREDS="${UPDATE_CREDS,,}"
  if [[ "$UPDATE_CREDS" != "s" && "$UPDATE_CREDS" != "sim" && "$UPDATE_CREDS" != "y" && "$UPDATE_CREDS" != "yes" ]]; then
    NEW_WORKER_ID="$CUR_WORKER_ID"
    NEW_API_KEY="$CUR_API_KEY"
    echo ""
    info "Mantendo credenciais existentes."
  else
    NEW_WORKER_ID=""
    NEW_API_KEY=""
  fi
else
  NEW_WORKER_ID=""
  NEW_API_KEY=""
fi

if [[ -z "$NEW_WORKER_ID" || -z "$NEW_API_KEY" ]]; then
  echo ""
  while [[ -z "$NEW_WORKER_ID" ]]; do
    read -rp "$(echo -e "  ${BOLD}Worker ID${RESET} (UUID): ")" NEW_WORKER_ID
    NEW_WORKER_ID="${NEW_WORKER_ID// /}"
    if [[ ! "$NEW_WORKER_ID" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]; then
      warn "Formato inválido — deve ser um UUID (ex: a1b2c3d4-e5f6-…)"
      NEW_WORKER_ID=""
    fi
  done

  while [[ -z "$NEW_API_KEY" ]]; do
    read -rp "$(echo -e "  ${BOLD}API Key${RESET}          : ")" NEW_API_KEY
    NEW_API_KEY="${NEW_API_KEY// /}"
    [[ ${#NEW_API_KEY} -lt 10 ]] && { warn "API Key muito curta."; NEW_API_KEY=""; }
  done
fi

# ── Write to .env ─────────────────────────────────────────────────────────────
echo ""
info "Salvando credenciais em ${ENV_FILE}…"

_set_env() {
  local key="$1" val="$2"
  if grep -q "^${key}=" "$ENV_FILE" 2>/dev/null; then
    sed -i "s|^${key}=.*|${key}=${val}|" "$ENV_FILE"
  else
    echo "${key}=${val}" >> "$ENV_FILE"
  fi
}

_set_env "WORKER_ID"      "$NEW_WORKER_ID"
_set_env "WORKER_API_KEY" "$NEW_API_KEY"
chmod 640 "$ENV_FILE"
success "Credenciais salvas"

# ── Connectivity test ─────────────────────────────────────────────────────────
echo ""
info "Testando conexão com ${API_URL}…"

# curl bypasses Cloudflare UA checks that block Python urllib
HTTP_CODE=$(curl -s -o /tmp/_alvify_test.json -w "%{http_code}" \
  --max-time 10 \
  -X POST "${API_URL}/internal/workers/heartbeat" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${NEW_API_KEY}" \
  -H "X-Worker-ID: ${NEW_WORKER_ID}" \
  -H "User-Agent: alvify-worker/1.0" \
  -d '{"cpu_usage":0,"ram_usage":0,"active_jobs":0,"version":"setup"}' 2>/dev/null || echo "000")
RESP_BODY=$(cat /tmp/_alvify_test.json 2>/dev/null || echo "")

TEST_RESULT=""
case "$HTTP_CODE" in
  200|204) TEST_RESULT="OK:${HTTP_CODE}" ;;
  000)     TEST_RESULT="FAIL:Não foi possível conectar (timeout ou DNS)" ;;
  *)       TEST_RESULT="ERR:${HTTP_CODE}:${RESP_BODY:0:200}" ;;
esac

echo ""
# Detect Cloudflare proxy blocking (error 1010 = bot fingerprint ban)
_is_cloudflare_block() {
  echo "$RESP_BODY" | grep -qi "cloudflare\|error code: 1010\|1010"
}

if [[ "$TEST_RESULT" == OK:* ]]; then
  SUCCESS_CODE="${TEST_RESULT#OK:}"
  echo -e "${GREEN}${BOLD}  ✔ Conexão bem-sucedida! (HTTP ${SUCCESS_CODE})${RESET}"
  echo -e "  Worker autenticado com a API Alvify."
  CONN_OK=true
elif [[ "$TEST_RESULT" == ERR:401:* ]]; then
  echo -e "${RED}${BOLD}  ✘ Autenticação falhou (HTTP 401)${RESET}"
  echo -e "  A API Key está incorreta ou foi revogada."
  CONN_OK=false
elif [[ "$TEST_RESULT" == ERR:404:* ]]; then
  echo -e "${RED}${BOLD}  ✘ Worker não encontrado (HTTP 404)${RESET}"
  echo -e "  Verifique se o Worker ID corresponde ao worker criado no painel."
  CONN_OK=false
elif [[ "$TEST_RESULT" == ERR:403:* ]] && _is_cloudflare_block; then
  echo -e "${YELLOW}${BOLD}  ⚠ Teste bloqueado pelo proxy Cloudflare (erro 1010)${RESET}"
  echo -e "  Não foi possível verificar as credenciais automaticamente."
  echo -e "  O serviço será iniciado assim mesmo — se a API Key estiver errada"
  echo -e "  os logs mostrarão erro de autenticação (journalctl -u ${SERVICE_NAME} -f)."
  CONN_OK=true   # proceed — service will log auth errors on its own if key is wrong
elif [[ "$TEST_RESULT" == ERR:403:* ]]; then
  echo -e "${RED}${BOLD}  ✘ Acesso negado (HTTP 403)${RESET}"
  echo -e "  Verifique se o Worker ID e a API Key estão corretos."
  echo -e "  ${DIM}Resposta: ${RESP_BODY:0:120}${RESET}"
  CONN_OK=false
else
  echo -e "${YELLOW}${BOLD}  ⚠ Não foi possível conectar à API${RESET}"
  echo -e "  O serviço será iniciado assim mesmo — verifique os logs se houver problemas."
  echo -e "  ${DIM}Detalhe: ${TEST_RESULT}${RESET}"
  CONN_OK=true   # network issues should not block installation
fi

echo ""
hr

# ── Start / restart service ───────────────────────────────────────────────────
if [[ "$CONN_OK" == true ]]; then
  echo ""
  if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
    info "Reiniciando serviço ${SERVICE_NAME}…"
    systemctl restart "$SERVICE_NAME"
  else
    info "Iniciando serviço ${SERVICE_NAME}…"
    systemctl start "$SERVICE_NAME"
  fi
  sleep 2
  if systemctl is-active --quiet "$SERVICE_NAME"; then
    success "Serviço ${SERVICE_NAME} está rodando!"
  else
    warn "Serviço não iniciou — verifique: journalctl -u ${SERVICE_NAME} -n 30"
  fi
else
  echo ""
  warn "Serviço não iniciado. Corrija as credenciais e rode novamente:"
  echo -e "    ${CYAN}sudo ${INSTALL_DIR}/setup.sh${RESET}"
fi

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║               Configuração concluída!             ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════╝${RESET}"
echo ""
echo -e "  ${BOLD}Arquivo de config :${RESET} ${CYAN}${ENV_FILE}${RESET}"
echo -e "  ${BOLD}Ver logs em tempo :${RESET} ${CYAN}sudo journalctl -u ${SERVICE_NAME} -f${RESET}"
echo -e "  ${BOLD}Status do serviço :${RESET} ${CYAN}sudo systemctl status ${SERVICE_NAME}${RESET}"
echo ""
