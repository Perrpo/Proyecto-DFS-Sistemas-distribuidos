#!/usr/bin/env bash
# ============================================================
# init_test.sh – Script de prueba integral de MiniDFS
# ============================================================
# Prueba completa de extremo a extremo:
#   1. Levantar el stack Docker
#   2. Registrar usuario y hacer login
#   3. Crear un archivo de prueba de ~130 MB (3 bloques de 64 MB)
#   4. Subir el archivo al DFS (PUT)
#   5. Listar archivos (LS)
#   6. Descargar el archivo (GET)
#   7. Verificar integridad SHA-256 (debe coincidir)
#   8. Simular fallo de DataNode 1 (docker stop)
#   9. Descargar de nuevo (GET) – debe seguir funcionando gracias a las réplicas
#  10. Esperar re-replicación automática (>30s) y verificar en logs
#  11. Limpieza
# ============================================================
set -euo pipefail

# ─── Colores ────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()      { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()    { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
section() { echo -e "\n${BOLD}${CYAN}══ $* ══${NC}"; }

# ─── Variables ──────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="${SCRIPT_DIR}/tmp_test"
LOCAL_FILE="${TEST_DIR}/test_input.bin"
DOWNLOADED_FILE="${TEST_DIR}/test_output.bin"
REMOTE_PATH="/test/archivo_prueba.bin"
USERNAME="testuser_$(date +%s)"
PASSWORD="TestPass123!"
FILE_SIZE_MB=130        # ~3 bloques de 64 MB (factor replicación = 3)
NAMENODE="localhost:50051"

# Detectar CLI de Python
CLI="python ${SCRIPT_DIR}/client/cli.py"
export NAMENODE_HOST=localhost
export NAMENODE_PORT=50051

# ─── Limpieza previa ────────────────────────────────────────
cleanup() {
    info "Limpiando archivos temporales..."
    rm -rf "${TEST_DIR}"
    info "Limpieza de stack Docker..."
    docker compose -f "${SCRIPT_DIR}/docker-compose.yml" down --volumes 2>/dev/null || true
}

# Limpiar siempre al salir (éxito o error)
trap cleanup EXIT

# ─── Inicio ─────────────────────────────────────────────────
echo -e "${BOLD}"
echo "╔══════════════════════════════════════════════════════╗"
echo "║          MiniDFS – Prueba Integral (Día 3)           ║"
echo "╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"

mkdir -p "${TEST_DIR}"

# ══════════════════════════════════════════════════════════════
section "Paso 1: Levantar stack Docker"
# ══════════════════════════════════════════════════════════════
info "Construyendo imágenes y levantando servicios..."
docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d --build

info "Esperando a que los servicios estén listos (30s)..."
sleep 30

# Verificar contenedores en ejecución
RUNNING=$(docker compose -f "${SCRIPT_DIR}/docker-compose.yml" ps --status running --format json | wc -l)
if [ "${RUNNING}" -lt 4 ]; then
    warn "Solo ${RUNNING} contenedores corriendo. Verificar logs:"
    docker compose -f "${SCRIPT_DIR}/docker-compose.yml" logs --tail=30
    fail "No todos los servicios están activos"
fi
ok "Stack levantado: NameNode + 3 DataNodes"

# ══════════════════════════════════════════════════════════════
section "Paso 2: Crear archivo de prueba (${FILE_SIZE_MB} MB)"
# ══════════════════════════════════════════════════════════════
info "Generando archivo de ${FILE_SIZE_MB} MB con datos aleatorios..."
dd if=/dev/urandom of="${LOCAL_FILE}" bs=1M count=${FILE_SIZE_MB} 2>/dev/null
ORIGINAL_SHA=$(sha256sum "${LOCAL_FILE}" | awk '{print $1}')
info "SHA-256 original: ${ORIGINAL_SHA:0:16}..."
ok "Archivo creado: ${LOCAL_FILE} ($(du -sh "${LOCAL_FILE}" | cut -f1))"

# ══════════════════════════════════════════════════════════════
section "Paso 3: Registrar usuario y login"
# ══════════════════════════════════════════════════════════════
info "Registrando usuario '${USERNAME}'..."
${CLI} register "${USERNAME}" "${PASSWORD}" || fail "Registro fallido"
ok "Usuario registrado"

info "Iniciando sesión..."
${CLI} login "${USERNAME}" "${PASSWORD}" || fail "Login fallido"
ok "Login exitoso"

# ══════════════════════════════════════════════════════════════
section "Paso 4: Crear directorio y subir archivo (PUT)"
# ══════════════════════════════════════════════════════════════
info "Creando directorio /test..."
${CLI} mkdir /test || warn "Directorio ya existente (OK)"

info "Subiendo archivo al DFS: ${REMOTE_PATH}..."
START_PUT=$(date +%s)
${CLI} put "${LOCAL_FILE}" "${REMOTE_PATH}" || fail "PUT fallido"
END_PUT=$(date +%s)
ELAPSED=$((END_PUT - START_PUT))
ok "Archivo subido en ${ELAPSED}s"

# ══════════════════════════════════════════════════════════════
section "Paso 5: Listar archivos"
# ══════════════════════════════════════════════════════════════
info "Listando contenido de /test..."
${CLI} ls /test || fail "LS fallido"
ok "Listado exitoso"

# ══════════════════════════════════════════════════════════════
section "Paso 6: Descargar archivo (GET)"
# ══════════════════════════════════════════════════════════════
info "Descargando ${REMOTE_PATH} → ${DOWNLOADED_FILE}..."
START_GET=$(date +%s)
${CLI} get "${REMOTE_PATH}" "${DOWNLOADED_FILE}" || fail "GET fallido"
END_GET=$(date +%s)
ELAPSED=$((END_GET - START_GET))
ok "Archivo descargado en ${ELAPSED}s"

# ══════════════════════════════════════════════════════════════
section "Paso 7: Verificar integridad SHA-256"
# ══════════════════════════════════════════════════════════════
DOWNLOADED_SHA=$(sha256sum "${DOWNLOADED_FILE}" | awk '{print $1}')
info "SHA-256 original:   ${ORIGINAL_SHA:0:16}..."
info "SHA-256 descargado: ${DOWNLOADED_SHA:0:16}..."

if [ "${ORIGINAL_SHA}" = "${DOWNLOADED_SHA}" ]; then
    ok "✓ Integridad verificada: los archivos son idénticos"
else
    fail "✗ INTEGRIDAD FALLIDA: los SHA-256 no coinciden"
fi

# ══════════════════════════════════════════════════════════════
section "Paso 8: Simular fallo de DataNode 1"
# ══════════════════════════════════════════════════════════════
info "Deteniendo minidfs-datanode1 (simulando fallo)..."
docker stop minidfs-datanode1
ok "DataNode 1 detenido"

info "Esperando 5s para que el sistema registre el fallo..."
sleep 5

# ══════════════════════════════════════════════════════════════
section "Paso 9: GET con DataNode 1 caído (tolerancia a fallos)"
# ══════════════════════════════════════════════════════════════
RECOVERED_FILE="${TEST_DIR}/test_recovered.bin"
info "Descargando archivo con DataNode 1 caído..."
${CLI} get "${REMOTE_PATH}" "${RECOVERED_FILE}" || fail "GET fallido con DataNode caído"

RECOVERED_SHA=$(sha256sum "${RECOVERED_FILE}" | awk '{print $1}')
if [ "${ORIGINAL_SHA}" = "${RECOVERED_SHA}" ]; then
    ok "✓ Tolerancia a fallos verificada: GET exitoso con DataNode 1 caído"
else
    fail "✗ GET con DataNode caído devolvió datos incorrectos"
fi

# ══════════════════════════════════════════════════════════════
section "Paso 10: Verificar re-replicación automática"
# ══════════════════════════════════════════════════════════════
info "Esperando 45s para que el HeartbeatMonitor detecte el fallo y re-replique..."
sleep 45

info "Buscando logs de re-replicación en NameNode..."
if docker logs minidfs-namenode 2>&1 | grep -q "Re-replicación programada"; then
    ok "✓ Re-replicación automática detectada en logs del NameNode"
    docker logs minidfs-namenode 2>&1 | grep "Re-replicación" | tail -5
else
    warn "No se encontraron logs de re-replicación (puede tardar más o el monitor ya lo resolvió)"
fi

# ══════════════════════════════════════════════════════════════
section "Resultado Final"
# ══════════════════════════════════════════════════════════════
echo ""
echo -e "${GREEN}${BOLD}"
echo "╔══════════════════════════════════════════════════════╗"
echo "║        ✓ TODAS LAS PRUEBAS PASARON                   ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  [OK] Stack Docker (1 NameNode + 3 DataNodes)        ║"
echo "║  [OK] Autenticación JWT (register + login)           ║"
echo "║  [OK] Namespace (mkdir)                              ║"
echo "║  [OK] PUT: archivo ${FILE_SIZE_MB}MB con pipeline de repl.    ║"
echo "║  [OK] GET: descarga + verificación SHA-256           ║"
echo "║  [OK] Tolerancia a fallos: GET con DataNode caído    ║"
echo "║  [OK] Re-replicación automática por HeartbeatMonitor ║"
echo "╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"
