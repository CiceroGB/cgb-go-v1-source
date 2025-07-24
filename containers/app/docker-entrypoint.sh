#!/bin/sh

# Rinha v1 - Script de inicialização
set -e

# Configurações de ambiente para máxima performance
export GOMAXPROCS=${GOMAXPROCS:-2}
export GOGC=${GOGC:-300}
export GOMEMLIMIT=${GOMEMLIMIT:-250MiB}

# Log de inicialização
echo "Rinha v1 - Performance Backend"
echo "Performance Target: p99 < 11ms"
echo "GOMAXPROCS: $GOMAXPROCS"
echo "GOGC: $GOGC"
echo "GOMEMLIMIT: $GOMEMLIMIT"

# Executar aplicação
echo "Iniciando Rinha v1 API..."
exec ./app
