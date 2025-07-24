# Estágio de compilação - Ambiente Go otimizado para construção
FROM golang:1.23.4-alpine3.20 AS build-stage

# Diretório de trabalho para compilação
WORKDIR /build

# Instalar dependências essenciais para compilação e upx para compressão
RUN apk add --no-cache git ca-certificates upx

# Copiar arquivos de dependências primeiro (para cache do Docker)
COPY go.mod go.sum ./

# Baixar dependências Go
RUN go mod download && go mod verify

# Copiar código fonte completo
COPY . .

# Construir aplicação com otimizações máximas de produção
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -trimpath \
    -a -installsuffix cgo \
    -o rinha-backend main.go && \
    # Comprimir binário para reduzir tamanho da imagem
    upx --best --lzma rinha-backend

# Estágio final - Imagem mínima de produção  
FROM alpine:3.20 AS production

# Instalar apenas o mínimo necessário
RUN apk --no-cache add ca-certificates && \
    update-ca-certificates

# Criar usuário não-root para segurança
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# Definir diretório de trabalho da aplicação
WORKDIR /app

# Copiar script de inicialização e binário da aplicação
COPY --chown=appuser:appgroup ./containers/app/docker-entrypoint.sh ./entrypoint.sh
COPY --from=build-stage --chown=appuser:appgroup /build/rinha-backend ./rinha-backend

# Tornar script executável
RUN chmod +x ./entrypoint.sh ./rinha-backend

# Configurar variáveis de ambiente para performance
ENV GOMAXPROCS=2 \
    GOGC=300 \
    GOMEMLIMIT=250MiB \
    PORT=8080 \
    REDIS_URL=redis://redis:6379 \
    DEFAULT_PROCESSOR_URL=http://payment-processor-default:8001 \
    FALLBACK_PROCESSOR_URL=http://payment-processor-fallback:8002

# Mudar para usuário não-root
USER appuser

# Expor porta da aplicação
EXPOSE 8080

# Health check para garantir que a aplicação está rodando
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Comando de inicialização da aplicação
ENTRYPOINT ["./entrypoint.sh"]