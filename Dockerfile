# Estágio de compilação - Ambiente Go otimizado para construção
FROM golang:1.24.5-alpine3.22 AS build-stage

# Diretório de trabalho para compilação
WORKDIR /build

# Instalar dependências essenciais para compilação
RUN apk add --no-cache git ca-certificates

# Copiar arquivos de dependências primeiro (para cache do Docker)
COPY go.mod go.sum ./

# Baixar dependências Go
RUN go mod download && go mod verify

# Copiar código fonte completo
COPY . .

# Construir aplicação com otimizações de produção
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o rinha-backend main.go

# Estágio final - Imagem mínima de produção  
FROM alpine:3.22 AS production

# Instalar certificados SSL e timezone data
RUN apk --no-cache add ca-certificates tzdata && \
    update-ca-certificates

# Criar usuário não-root para segurança
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# Definir diretório de trabalho da aplicação
WORKDIR /home/appuser

# Copiar script de inicialização e binário da aplicação
COPY --chown=appuser:appgroup ./containers/app/docker-entrypoint.sh ./entrypoint.sh
COPY --from=build-stage --chown=appuser:appgroup /build/rinha-backend ./app

# Tornar script executável
RUN chmod +x ./entrypoint.sh ./app

# Mudar para usuário não-root
USER appuser

# Expor porta da aplicação
EXPOSE 8080

# Comando de inicialização da aplicação
ENTRYPOINT ["./entrypoint.sh"]