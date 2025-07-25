package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fasthttp/router"
	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

// ============================================================================
// CONFIGURAÇÕES E VARIÁVEIS GLOBAIS
// ============================================================================

var redisConnection *redis.Client

// JSON otimizado para máxima performance (zero allocations)
var fastJson = jsoniter.ConfigFastest

// URLs dos processadores de pagamento
var defaultProcessorURL = getEnvVar("DEFAULT_PROCESSOR_URL", "http://payment-processor-default:8080")
var fallbackProcessorURL = getEnvVar("FALLBACK_PROCESSOR_URL", "http://payment-processor-fallback:8080")

// Configurações do Redis e fila in-memory
const (
	paymentQueueKey = "payment_queue"
	resultsTable    = "payment_results"
	maxWorkers      = 200
	channelBuffer   = 20000
	
	// Configurações da fila in-memory para otimização de performance
	inMemoryQueueSize = 50000     // Buffer grande para absorver picos
	batchSize         = 100       // Tamanho do batch para flush
	batchTimeout      = 30 * time.Millisecond  // Flush máximo a cada 30ms
)

// Configurações de cache e health check
const (
	processorCacheKey     = "active_processor_cache"
	selectionLockKey      = "processor_selection_lock"
	cacheLifetime         = 10 * time.Second
	selectionLockTimeout  = 3 * time.Second
	
	healthControlKey      = "health_check_control"
	healthLockKey         = "health_manager_lock"
	healthCheckInterval   = 5 * time.Second
	healthLockTimeout     = 5 * time.Second
	
	maxLatencyMs          = 50
	latencyDifferenceMs   = 50
)

// ============================================================================
// UTILITÁRIOS BÁSICOS
// ============================================================================

// getEnvVar - Obtém valor de variável de ambiente ou retorna padrão
func getEnvVar(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// ============================================================================
// ESTRUTURAS DE DADOS PRINCIPAIS
// ============================================================================

// PaymentData - Representa uma solicitação de pagamento recebida
type PaymentData struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

// Validate - Verifica se os dados do pagamento são válidos
func (p *PaymentData) Validate() error {
	if p.CorrelationID == "" {
		return errors.New("correlationId é obrigatório")
	}
	if p.Amount <= 0 {
		return errors.New("valor deve ser maior que zero")
	}
	return nil
}

// HealthStatus - Resposta do endpoint de health check dos processadores
type HealthStatus struct {
	Failing         bool `json:"failing"`
	ResponseTime    int  `json:"minResponseTime"`
}

// ProcessorRequest - Dados enviados para o processador de pagamento
type ProcessorRequest struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

// ============================================================================
// CONEXÃO COM REDIS
// ============================================================================

// connectRedis - Inicializa conexão com Redis e testa conectividade
func connectRedis() {
	redisConnection = redis.NewClient(&redis.Options{
		Addr:     getEnvVar("REDIS_URL", "rinha-redis:6379"),
		Password: "",
		DB:       0,
	})

	if err := redisConnection.Ping(context.Background()).Err(); err != nil {
		panic(fmt.Sprintf("Falha ao conectar com Redis: %v", err))
	}
	
	log.Println("Redis conectado com sucesso")
}

// ============================================================================
// CLIENTES HTTP PARA PROCESSADORES
// ============================================================================

var defaultHttpClient = &fasthttp.Client{
	MaxConnsPerHost:               8192,
	MaxIdleConnDuration:           90 * time.Second,
	ReadTimeout:                   5 * time.Second,
	WriteTimeout:                  5 * time.Second,
	DisableHeaderNamesNormalizing: true,
}

var fallbackHttpClient = &fasthttp.Client{
	MaxConnsPerHost:               8192,
	MaxIdleConnDuration:           90 * time.Second,
	ReadTimeout:                   5 * time.Second,
	WriteTimeout:                  5 * time.Second,
	DisableHeaderNamesNormalizing: true,
}

// sendPaymentDefault - Envia pagamento para o processador padrão
func sendPaymentDefault(req ProcessorRequest) error {
	reqBody, err := fastJson.Marshal(req)
	if err != nil {
		return fmt.Errorf("erro ao serializar request: %w", err)
	}

	request := fasthttp.AcquireRequest()
	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(request)
	defer fasthttp.ReleaseResponse(response)

	request.SetRequestURI(defaultProcessorURL + "/payments")
	request.Header.SetMethod(fasthttp.MethodPost)
	request.Header.SetContentType("application/json")
	request.SetBody(reqBody)

	if err := defaultHttpClient.Do(request, response); err != nil {
		return fmt.Errorf("erro na comunicação HTTP: %w", err)
	}

	if response.StatusCode() != fasthttp.StatusOK {
		return errors.New("processador padrão retornou status inesperado")
	}

	return nil
}

// checkDefaultHealth - Consulta health check do processador padrão
func checkDefaultHealth() (*HealthStatus, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(defaultProcessorURL + "/payments/service-health")
	req.Header.SetMethod(fasthttp.MethodGet)

	if err := defaultHttpClient.Do(req, resp); err != nil {
		return nil, fmt.Errorf("falha na consulta de saúde: %w", err)
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		return nil, errors.New("health check retornou status inválido")
	}

	var result HealthStatus
	if err := fastJson.Unmarshal(resp.Body(), &result); err != nil {
		return nil, fmt.Errorf("erro ao interpretar resposta: %w", err)
	}

	return &result, nil
}

// sendPaymentFallback - Envia pagamento para o processador de reserva
func sendPaymentFallback(req ProcessorRequest) error {
	reqBody, err := fastJson.Marshal(req)
	if err != nil {
		return fmt.Errorf("erro ao serializar request: %w", err)
	}

	request := fasthttp.AcquireRequest()
	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(request)
	defer fasthttp.ReleaseResponse(response)

	request.SetRequestURI(fallbackProcessorURL + "/payments")
	request.Header.SetMethod(fasthttp.MethodPost)
	request.Header.SetContentType("application/json")
	request.SetBody(reqBody)

	if err := fallbackHttpClient.Do(request, response); err != nil {
		return fmt.Errorf("erro na comunicação HTTP: %w", err)
	}

	if response.StatusCode() != fasthttp.StatusOK {
		return errors.New("processador de reserva retornou status inesperado")
	}

	return nil
}

// checkFallbackHealth - Consulta health check do processador de reserva
func checkFallbackHealth() (*HealthStatus, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(fallbackProcessorURL + "/payments/service-health")
	req.Header.SetMethod(fasthttp.MethodGet)

	if err := fallbackHttpClient.Do(req, resp); err != nil {
		return nil, fmt.Errorf("falha na consulta de saúde: %w", err)
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		return nil, errors.New("health check retornou status inválido")
	}

	var result HealthStatus
	if err := fastJson.Unmarshal(resp.Body(), &result); err != nil {
		return nil, fmt.Errorf("erro ao interpretar resposta: %w", err)
	}

	return &result, nil
}

// ============================================================================
// SISTEMA DE TRAVAS DISTRIBUÍDAS
// ============================================================================

// executeWithLock - Executa função com trava Redis para evitar concorrência
func executeWithLock(ctx context.Context, lockKey string, duration time.Duration, function func()) error {
	lockValue := time.Now().UnixNano()

	success, err := redisConnection.SetNX(ctx, lockKey, lockValue, duration).Result()
	if err != nil || !success {
		return errors.New("não foi possível obter a trava")
	}
	defer releaseLock(ctx, lockKey, lockValue)

	function()
	return nil
}

// releaseLock - Remove trava do Redis de forma atômica
func releaseLock(ctx context.Context, lockKey string, lockValue int64) {
	luaScript := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`
	redisConnection.Eval(ctx, luaScript, []string{lockKey}, lockValue)
}

// ============================================================================
// LÓGICA DE SELEÇÃO DE PROCESSADOR
// ============================================================================

// ProcessorCache - Armazena informações do processador selecionado
type ProcessorCache struct {
	CurrentProcessor    string          `json:"processador_atual"`
	DefaultData        json.RawMessage `json:"dados_padrao"`
	FallbackData       json.RawMessage `json:"dados_reserva"`
	WasOverridden     bool            `json:"foi_sobrescrito"`
	UpdateTimestamp    time.Time       `json:"timestamp_update"`
}

// updateProcessorCache - Consulta saúde dos processadores e atualiza cache
func updateProcessorCache(ctx context.Context) error {
	defaultHealth, _ := checkDefaultHealth()
	fallbackHealth, _ := checkFallbackHealth()

	chosenProcessor := chooseBestProcessor(defaultHealth, fallbackHealth)

	return saveProcessorCache(ctx, chosenProcessor, defaultHealth, fallbackHealth, false)
}

// recalculateFailedProcessor - Recalcula processador quando um falha
func recalculateFailedProcessor(ctx context.Context, failingProcessor string) error {
	return executeWithLock(ctx, selectionLockKey, selectionLockTimeout, func() {
		executeProcessorRecalculation(ctx, failingProcessor)
	})
}

// executeProcessorRecalculation - Executa recálculo de processador em caso de falha
func executeProcessorRecalculation(ctx context.Context, failingProcessor string) error {
	cache, err := getProcessorCache(ctx)
	if err != nil || cache == nil || cache.WasOverridden || failingProcessor != cache.CurrentProcessor {
		return nil
	}

	defaultHealth := &HealthStatus{}
	fallbackHealth := &HealthStatus{}

	if err := fastJson.Unmarshal(cache.DefaultData, defaultHealth); err != nil {
		return err
	}
	if err := fastJson.Unmarshal(cache.FallbackData, fallbackHealth); err != nil {
		return err
	}

	// Marca o processador como falhando
	if failingProcessor == "default" {
		defaultHealth.Failing = true
	} else {
		fallbackHealth.Failing = true
	}

	chosenProcessor := chooseBestProcessor(defaultHealth, fallbackHealth)

	return saveProcessorCache(ctx, chosenProcessor, defaultHealth, fallbackHealth, true)
}

// getCurrentProcessor - Retorna o processador atualmente ativo
func getCurrentProcessor(ctx context.Context) (string, error) {
	cache, err := getProcessorCache(ctx)
	if err != nil || cache == nil {
		return "default", nil
	}

	switch cache.CurrentProcessor {
	case "default", "fallback":
		return cache.CurrentProcessor, nil
	default:
		return "default", nil
	}
}

// getProcessorCache - Recupera dados do cache do processador
func getProcessorCache(ctx context.Context) (*ProcessorCache, error) {
	cacheData, err := redisConnection.Get(ctx, processorCacheKey).Result()
	if err != nil || cacheData == "" {
		return nil, err
	}

	var cache ProcessorCache
	if err := fastJson.Unmarshal([]byte(cacheData), &cache); err != nil {
		return nil, err
	}

	return &cache, nil
}

// saveProcessorCache - Persiste dados do processador selecionado no cache
func saveProcessorCache(ctx context.Context, processor string, defaultHealth, fallbackHealth *HealthStatus, overridden bool) error {
	defaultDataJSON, err := fastJson.Marshal(defaultHealth)
	if err != nil {
		return fmt.Errorf("falha ao serializar dados padrão: %w", err)
	}
	fallbackDataJSON, err := fastJson.Marshal(fallbackHealth)
	if err != nil {
		return fmt.Errorf("falha ao serializar dados reserva: %w", err)
	}

	cache := ProcessorCache{
		CurrentProcessor: processor,
		DefaultData:     defaultDataJSON,
		FallbackData:    fallbackDataJSON,
		WasOverridden:  overridden,
		UpdateTimestamp: time.Now().UTC(),
	}

	cacheData, err := fastJson.Marshal(cache)
	if err != nil {
		return fmt.Errorf("falha ao serializar cache: %w", err)
	}

	return redisConnection.Set(ctx, processorCacheKey, cacheData, cacheLifetime).Err()
}

// chooseBestProcessor - Algoritmo para escolher o melhor processador
func chooseBestProcessor(defaultHealth, fallbackHealth *HealthStatus) string {
	switch {
	case defaultHealth != nil && !defaultHealth.Failing && (fallbackHealth == nil || fallbackHealth.Failing):
		return "default"

	case fallbackHealth != nil && !fallbackHealth.Failing && (defaultHealth == nil || defaultHealth.Failing):
		return "fallback"

	case defaultHealth == nil && fallbackHealth == nil:
		return "default"

	case defaultHealth != nil && fallbackHealth != nil && !defaultHealth.Failing && !fallbackHealth.Failing:
		if preferDefaultProcessor(defaultHealth, fallbackHealth) {
			return "default"
		}
		return "fallback"
	}

	return "default"
}

// preferDefaultProcessor - Decide se deve usar o processador padrão baseado na latência
func preferDefaultProcessor(defaultHealth, fallbackHealth *HealthStatus) bool {
	if defaultHealth.ResponseTime <= maxLatencyMs {
		return true
	}
	if fallbackHealth.ResponseTime <= maxLatencyMs {
		return false
	}
	return defaultHealth.ResponseTime < fallbackHealth.ResponseTime+latencyDifferenceMs
}

// ============================================================================
// GERENCIADOR DE HEALTH CHECKS
// ============================================================================

// runHealthManager - Executa verificação periódica de saúde dos processadores
func runHealthManager() {
	ctx := context.Background()

	// Throttling para evitar excesso de health checks
	success, err := redisConnection.SetNX(ctx, healthControlKey, "1", healthCheckInterval).Result()
	if err != nil || !success {
		return
	}

	err = executeWithLock(ctx, healthLockKey, healthLockTimeout, func() {
		updateProcessorCache(ctx)
	})
	if err != nil {
		// Erro health manager (silenciado)
	}
}

// ============================================================================
// SISTEMA DE FILAS E PROCESSAMENTO
// ============================================================================

// TransactionPayload - Dados da transação a ser processada
type TransactionPayload struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

var transactionChannel = make(chan []byte, channelBuffer)

// Canal in-memory para otimização de performance - evita Redis na rota crítica
var inMemoryQueue = make(chan PaymentData, inMemoryQueueSize)

// enqueueTransaction - Adiciona transação na fila in-memory (não-bloqueante)
func enqueueTransaction(payment PaymentData) error {
	mode := getEnvVar("MODE", "monolith")
	
	// MODO API HÍBRIDO: processar direto quando possível, Redis como fallback
	if mode == "api" {
		// Tentar fila in-memory primeiro (ultra-rápido)
		select {
		case inMemoryQueue <- payment:
			return nil
		default:
			// Fila cheia - fallback para Redis
			transactionData, err := fastJson.Marshal(payment)
			if err != nil {
				return fmt.Errorf("erro ao serializar transação: %w", err)
			}
			return redisConnection.RPush(context.Background(), paymentQueueKey, transactionData).Err()
		}
	}
	
	// Modo monolítico: usar fila in-memory real (sem Redis no hot path)
	select {
	case inMemoryQueue <- payment:
		// Sucesso - não fazer log para performance
		return nil
	default:
		// Fila cheia - fallback direto para Redis
		transactionData, err := fastJson.Marshal(payment)
		if err != nil {
			return fmt.Errorf("erro ao serializar transação: %w", err)
		}
		return redisConnection.RPush(context.Background(), paymentQueueKey, transactionData).Err()
	}
}

// clearTransactionQueue - Remove todas as transações pendentes das filas
func clearTransactionQueue() error {
	// Limpar fila in-memory
	for len(inMemoryQueue) > 0 {
		<-inMemoryQueue
	}
	
	// Limpar fila Redis
	return redisConnection.Del(context.Background(), paymentQueueKey).Err()
}

// REMOVIDO: startInMemoryFlusher não é mais necessário
// Agora processamos direto da inMemoryQueue para eliminar latência do Redis

// startProcessingSystem - Inicia workers para processar transações
func startProcessingSystem(ctx context.Context) {
	// Sistema de processamento iniciado

	// REMOVIDO: Flusher in-memory não é mais necessário no hot path
	// Transações vão direto para workers, Redis apenas para persistência após sucesso

	// Inicia enfileirador Redis-to-Channel (para recovery/fallback)
	go startRedisEnqueuer(ctx, 1)

	// Inicia workers para processar transações DIRETO da inMemoryQueue
	for i := 0; i < maxWorkers; i++ {
		go startInMemoryWorker(ctx, i+1)
	}
	
	// Workers adicionais para processar do Redis (recovery)
	for i := 0; i < 10; i++ {
		go startProcessingWorker(ctx, i+1)
	}
}

// startRedisEnqueuer - Move transações do Redis para canal interno
func startRedisEnqueuer(ctx context.Context, workerId int) {
	// Enfileirador iniciado
	
	for {
		select {
		case <-ctx.Done():
			// Enfileirador finalizando
			return
		default:
			result, err := redisConnection.BLPop(ctx, 1*time.Second, paymentQueueKey).Result()
			if err == redis.Nil || len(result) < 2 {
				continue
			} else if err != nil {
				// Erro no enfileirador (silenciado para performance)
				continue
			}

			message := result[1]

			select {
			case transactionChannel <- []byte(message):
				// Transação enviada para canal interno
			default:
				// Canal cheio, recolocar na fila
				_ = redisConnection.LPush(ctx, paymentQueueKey, message).Err()
			}
		}
	}
}

// startInMemoryWorker - Processa transações DIRETO da inMemoryQueue (hot path)
func startInMemoryWorker(ctx context.Context, workerId int) {
	// InMemory Worker iniciado - processa direto sem Redis
	
	for {
		select {
		case <-ctx.Done():
			// Worker finalizando
			return
		case payment := <-inMemoryQueue:
			// Converter PaymentData para TransactionPayload
			transaction := TransactionPayload{
				CorrelationID: payment.CorrelationID,
				Amount:        payment.Amount,
			}
			
			// Processar direto - sem passar pelo Redis!
			processTransactionWithPersistence(ctx, transaction)
		}
	}
}

// startProcessingWorker - Processa transações do canal interno (Redis recovery)
func startProcessingWorker(ctx context.Context, workerId int) {
	// Worker iniciado
	
	for {
		select {
		case <-ctx.Done():
			// Worker finalizando
			return
		case transactionData := <-transactionChannel:
			var transaction TransactionPayload
			if err := fastJson.Unmarshal(transactionData, &transaction); err != nil {
				// Erro deserializacao (silenciado)
				continue
			}
			processTransaction(ctx, transaction)
		}
	}
}

// processTransactionWithPersistence - Processa transação e persiste no Redis APÓS sucesso
func processTransactionWithPersistence(ctx context.Context, transaction TransactionPayload) {
	chosenProcessor, err := getCurrentProcessor(ctx)
	if err != nil {
		// Em caso de erro, enviar para Redis para retry
		requeueTransaction(ctx, transaction)
		return
	}

	params := ProcessorRequest{
		CorrelationID: transaction.CorrelationID,
		Amount:        transaction.Amount,
		RequestedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	var sendError error
	switch chosenProcessor {
	case "default":
		sendError = sendPaymentDefault(params)
	case "fallback":
		sendError = sendPaymentFallback(params)
	default:
		sendError = sendPaymentDefault(params)
	}

	if sendError != nil {
		// Processador falhou, recalcular e enviar para Redis
		recalculateFailedProcessor(ctx, chosenProcessor)
		requeueTransaction(ctx, transaction)
		return
	}

	// Sucesso! Agora sim persiste no Redis (fora do hot path)
	saveProcessedTransaction(ctx, transaction, chosenProcessor, params.RequestedAt)
	
	// Opcional: salvar em lote no Redis para log/auditoria
	// Isso pode ser feito async sem bloquear
}

// processTransaction - Processa uma transação individual (usado pelo recovery do Redis)
func processTransaction(ctx context.Context, transaction TransactionPayload) {
	chosenProcessor, err := getCurrentProcessor(ctx)
	if err != nil {
		requeueTransaction(ctx, transaction)
		return
	}

	params := ProcessorRequest{
		CorrelationID: transaction.CorrelationID,
		Amount:        transaction.Amount,
		RequestedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	var sendError error
	switch chosenProcessor {
	case "default":
		sendError = sendPaymentDefault(params)
	case "fallback":
		sendError = sendPaymentFallback(params)
	default:
		sendError = sendPaymentDefault(params)
	}

	if sendError != nil {
		// Processador falhou, recalcular e recolocar na fila
		recalculateFailedProcessor(ctx, chosenProcessor)
		requeueTransaction(ctx, transaction)
		return
	}

	// Transação processada com sucesso, salvar no histórico
	saveProcessedTransaction(ctx, transaction, chosenProcessor, params.RequestedAt)
}

// requeueTransaction - Recoloca transação na fila em caso de falha
func requeueTransaction(ctx context.Context, transaction TransactionPayload) {
	transactionData, _ := fastJson.Marshal(transaction)
	_ = redisConnection.LPush(ctx, paymentQueueKey, transactionData).Err()
}

// saveProcessedTransaction - Salva transação processada no histórico
func saveProcessedTransaction(ctx context.Context, transaction TransactionPayload, processor, timestamp string) {
	transactionRecord := map[string]interface{}{
		"correlationId": transaction.CorrelationID,
		"amount":        transaction.Amount,
		"processor":     processor,
		"requestedAt":   timestamp,
	}
	dataJSON, _ := fastJson.Marshal(transactionRecord)
	_ = redisConnection.HSet(ctx, resultsTable, transaction.CorrelationID, dataJSON).Err()
}

// ============================================================================
// RELATÓRIOS E RESUMOS
// ============================================================================

// TransactionRecord - Representa uma transação armazenada
type TransactionRecord struct {
	Amount       float64 `json:"amount"`
	Processor string  `json:"processor"`
	RequestedAt string  `json:"requestedAt"`
}

// ProcessorSummary - Estatísticas por processador
type ProcessorSummary struct {
	TotalTransactions int     `json:"totalRequests"`
	TotalAmount      float64 `json:"totalAmount"`
}

// generatePaymentsReport - Gera relatório de pagamentos por período
func generatePaymentsReport(startDate, endDate string) (map[string]ProcessorSummary, error) {
	ctx := context.Background()

	allRecords, err := redisConnection.HGetAll(ctx, resultsTable).Result()
	if err != nil {
		return nil, fmt.Errorf("erro ao buscar histórico: %w", err)
	}

	start, _ := parseDateTime(startDate)
	end, _ := parseDateTime(endDate)

	report := map[string]ProcessorSummary{
		"default":  {0, 0},
		"fallback": {0, 0},
	}

	for _, dataJSON := range allRecords {
		var record TransactionRecord
		if err := fastJson.Unmarshal([]byte(dataJSON), &record); err != nil {
			continue
		}

		transactionTime, err := time.Parse(time.RFC3339Nano, record.RequestedAt)
		if err != nil {
			continue
		}

		if !withinTimeRange(transactionTime, start, end) {
			continue
		}

		if summary, exists := report[record.Processor]; exists {
			summary.TotalTransactions++
			summary.TotalAmount += record.Amount
			report[record.Processor] = summary
		}
	}

	roundReportValues(report)
	return report, nil
}

// parseDateTime - Converte string em time.Time
func parseDateTime(dateStr string) (time.Time, bool) {
	if dateStr == "" {
		return time.Time{}, false
	}
	convertedDate, err := time.Parse(time.RFC3339Nano, dateStr)
	return convertedDate, err == nil
}

// withinTimeRange - Verifica se data está dentro do intervalo especificado
func withinTimeRange(timestamp time.Time, start time.Time, end time.Time) bool {
	if !start.IsZero() && timestamp.Before(start) {
		return false
	}
	if !end.IsZero() && timestamp.After(end) {
		return false
	}
	return true
}

// roundReportValues - Arredonda valores monetários para 2 casas decimais
func roundReportValues(report map[string]ProcessorSummary) {
	for key, summary := range report {
		report[key] = ProcessorSummary{
			TotalTransactions: summary.TotalTransactions,
			TotalAmount:      math.Round(summary.TotalAmount*100) / 100.0,
		}
	}
}

// ============================================================================
// HANDLERS HTTP
// ============================================================================

// checkSystemStatus - Endpoint de health check do sistema (otimizado)
func checkSystemStatus(ctx *fasthttp.RequestCtx) {
	if !ctx.IsGet() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		return
	}

	// Health check ultra-rápido: só verifica se Redis responde
	result, err := redisConnection.Ping(context.Background()).Result()
	if err != nil || result != "PONG" {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString(`{"status":"sistema_indisponivel"}`)
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetBodyString(`{"status":"sistema_operacional"}`)
}

// receivePaymentRequest - Endpoint para receber novos pagamentos
func receivePaymentRequest(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		return
	}

	var paymentData PaymentData
	if err := fastJson.Unmarshal(ctx.PostBody(), &paymentData); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString("formato de dados inválido")
		return
	}

	if err := paymentData.Validate(); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(err.Error())
		return
	}

	if err := enqueueTransaction(paymentData); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("falha ao processar solicitação")
		return
	}

	ctx.SetStatusCode(fasthttp.StatusAccepted)
}

// clearDatabase - Endpoint administrativo para limpar filas (otimizado)
func clearDatabase(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		return
	}

	if err := clearTransactionQueue(); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("falha ao limpar base de dados")
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetBodyString(`{"message":"Base de dados limpa com sucesso"}`)
}

// getTransactionsSummary - Endpoint para obter resumo de transações
func getTransactionsSummary(ctx *fasthttp.RequestCtx) {
	startDate := string(ctx.QueryArgs().Peek("from"))
	endDate := string(ctx.QueryArgs().Peek("to"))

	report, err := generatePaymentsReport(startDate, endDate)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("falha ao gerar relatório")
		return
	}

	ctx.SetContentType("application/json")
	json.NewEncoder(ctx).Encode(report)
}

// ============================================================================
// FUNÇÕES PRINCIPAIS DE INICIALIZAÇÃO
// ============================================================================

func main() {
	connectRedis()
	
	// Verificar modo de operação via variável de ambiente
	mode := getEnvVar("MODE", "monolith")
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Captura sinais do sistema para encerramento gracioso
	go func() {
		signalChannel := make(chan os.Signal, 1)
		signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
		<-signalChannel
		log.Println("Sinal de encerramento recebido")
		cancel()
	}()

	switch mode {
	case "api":
		log.Println("Iniciando modo API HÍBRIDO")
		// API com workers próprios para eliminar latência do Redis
		go startProcessingSystem(ctx)
		runHTTPServer(ctx)
		
	case "worker":
		log.Println("Iniciando modo Worker")
		// Apenas workers de processamento
		go startProcessingSystem(ctx)
		
		// Inicia gerenciador de health checks
		go func() {
			heartbeat := time.NewTicker(2 * time.Second)
			defer heartbeat.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-heartbeat.C:
					runHealthManager()
				}
			}
		}()
		
		// Aguarda sinal de encerramento
		<-ctx.Done()
		
	default:
		// Modo monolítico (fallback)
		log.Println("Iniciando modo monolítico")
		
		// Inicia workers de processamento em background
		go startProcessingSystem(ctx)
		
		// Inicia gerenciador de health checks
		go func() {
			heartbeat := time.NewTicker(2 * time.Second)
			defer heartbeat.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-heartbeat.C:
					runHealthManager()
				}
			}
		}()

		// Inicia servidor HTTP (blocking)
		runHTTPServer(ctx)
	}
}

// runHTTPServer - Servidor HTTP monolítico (API + Workers no mesmo processo)
func runHTTPServer(ctx context.Context) {
	log.Println("Servidor HTTP iniciado na porta 8080")
	
	routerInstance := router.New()
	routerInstance.GET("/healthcheck", checkSystemStatus)
	routerInstance.POST("/payments", receivePaymentRequest)
	routerInstance.GET("/payments-summary", getTransactionsSummary)
	routerInstance.POST("/purge-payments", clearDatabase)

	server := &fasthttp.Server{
		Handler:                       routerInstance.Handler,
		ReadTimeout:                  3 * time.Second,    // Mais agressivo
		WriteTimeout:                 3 * time.Second,    // Mais agressivo  
		IdleTimeout:                  30 * time.Second,   // Reduzido
		ReduceMemoryUsage:            true,
		NoDefaultServerHeader:        true,
		NoDefaultContentType:         true,               // Remove header desnecessário
		DisableHeaderNamesNormalizing: true,              // Evita normalização
		Concurrency:                  512 * 1024,         // Alta concorrência
		MaxRequestBodySize:           4096,               // 4KB - margem para headers + JSON
	}

	// Servidor em goroutine separada para permitir graceful shutdown
	go func() {
		if err := server.ListenAndServe(":8080"); err != nil {
			log.Fatalf("Erro no servidor: %v", err)
		}
	}()

	// Aguarda sinal de encerramento
	<-ctx.Done()
	
	// Graceful shutdown
	log.Println("Finalizando servidor HTTP")
	server.Shutdown()
}
