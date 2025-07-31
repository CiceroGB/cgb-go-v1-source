package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// ============================================================================
// MODELS
// ============================================================================

type PaymentRequest struct {
	CorrelationID uuid.UUID       `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
}

type Payment struct {
	CorrelationID uuid.UUID       `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
	ProcessedBy   string          `json:"processedBy"`
	RequestedAt   time.Time       `json:"requestedAt"`
}

type SummaryRow struct {
	ProcessedBy   string
	TotalRequests int64
	TotalAmount   decimal.Decimal
}

type SummaryResponse struct {
	Default  Summary `json:"default"`
	Fallback Summary `json:"fallback"`
}

type Summary struct {
	TotalRequests int64   `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

// ============================================================================
// CHANNELS
// ============================================================================

type ProcessorChannel struct {
	ch chan PaymentRequest
}

func NewProcessorChannel() *ProcessorChannel {
	return &ProcessorChannel{
		// Use buffered channel with size similar to UnboundedChannel
		// SingleReader = true optimization in Go is implicit with single goroutine reading
		ch: make(chan PaymentRequest, 500000), // Increased buffer
	}
}

func (c *ProcessorChannel) WriteAsync(req PaymentRequest) {
	c.ch <- req // Never drop, just like UnboundedChannel in C#
}

func (c *ProcessorChannel) Reader() <-chan PaymentRequest {
	return c.ch
}

type PersistenceChannel struct {
	ch chan Payment
}

func NewPersistenceChannel() *PersistenceChannel {
	return &PersistenceChannel{
		ch: make(chan Payment, 500000), // Increased buffer
	}
}

func (c *PersistenceChannel) WriteAsync(payment Payment) {
	c.ch <- payment // Never drop, just like UnboundedChannel in C#
}

func (c *PersistenceChannel) Reader() <-chan Payment {
	return c.ch
}

// ============================================================================
// RETRY WITH BACKOFF
// ============================================================================

func decorrelatedJitterBackoffV2(attempt int) time.Duration {
	const medianFirstRetryDelay = 1500.0 // 1.5s matching the reference
	
	if attempt == 0 {
		return time.Duration(medianFirstRetryDelay) * time.Millisecond
	}
	
	// Calculate upper bound
	upperBound := medianFirstRetryDelay * math.Pow(2, float64(attempt))
	
	// Random jitter between 0 and upperBound
	jitter := rand.Float64() * upperBound
	
	return time.Duration(jitter) * time.Millisecond
}

// ============================================================================
// PAYMENT PROCESSOR SERVICE
// ============================================================================

type PaymentProcessorService struct {
	client        *http.Client
	baseURL       string
	processorName string
}

func NewPaymentProcessorService(baseURL, name string, timeout time.Duration) *PaymentProcessorService {
	// Configure connection pooling like the reference implementation's SetHandlerLifetime(5min)
	transport := &http.Transport{
		MaxIdleConns:        1000, // Match Kestrel MaxConcurrentConnections
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     5 * time.Minute, // Same as SetHandlerLifetime
		DisableCompression:  true,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   false, // Disable HTTP/2 for lower latency
		MaxConnsPerHost:     1000,
		TLSHandshakeTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	
	return &PaymentProcessorService{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		baseURL:       baseURL,
		processorName: name,
	}
}

func (s *PaymentProcessorService) ProcessAsync(ctx context.Context, payment Payment) bool {
	// Try once without retry - retry is handled by ProcessWithRetry
	resp, err := s.doRequest(ctx, payment)
	if err != nil {
		return false
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	return resp.StatusCode >= 200 && resp.StatusCode < 300 // IsSuccessStatusCode
}

func (s *PaymentProcessorService) ProcessWithRetry(ctx context.Context, payment Payment) bool {
	// Initial attempt
	resp, err := s.doRequest(ctx, payment)
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return true
	}
	
	// Check if we should retry the initial attempt
	shouldRetry := false
	if err != nil {
		// Network error = transient = retry
		shouldRetry = true
	} else if resp != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		// Retry on 5xx (server errors) or 404 (same as the reference implementation)
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusNotFound {
			shouldRetry = true
		}
	}
	
	if !shouldRetry {
		return false
	}
	
	// Now do up to 3 retries with backoff
	for attempt := 0; attempt < 3; attempt++ {
		resp, err := s.doRequest(ctx, payment)
		
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return true
		}
		
		// Check if we should retry
		shouldRetry := false
		if err != nil {
			// Network error = transient = retry
			shouldRetry = true
		} else if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			// Retry on 5xx (server errors) or 404 (same as the reference implementation)
			if resp.StatusCode >= 500 || resp.StatusCode == http.StatusNotFound {
				shouldRetry = true
			}
		}
		
		if !shouldRetry {
			return false
		}
		
		// Wait with backoff
		backoff := decorrelatedJitterBackoffV2(attempt)
		select {
		case <-time.After(backoff):
			// Continue to next attempt
		case <-ctx.Done():
			return false
		}
	}
	return false
}

func (s *PaymentProcessorService) doRequest(ctx context.Context, payment Payment) (*http.Response, error) {
	// Marshal JSON like the reference implementation
	data, err := json.Marshal(payment)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", s.baseURL+"/payments", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	return s.client.Do(req)
}

// ============================================================================
// ORCHESTRATOR SERVICE
// ============================================================================

type PaymentProcessingOrchestratorService struct {
	defaultProcessor  *PaymentProcessorService
	fallbackProcessor *PaymentProcessorService
}

func NewOrchestratorService() *PaymentProcessingOrchestratorService {
	return &PaymentProcessingOrchestratorService{
		defaultProcessor:  NewPaymentProcessorService("http://payment-processor-default:8080", "default", 10*time.Second),
		fallbackProcessor: NewPaymentProcessorService("http://payment-processor-fallback:8080", "fallback", 30*time.Second),
	}
}

func (s *PaymentProcessingOrchestratorService) ProcessAsync(ctx context.Context, payment *Payment) bool {
	// Try default processor with retry policy
	if s.defaultProcessor.ProcessWithRetry(ctx, *payment) {
		payment.ProcessedBy = "default"
		return true
	}

	// Try fallback processor with retry policy
	if s.fallbackProcessor.ProcessWithRetry(ctx, *payment) {
		payment.ProcessedBy = "fallback"
		return true
	}

	return false
}

// ============================================================================
// REPOSITORY
// ============================================================================

type PaymentRepository struct {
	pool *pgxpool.Pool
}

func NewRepository(connString string) (*PaymentRepository, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	// Optimize for high concurrency
	config.MaxConns = 256 // Match the reference implementation maximum pool size
	config.MinConns = 50  // Higher min for faster response
	config.MaxConnLifetime = 5 * time.Minute
	config.MaxConnIdleTime = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return &PaymentRepository{pool: pool}, nil
}

func (r *PaymentRepository) InsertBatchAsync(ctx context.Context, payments []Payment) error {
	if len(payments) == 0 {
		return nil
	}

	rows := make([][]interface{}, len(payments))
	for i, p := range payments {
		rows[i] = []interface{}{p.CorrelationID, p.Amount, p.ProcessedBy, p.RequestedAt}
	}

	_, err := r.pool.CopyFrom(
		ctx,
		pgx.Identifier{"payments"},
		[]string{"correlation_id", "amount", "processed_by", "requested_at_utc"},
		pgx.CopyFromRows(rows),
	)
	return err
}

func (r *PaymentRepository) GetProcessorsSummaryAsync(ctx context.Context, from, to *time.Time) ([]SummaryRow, error) {
	query := `
		SELECT 
			processed_by,
			COUNT(*) as total_requests,
			SUM(amount) as total_amount
		FROM payments
		WHERE ($1::timestamptz IS NULL OR requested_at_utc >= $1)
		  AND ($2::timestamptz IS NULL OR requested_at_utc <= $2)
		GROUP BY processed_by
	`

	rows, err := r.pool.Query(ctx, query, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summaries []SummaryRow
	for rows.Next() {
		var s SummaryRow
		var totalAmount decimal.NullDecimal
		if err := rows.Scan(&s.ProcessedBy, &s.TotalRequests, &totalAmount); err != nil {
			return nil, err
		}
		if totalAmount.Valid {
			s.TotalAmount = totalAmount.Decimal
		} else {
			s.TotalAmount = decimal.Zero
		}
		summaries = append(summaries, s)
	}

	return summaries, nil
}

func (r *PaymentRepository) PurgeAsync(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, "TRUNCATE TABLE payments")
	return err
}

func (r *PaymentRepository) Close() {
	r.pool.Close()
}

// ============================================================================
// WORKER (BACKGROUND SERVICE) - SINGLE INSTANCE
// ============================================================================

func PaymentProcessingJob(
	processorCh <-chan PaymentRequest,
	persistenceCh *PersistenceChannel,
	orchestrator *PaymentProcessingOrchestratorService,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Match the reference implementation exact parameters
	const batchSize = 100
	const maxWaitMs = 50
	const maxParallelism = 10

	// Pre-allocate with exact capacity to avoid reallocations
	buffer := make([]PaymentRequest, 0, batchSize)
	batchPool := &sync.Pool{
		New: func() interface{} {
			return make([]PaymentRequest, 0, batchSize)
		},
	}

	for {
		// Try to get first item
		first, ok := <-processorCh
		if !ok {
			// Channel closed, process remaining buffer
			if len(buffer) > 0 {
				processBatch(buffer, persistenceCh, orchestrator, maxParallelism)
			}
			return
		}
		
		buffer = append(buffer, first)
		
		// Collect batch with timeout (matching the reference)
		batchStart := time.Now()
		for len(buffer) < batchSize {
			elapsed := time.Since(batchStart).Milliseconds()
			if elapsed >= maxWaitMs {
				break
			}
			
			remaining := time.Duration(maxWaitMs-elapsed) * time.Millisecond
			timer := time.NewTimer(remaining)
			done := false
			
			select {
			case req, ok := <-processorCh:
				timer.Stop()
				if !ok {
					if len(buffer) > 0 {
						processBatch(buffer, persistenceCh, orchestrator, maxParallelism)
					}
					return
				}
				buffer = append(buffer, req)
				// Try to read more without blocking (like TryRead)
				drain:
				for len(buffer) < batchSize {
					select {
					case req, ok := <-processorCh:
						if !ok {
							done = true
							break drain
						}
						buffer = append(buffer, req)
					default:
						break drain
					}
				}
				if done {
					break
				}
			case <-timer.C:
				break
			}
		}

		// Process batch
		if len(buffer) > 0 {
			// Use pool for better memory reuse
			batch := buffer
			buffer = batchPool.Get().([]PaymentRequest)[:0]
			
			processBatch(batch, persistenceCh, orchestrator, maxParallelism)
			
			// Return batch to pool after processing
			batch = batch[:0]
			batchPool.Put(batch)
		}
	}
}

// Removed workerPool - not needed with new approach

// Worker pool with fixed workers like Parallel.ForEachAsync
type workerJob struct {
	req PaymentRequest
	ctx context.Context
}

var jobPool = sync.Pool{
	New: func() interface{} {
		return &workerJob{}
	},
}

func processBatch(
	batch []PaymentRequest,
	persistenceCh *PersistenceChannel,
	orchestrator *PaymentProcessingOrchestratorService,
	maxParallelism int,
) {
	ctx := context.Background()
	jobs := make(chan *workerJob, len(batch))
	var wg sync.WaitGroup

	// Start fixed workers (like Parallel.ForEachAsync)
	for i := 0; i < maxParallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Pre-allocate payment for reuse
			payment := &Payment{}
			
			for job := range jobs {
				// Reset and reuse payment struct
				payment.CorrelationID = job.req.CorrelationID
				payment.Amount = job.req.Amount
				payment.RequestedAt = time.Now().UTC()
				payment.ProcessedBy = ""

				if orchestrator.ProcessAsync(job.ctx, payment) {
					persistenceCh.WriteAsync(*payment)
				}
				// Return job to pool
				jobPool.Put(job)
			}
		}()
	}

	// Queue all jobs
	for i := range batch {
		job := jobPool.Get().(*workerJob)
		job.req = batch[i]
		job.ctx = ctx
		jobs <- job
	}
	close(jobs)

	wg.Wait()
}

func PaymentPersistingJob(
	persistenceCh <-chan Payment,
	repo *PaymentRepository,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Match the reference implementation exact parameters  
	const batchSize = 200
	const maxWaitMs = 40

	// Pre-allocate with exact capacity
	buffer := make([]Payment, 0, batchSize)
	persistPool := &sync.Pool{
		New: func() interface{} {
			return make([]Payment, 0, batchSize)
		},
	}

	for {
		// Try to get first item
		first, ok := <-persistenceCh
		if !ok {
			// Channel closed, flush remaining buffer
			if len(buffer) > 0 {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := repo.InsertBatchAsync(ctx, buffer); err != nil {
					log.Printf("Erro ao persistir o batch final: %v", err)
				}
				cancel()
			}
			return
		}
		
		buffer = append(buffer, first)
		
		// Collect batch with timeout (matching the reference)
		batchStart := time.Now()
		for len(buffer) < batchSize {
			elapsed := time.Since(batchStart).Milliseconds()
			if elapsed >= maxWaitMs {
				break
			}
			
			remaining := time.Duration(maxWaitMs-elapsed) * time.Millisecond
			timer := time.NewTimer(remaining)
			done := false
			
			select {
			case payment, ok := <-persistenceCh:
				timer.Stop()
				if !ok {
					if len(buffer) > 0 {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						if err := repo.InsertBatchAsync(ctx, buffer); err != nil {
							log.Printf("Erro ao persistir o batch final: %v", err)
						}
						cancel()
					}
					return
				}
				buffer = append(buffer, payment)
				// Try to read more without blocking (like TryRead)
				drain2:
				for len(buffer) < batchSize {
					select {
					case payment, ok := <-persistenceCh:
						if !ok {
							done = true
							break drain2
						}
						buffer = append(buffer, payment)
					default:
						break drain2
					}
				}
				if done {
					break
				}
			case <-timer.C:
				break
			}
		}

		// Persist batch
		if len(buffer) > 0 {
			// Use pool for better memory reuse
			batch := buffer
			buffer = persistPool.Get().([]Payment)[:0]

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := repo.InsertBatchAsync(ctx, batch); err != nil {
				log.Printf("Erro ao persistir o batch: %v", err)
				// In the reference implementation, errors are logged but batch is lost
			}
			cancel()
			
			// Return batch to pool
			batch = batch[:0]
			persistPool.Put(batch)
		}
	}
}

// ============================================================================
// HTTP HANDLERS
// ============================================================================

func handlePayment(processorCh *ProcessorChannel) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// Decode directly from body - most efficient approach
		var req PaymentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// the reference implementation uses Guid which auto-validates, but NO validation on Amount
		// Just send to channel and return Accepted
		processorCh.WriteAsync(req)
		w.WriteHeader(http.StatusAccepted)
	}
}

func handleSummary(repo *PaymentRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var from, to *time.Time
		
		if fromStr := r.URL.Query().Get("from"); fromStr != "" {
			if t, err := time.Parse(time.RFC3339, fromStr); err == nil {
				from = &t
			}
		}
		
		if toStr := r.URL.Query().Get("to"); toStr != "" {
			if t, err := time.Parse(time.RFC3339, toStr); err == nil {
				to = &t
			}
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		summaries, err := repo.GetProcessorsSummaryAsync(ctx, from, to)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		response := SummaryResponse{
			Default:  Summary{TotalRequests: 0, TotalAmount: 0},
			Fallback: Summary{TotalRequests: 0, TotalAmount: 0},
		}

		for _, s := range summaries {
			switch s.ProcessedBy {
			case "default":
				response.Default.TotalRequests = s.TotalRequests
				response.Default.TotalAmount, _ = s.TotalAmount.Float64()
			case "fallback":
				response.Fallback.TotalRequests = s.TotalRequests
				response.Fallback.TotalAmount, _ = s.TotalAmount.Float64()
			}
		}

		// Pre-allocate buffer for response
		buf, _ := json.Marshal(response)
		
		w.Header().Set("Content-Type", "application/json")
		w.Write(buf)
	}
}

func handlePurge(repo *PaymentRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		if err := repo.PurgeAsync(ctx); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// ============================================================================
// MAIN (PROGRAM.CS EQUIVALENT)  
// ============================================================================

func main() {
	// Set GOMAXPROCS to use all available CPUs
	runtime.GOMAXPROCS(runtime.NumCPU())
	
	// Pre-allocate decoder buffers like the reference implementation uses JsonSerializer
	json.Valid([]byte(`{}`))
	
	// GC settings
	debug.SetGCPercent(100) // Default GC
	debug.SetMemoryLimit(300 * 1024 * 1024) // 300MB limit
	
	// Database connection
	connString := "postgres://user:password@postgres:5432/PaymentsDB?sslmode=disable"
	repo, err := NewRepository(connString)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer repo.Close()

	// Initialize channels (DI equivalent)
	processorCh := NewProcessorChannel()
	persistenceCh := NewPersistenceChannel()

	// Initialize services
	orchestrator := NewOrchestratorService()

	// Start background services (HostedService equivalent)
	var wg sync.WaitGroup
	
	// Start SINGLE PaymentProcessingJob worker (like the reference implementation)
	wg.Add(1)
	go PaymentProcessingJob(processorCh.Reader(), persistenceCh, orchestrator, &wg)

	// Start SINGLE PaymentPersistingJob worker (like the reference implementation)
	wg.Add(1)
	go PaymentPersistingJob(persistenceCh.Reader(), repo, &wg)

	// Routes are configured in the mux above

	// Configure and start server (like Kestrel in the reference implementation)
	// Custom handler to avoid DefaultServeMux overhead
	mux := http.NewServeMux()
	mux.HandleFunc("/payments", handlePayment(processorCh))
	mux.HandleFunc("/payments-summary", handleSummary(repo))
	mux.HandleFunc("/purge-payments", handlePurge(repo))
	mux.HandleFunc("/healthz", handleHealth)
	
	server := &http.Server{
		Addr:              ":80",
		Handler:           mux,
		ReadTimeout:       60 * time.Second,  // Match Kestrel RequestHeadersTimeout
		WriteTimeout:      60 * time.Second,  // Match Kestrel RequestHeadersTimeout
		IdleTimeout:       120 * time.Second, // KeepAliveTimeout
		MaxHeaderBytes:    1 << 10,           // 1KB - smaller for payment requests
		ReadHeaderTimeout: 1 * time.Second,   // Further reduced
	}

	// Graceful shutdown
	go func() {
		log.Println("Server starting on :80")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Shutdown server
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	// Close channels and wait for workers
	close(processorCh.ch)
	close(persistenceCh.ch)
	wg.Wait()

	log.Println("Server shutdown complete")
}