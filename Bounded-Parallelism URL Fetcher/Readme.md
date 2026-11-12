# Concurrency & Pipeline Design Matrix in Go

A reference guide breaking down concurrent task execution in Go across **three core architectural dimensions** and the **8 possible combinations** they form.

---

## The 3 Core Dimensions

### 1. Concurrency Limit: Bounded vs. Unbounded
Controls **how many tasks can run simultaneously**.
* **Bounded (`≤ N`):** Caps active concurrent goroutines to a fixed limit `N` (e.g., via a buffered channel semaphore or `errgroup.SetLimit(N)`). Protects CPU, memory, file descriptors, and downstream services.
* **Unbounded (`∞`):** Places no cap on simultaneous execution. Spawns a goroutine for every task immediately, risking resource exhaustion on large inputs.

### 2. Admission Policy: Blocking vs. Non-Blocking
Controls **what happens when the concurrency limit `N` is reached** (applies primarily to Bounded systems).
* **Blocking (Backpressure):** The caller/producer pauses and waits until an active worker finishes and frees a slot (`g.Go(...)` with `SetLimit`, or `sem <- struct{}{}`). Guarantees all items are eventually processed.
* **Non-Blocking (Load Shedding):** Excess tasks are immediately rejected, dropped, or return an error without waiting (`g.TryGo(...)`, or `select` with a `default` branch). Protects latency under heavy load.

### 3. Error Strategy: Fail-Fast vs. Without Fail-Fast
Controls **how sibling tasks react when one task returns an error**.
* **Fail-Fast (Correlated Cancellation):** The first non-nil error cancels a shared `context.Context`, immediately aborting all other in-flight tasks and stopping new ones (`errgroup.WithContext(ctx)`).
* **Without Fail-Fast (Independent / Best-Effort):** Errors are recorded per task, while all remaining tasks continue to completion (`errgroup.Group{}` + `errors.Join(...)`).

---

## Complete Combination Matrix (2 × 2 × 2 = 8 Patterns)

| # | Concurrency | Admission | Error Handling | Go Mechanism | System Behavior & Real-World Use Case |
| :- | :--- | :--- | :--- | :--- | :--- |
| **1** | **Bounded** | **Blocking** | **Fail-Fast** | `errgroup.WithContext` + `g.SetLimit(N)` + `g.Go()` | **Safe Transactional Batch:** Runs at most `N` tasks, pauses loop when full, aborts everything on first error. *(Default choice for batch pipelines)* |
| **2** | **Bounded** | **Blocking** | **Without Fail-Fast** | `errgroup.Group{}` + `g.SetLimit(N)` + `g.Go()` | **Best-Effort Batch:** Runs at most `N` tasks, pauses loop when full, collects all errors via `errors.Join`. *(Webhooks, scraping, bulk emails)* |
| **3** | **Bounded** | **Non-Blocking** | **Fail-Fast** | `errgroup.WithContext` + `g.SetLimit(N)` + `g.TryGo()` | **Strict Load-Shedding Fan-Out:** Rejects tasks immediately if `N` are busy, and cancels running tasks if any accepted task fails. |
| **4** | **Bounded** | **Non-Blocking** | **Without Fail-Fast** | `errgroup.Group{}` + `g.SetLimit(N)` + `g.TryGo()` | **Rate-Limited API / Drop-on-Full:** Drops excess requests with `429/503` when `N` workers are busy; isolated failures don't affect peers. |
| **5** | **Unbounded** | **Blocking** *(N/A)* | **Fail-Fast** | `errgroup.WithContext` (no `SetLimit`) | **Small Critical Fan-Out:** Fires all tasks at once; aborts peers immediately if one fails. *(Fetching 3 microservices for 1 API response)* |
| **6** | **Unbounded** | **Blocking** *(N/A)* | **Without Fail-Fast** | `errgroup.Group{}` or `sync.WaitGroup` | **Small Best-Effort Fan-Out:** Fires all tasks at once and waits for every task to finish, collecting all errors. |
| **7** | **Unbounded** | **Non-Blocking** *(N/A)* | **Fail-Fast** | `go func()` + `context.WithCancel` (fire-and-forget) | **Async Event Trigger:** Dispatches all tasks immediately without waiting; cancels shared context if any background task fails. |
| **8** | **Unbounded** | **Non-Blocking** *(N/A)* | **Without Fail-Fast** | Raw `go func()` (fire-and-forget) | **Uncontrolled Fire-and-Forget:** Spawns unlimited independent background goroutines. *(High risk of leaks and OOM in production)* |

> **Note on Unbounded + Blocking vs. Non-Blocking:** Because an **Unbounded** system has no capacity limit (`N = ∞`), admission never fills up—so the producer never blocks on admission. In rows 5–8, "Blocking vs. Non-Blocking" reflects whether the caller waits (`g.Wait()`) for completion or fires-and-forgets.

---

## Code Cheat Sheet (Bounded Patterns 1–4)

### 1. Bounded + Blocking + Fail-Fast
```go
g, ctx := errgroup.WithContext(parentCtx)
g.SetLimit(limit)

for _, url := range urls {
	if ctx.Err() != nil {
		break
	}
	g.Go(func() error {
		return fetch(ctx, url)
	})
}
return g.Wait()
```

### 2. Bounded + Blocking + Without Fail-Fast
```go
var g errgroup.Group
g.SetLimit(limit)
errs := make([]error, len(urls))

for i, url := range urls {
	g.Go(func() error {
		errs[i] = fetch(ctx, url)
		return nil
	})
}
_ = g.Wait()
return errors.Join(errs...)
```

### 3. Bounded + Non-Blocking + Fail-Fast
```go
g, ctx := errgroup.WithContext(parentCtx)
g.SetLimit(limit)

for _, url := range urls {
	started := g.TryGo(func() error {
		return fetch(ctx, url)
	})
	if !started {
		// Shed load immediately instead of blocking
		return errors.New("concurrency limit reached")
	}
}
return g.Wait()
```

### 4. Bounded + Non-Blocking + Without Fail-Fast
```go
var g errgroup.Group
g.SetLimit(limit)
errs := make([]error, len(urls))

for i, url := range urls {
	started := g.TryGo(func() error {
		errs[i] = fetch(ctx, url)
		return nil
	})
	if !started {
		errs[i] = errors.New("dropped: worker pool full")
	}
}
_ = g.Wait()
return errors.Join(errs...)
```
