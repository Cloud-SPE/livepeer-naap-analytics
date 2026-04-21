package resolver

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const (
	repairRequestMaxWindowAnyOrg    = time.Hour
	repairRequestMaxWindowScopedOrg = 6 * time.Hour
)

type createRepairRequestPayload struct {
	Org         string `json:"org"`
	From        string `json:"from"`
	To          string `json:"to"`
	RequestedBy string `json:"requested_by"`
	Reason      string `json:"reason"`
	DryRun      bool   `json:"dry_run"`
}

func (e *Engine) registerAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/internal/v1/repair-requests", e.handleRepairRequests)
	mux.HandleFunc("/internal/v1/repair-requests/", e.handleRepairRequestByID)
}

func (e *Engine) handleRepairRequests(w http.ResponseWriter, r *http.Request) {
	if !e.authorizeAdminRequest(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		e.handleListRepairRequests(w, r)
	case http.MethodPost:
		e.handleCreateRepairRequest(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *Engine) handleRepairRequestByID(w http.ResponseWriter, r *http.Request) {
	if !e.authorizeAdminRequest(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	requestID := strings.TrimPrefix(r.URL.Path, "/internal/v1/repair-requests/")
	if requestID == "" || strings.Contains(requestID, "/") {
		http.NotFound(w, r)
		return
	}
	request, err := e.repo.repairRequestState(r.Context(), requestID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if request == nil {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, request)
}

func (e *Engine) handleListRepairRequests(w http.ResponseWriter, r *http.Request) {
	org := strings.TrimSpace(r.URL.Query().Get("org"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	requests, err := e.repo.listRepairRequests(r.Context(), org, nil, status, 100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"requests": requests})
}

func (e *Engine) handleCreateRepairRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var payload createRepairRequestPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	request, err := newRepairRequestFromPayload(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := e.repo.createRepairRequest(r.Context(), request); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	created, err := e.repo.repairRequestState(r.Context(), request.RequestID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if created == nil {
		http.Error(w, "created repair request not found", http.StatusInternalServerError)
		return
	}
	resolverRepairRequestsTotal.WithLabelValues("queued").Inc()
	writeJSON(w, http.StatusAccepted, created)
}

func (e *Engine) authorizeAdminRequest(w http.ResponseWriter, r *http.Request) bool {
	token := strings.TrimSpace(e.cfg.ResolverAdminToken)
	if token == "" {
		http.Error(w, "resolver admin api disabled", http.StatusServiceUnavailable)
		return false
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(auth, "Bearer ") {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return false
	}
	if strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")) != token {
		http.Error(w, "invalid bearer token", http.StatusUnauthorized)
		return false
	}
	return true
}

func newRepairRequestFromPayload(payload createRepairRequestPayload) (repairRequest, error) {
	from, err := time.Parse(time.RFC3339, strings.TrimSpace(payload.From))
	if err != nil {
		return repairRequest{}, errors.New("from must be valid RFC3339")
	}
	to, err := time.Parse(time.RFC3339, strings.TrimSpace(payload.To))
	if err != nil {
		return repairRequest{}, errors.New("to must be valid RFC3339")
	}
	if !from.Before(to) {
		return repairRequest{}, errors.New("from must be earlier than to")
	}
	org := strings.TrimSpace(payload.Org)
	if err := validateRepairRequestWindow(org, from.UTC(), to.UTC()); err != nil {
		return repairRequest{}, err
	}
	requestedBy := strings.TrimSpace(payload.RequestedBy)
	if requestedBy == "" {
		return repairRequest{}, errors.New("requested_by is required")
	}
	reason := strings.TrimSpace(payload.Reason)
	if reason == "" {
		reason = "manual_repair"
	}
	return repairRequest{
		RequestID:   stableHash("repair-request", org, from.UTC().Format(time.RFC3339Nano), to.UTC().Format(time.RFC3339Nano), requestedBy, reason, boolKey(payload.DryRun)),
		Org:         org,
		WindowStart: from.UTC(),
		WindowEnd:   to.UTC(),
		RequestedBy: requestedBy,
		Reason:      reason,
		DryRun:      payload.DryRun,
	}, nil
}

func validateRepairRequestWindow(org string, from, to time.Time) error {
	window := to.Sub(from)
	if window > repairRequestMaxWindowScopedOrg {
		return errors.New("repair request window must be 6h or less; use resolver-repair-window or backfill for larger ranges")
	}
	if org == "" && window > repairRequestMaxWindowAnyOrg {
		return errors.New("org is required for repair requests over 1h; use resolver-repair-window or backfill for broader all-org recovery")
	}
	return nil
}

func boolKey(v bool) string {
	if v {
		return "dry-run"
	}
	return "mutating"
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
