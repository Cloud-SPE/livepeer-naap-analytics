package runtime

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleListPricing(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListPricing(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list pricing failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.OrchPricingEntry{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetOrchPricingProfile(w http.ResponseWriter, r *http.Request) {
	address := chi.URLParam(r, "address")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "address is required")
		return
	}
	result, err := s.svc.GetOrchPricingProfile(r.Context(), address)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get orch pricing profile failed", "error", err, "address", address)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusNotFound, "Not Found", "orchestrator not found")
		return
	}
	if result.Pipelines == nil {
		result.Pipelines = []types.PipelinePricing{}
	}
	respondJSON(w, http.StatusOK, result)
}
