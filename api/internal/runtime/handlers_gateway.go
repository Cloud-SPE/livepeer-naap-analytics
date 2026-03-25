package runtime

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleListGateways(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListGateways(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list gateways failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.Gateway{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetGatewayProfile(w http.ResponseWriter, r *http.Request) {
	address := chi.URLParam(r, "address")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "address is required")
		return
	}
	result, err := s.svc.GetGatewayProfile(r.Context(), address)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get gateway profile failed", "error", err, "address", address)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusNotFound, "Not Found", "gateway not found")
		return
	}
	if result.OrchsUsed == nil {
		result.OrchsUsed = []string{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListGatewayOrchestrators(w http.ResponseWriter, r *http.Request) {
	address := chi.URLParam(r, "address")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "address is required")
		return
	}
	p := parseQueryParams(r)
	result, err := s.svc.ListGatewayOrchestrators(r.Context(), address, p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list gateway orchestrators failed", "error", err, "address", address)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.GatewayOrch{}
	}
	respondJSON(w, http.StatusOK, result)
}
