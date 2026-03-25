package runtime

import (
	"net/http"

	"github.com/livepeer/naap-analytics/internal/types"
)

func (s *Server) handleGetPaymentSummary(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.GetPaymentSummary(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("get payment summary failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result.ByOrg == nil {
		result.ByOrg = map[string]types.OrgPaymentTotal{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListPaymentHistory(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListPaymentHistory(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list payment history failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.PaymentBucket{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListPaymentsByPipeline(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListPaymentsByPipeline(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list payments by pipeline failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.PipelinePayment{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListPaymentsByOrch(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListPaymentsByOrch(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list payments by orch failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.OrchPayment{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListPaymentsByGateway(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListPaymentsByGateway(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list payments by gateway failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.GatewayPayment{}
	}
	respondJSON(w, http.StatusOK, result)
}

func (s *Server) handleListPaymentsByStream(w http.ResponseWriter, r *http.Request) {
	p := parseQueryParams(r)
	result, err := s.svc.ListPaymentsByStream(r.Context(), p)
	if err != nil {
		s.providers.Logger.Sugar().Errorw("list payments by stream failed", "error", err)
		writeError(w, http.StatusInternalServerError, "Internal Server Error", "")
		return
	}
	if result == nil {
		result = []types.StreamPayment{}
	}
	respondJSON(w, http.StatusOK, result)
}
