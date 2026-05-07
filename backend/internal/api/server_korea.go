package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/daydemir/stoarama/backend/internal/korea"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

type koreaInventoryQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

var loadKoreaInventory = func(ctx context.Context, q koreaInventoryQueryer) (korea.Inventory, error) {
	return korea.LoadInventory(ctx, q)
}

func (s *Server) handleKoreaInventory(w http.ResponseWriter, r *http.Request) {
	inventory, err := loadKoreaInventory(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load korea inventory: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, inventory)
}
