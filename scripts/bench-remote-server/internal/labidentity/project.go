// Package labidentity defines the fail-closed identity binding shared by the
// synthetic remote-server lab tools. It is deliberately internal to the lab.
package labidentity

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// SQLUser derives the one SQL account that is allowed to access projectID.
// A canonical UUID without separators is exactly MySQL's 32-character user
// limit and preserves every project identity bit; truncation is forbidden.
func SQLUser(projectID string) (string, error) {
	parsed, err := uuid.Parse(projectID)
	if err != nil || parsed == uuid.Nil || parsed.String() != projectID {
		return "", fmt.Errorf("project ID must be a non-nil canonical UUID")
	}
	user := strings.ReplaceAll(projectID, "-", "")
	if len(user) != 32 {
		return "", fmt.Errorf("derived SQL user must be exactly 32 characters")
	}
	return user, nil
}
