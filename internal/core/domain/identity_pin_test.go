package domain

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestIdentityPinTokenOnlyLeavesThroughExplicitJSONField(t *testing.T) {
	token := IdentityPinConfirmationToken(strings.Repeat("ab", 32))
	owner := struct{ Token IdentityPinConfirmationToken }{token}
	for _, value := range []any{token, owner, &owner} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%d", "%x"} {
			rendered := fmt.Sprintf(format, value)
			for _, leak := range []string{string(token), hex.EncodeToString([]byte(token)), fmt.Sprintf("%d", []byte(token))} {
				if strings.Contains(rendered, leak) {
					t.Errorf("%s leaked token through %T", format, value)
				}
			}
		}
	}
	raw, err := json.Marshal(struct {
		Token IdentityPinConfirmationToken `json:"confirm_token"`
	}{token})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), string(token)) {
		t.Fatal("explicit confirmation field lost token")
	}
}
