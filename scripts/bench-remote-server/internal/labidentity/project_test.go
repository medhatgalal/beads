package labidentity

import "testing"

func TestSQLUserPreservesCompleteProjectIdentity(t *testing.T) {
	const project = "11111111-1111-4111-8111-111111111111"
	user, err := SQLUser(project)
	if err != nil {
		t.Fatal(err)
	}
	if user != "11111111111141118111111111111111" || len(user) != 32 {
		t.Fatalf("SQL user = %q, want complete 32-hex project identity", user)
	}

	other, err := SQLUser("11111111-1111-4111-8111-111111111112")
	if err != nil {
		t.Fatal(err)
	}
	if other == user {
		t.Fatal("different project identities shared one SQL user")
	}
}

func TestSQLUserRejectsNoncanonicalAndNilProjects(t *testing.T) {
	for _, project := range []string{
		"",
		"11111111111141118111111111111111",
		"11111111-1111-4111-8111-11111111111A",
		"00000000-0000-0000-0000-000000000000",
	} {
		if user, err := SQLUser(project); err == nil {
			t.Fatalf("project %q produced SQL user %q", project, user)
		}
	}
}
