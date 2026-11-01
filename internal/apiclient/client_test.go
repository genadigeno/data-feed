package apiclient

import (
	"errors"
	"strings"
	"testing"

	"github.com/genadigeno/data-feed/internal/apperr"
)

func TestDecodeError(t *testing.T) {
	body := `{"error":{"code":"ERR_INVALID_TRANSITION","message":"no","problems":[{"path":"a","code":"b","message":"c"}],"request_id":"r"}}`
	e, ok := apperr.As(DecodeError(409, []byte(body)))
	if !ok || e.Kind != apperr.Conflict || e.Code != "ERR_INVALID_TRANSITION" || e.Message != "no" || len(e.Problems) != 1 {
		t.Fatalf("%+v", e)
	}
	for status, want := range map[int]apperr.Kind{
		401: apperr.Unauthenticated, 403: apperr.PermissionDenied, 404: apperr.NotFound, 422: apperr.InvalidArgument, 413: apperr.PayloadTooLarge, 500: apperr.Internal,
	} {
		e, _ := apperr.As(DecodeError(status, []byte(body)))
		if e.Kind != want {
			t.Errorf("%d: %v", status, e.Kind)
		}
	}
	unavailable, _ := apperr.As(DecodeError(503, []byte(body)))
	if !unavailable.Retryable {
		t.Fatal("503 should be retryable")
	}
	// A proxy's HTML page is still an error, with its status.
	html, _ := apperr.As(DecodeError(502, []byte("<html>bad gateway</html>")))
	if html.Code != "ERR_SERVER_RESPONSE" || !strings.Contains(html.Message, "502") {
		t.Fatalf("%+v", html)
	}
}

func TestReadSSE(t *testing.T) {
	stream := "event: resync\ndata: {}\n\n: heartbeat\n\nevent: state\ndata: {\"a\":1}\n\nevent: multi\ndata: one\ndata: two\n\nevent: last\ndata: 9\n\n"
	var seen []string
	err := ReadSSE(strings.NewReader(stream), func(e SSEEvent) (bool, error) {
		seen = append(seen, e.Type+"="+string(e.Data))
		return e.Type != "last", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"resync={}", `state={"a":1}`, "multi=one\ntwo", "last=9"}
	if strings.Join(seen, "|") != strings.Join(want, "|") {
		t.Fatalf("%q", seen)
	}
}

func TestReadSSEStops(t *testing.T) {
	count := 0
	err := ReadSSE(strings.NewReader("event: a\ndata: 1\n\nevent: b\ndata: 2\n\n"), func(SSEEvent) (bool, error) {
		count++
		return false, nil
	})
	if err != nil || count != 1 {
		t.Fatalf("%d %v", count, err)
	}
	boom := errors.New("boom")
	err = ReadSSE(strings.NewReader("event: a\ndata: 1\n\n"), func(SSEEvent) (bool, error) { return false, boom })
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
}

func TestListOptions(t *testing.T) {
	q := ListOptions{Limit: 5, Cursor: "abc"}.values()
	if q.Get("limit") != "5" || q.Get("cursor") != "abc" {
		t.Fatal(q)
	}
	if len(ListOptions{}.values()) != 0 {
		t.Fatal("sent empty parameters")
	}
}

func TestGroupPathEscapes(t *testing.T) {
	if got := groupPath("cat/1", 2, "a b"); got != "/catalogs/cat%2F1/versions/2/groups/a%20b" {
		t.Fatal(got)
	}
}
