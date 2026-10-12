package anthropic

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/QuantumNous/new-api/relaykit/relayconvert"
	"github.com/QuantumNous/new-api/relaykit/types"
)

// maxMediaDownload is the largest remote media file the media resolver fetches
const maxMediaDownload = 20 * 1024 * 1024

// registerMediaResolver teaches relaykit how to materialize image/file content:
// data URLs and inline base64 are parsed locally, http(s) URLs are downloaded.
// Anthropic clients (e.g. Claude Code) usually send inline base64 images, so the
// download path only matters for rare URL-sourced media.
func registerMediaResolver() {
	relayconvert.SetMediaResolver(relayconvert.MediaResolver{
		GetBase64Data: getBase64Data,
	})
}

// getBase64Data resolves a file source to base64 data plus its mime type
func getBase64Data(c context.Context, source types.FileSource, _ ...string) (string, string, error) {
	if source == nil {
		return "", "", nil
	}
	data := source.GetRawData()
	if source.IsURL() {
		return downloadAsBase64(c, data)
	}
	// a data URL ("data:<mime>;base64,<payload>") or bare base64
	if mime, b64, ok := strings.Cut(strings.TrimPrefix(data, "data:"), ","); ok && strings.Contains(mime, "base64") {
		return b64, strings.TrimSuffix(strings.Split(mime, ";")[0], ";"), nil
	}
	return data, "", nil
}

// downloadAsBase64 fetches a URL and returns its content as base64 with the
// reported content type
func downloadAsBase64(c context.Context, rawURL string) (string, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "", "", nil
	}
	req, err := http.NewRequestWithContext(c, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", "", &httpError{status: resp.StatusCode}
	}
	buf, err := io.ReadAll(io.LimitReader(resp.Body, maxMediaDownload+1))
	if err != nil {
		return "", "", err
	}
	if len(buf) > maxMediaDownload {
		return "", "", &httpError{status: http.StatusRequestEntityTooLarge}
	}
	return base64.StdEncoding.EncodeToString(buf), resp.Header.Get("Content-Type"), nil
}

// httpError carries a download status code so the caller can log it
type httpError struct{ status int }

func (e *httpError) Error() string { return "media download failed: " + http.StatusText(e.status) }
