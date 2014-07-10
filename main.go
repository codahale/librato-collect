package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/jmoiron/jsonq"
)

func main() {
	var (
		gaugePaths, counterPaths stringList
		metricsURL, source       string
		email, token             string
	)
	flag.StringVar(&metricsURL, "url", "", "URL of the service's metrics")
	flag.StringVar(&source, "source", "", "an optional source to use instead of the URL's host")
	flag.Var(&gaugePaths, "gauge", "the JSON path to a gauges's value")
	flag.Var(&counterPaths, "counter", "the JSON path to a counter's value")
	flag.StringVar(&email, "email", "", "Librato account email")
	flag.StringVar(&token, "token", "", "Librato account token")
	flag.Parse()

	if metricsURL == "" {
		fmt.Fprintln(os.Stderr, "No URL provided")
		flag.Usage()
		os.Exit(1)
	}

	if source == "" {
		u, err := url.Parse(metricsURL)
		if err != nil {
			panic(err)
		}
		source = u.Host
	}

	metrics := fetchMetrics(metricsURL)
	batch := batchMetrics(metrics, source, gaugePaths, counterPaths)
	postBatch(batch, email, token)
}

func postBatch(batch batch, email, token string) {
	j, err := json.Marshal(batch)
	if err != nil {
		panic(err)
	}

	r := bytes.NewReader(j)
	req, err := http.NewRequest("POST", "https://metrics-api.librato.com/v1/metrics", r)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", basicAuth(email, token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != 200 {
		body := bytes.NewBuffer(nil)
		if _, err := io.Copy(body, resp.Body); err != nil {
			panic(err)
		}
		fmt.Printf("Error: %s\n\n%s\n", resp.Status, body.String())
		os.Exit(1)
	}
}

func basicAuth(u, p string) string {
	creds := base64.URLEncoding.EncodeToString([]byte(u + ":" + p))
	return fmt.Sprintf("Basic %s", creds)
}

type batch struct {
	Gauges   map[string]gauge   `json:"gauges"`
	Counters map[string]counter `json:"counters"`
	Source   string             `json:"source"`
}

type gauge struct {
	Value float64 `json:"value"`
}

type counter struct {
	Value int `json:"value"`
}

func batchMetrics(jq *jsonq.JsonQuery, source string, gauges, counters []string) batch {
	b := batch{
		Gauges:   make(map[string]gauge),
		Counters: make(map[string]counter),
		Source:   source,
	}

	for _, path := range gauges {
		v, err := jq.Float(strings.Split(path, ".")...)
		if err != nil {
			panic(err)
		}
		b.Gauges[path] = gauge{Value: v}
	}

	for _, path := range counters {
		v, err := jq.Int(strings.Split(path, ".")...)
		if err != nil {
			panic(err)
		}
		b.Counters[path] = counter{Value: v}
	}

	return b
}

func fetchMetrics(url string) *jsonq.JsonQuery {
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != 200 {
		panic("received a " + resp.Status + " response")
	}

	var metrics map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		panic(err)
	}

	return jsonq.NewQuery(metrics)
}

type stringList []string

func (l *stringList) Set(v string) error {
	*l = append(*l, v)
	return nil
}

func (l *stringList) String() string {
	return strings.Join(*l, ",")
}
