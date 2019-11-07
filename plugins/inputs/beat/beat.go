package beat

import (
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/internal/tls"
	"github.com/influxdata/telegraf/plugins/inputs"

	jsonparser "github.com/influxdata/telegraf/plugins/parsers/json"
)

const sampleConfig = `
  ## An URL from which to read Beat-formatted JSON
  ## Default is "http://127.0.0.1:5066".
  url = "http://127.0.0.1:5066"

  ## Enable collection of the Beat stats
  collect_beat_stats = true

  ## Enable the collection if Libbeat stats
  collect_libbeat_stats = true

  ## Enable the collection of OS level stats
  collect_system_stats = false

  ## Enable the collection of Filebeat stats
  collect_filebeat_stats = true

  ## HTTP method
  # method = "GET"

  ## Optional HTTP headers
  # headers = {"X-Special-Header" = "Special-Value"}

  ## Override HTTP "Host" header
  # host_header = "logstash.example.com"

  ## Timeout for HTTP requests
  timeout = "5s"

  ## Optional HTTP Basic Auth credentials
  # username = "username"
  # password = "pa$$word"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`

const description = "Read metrics exposed by Beat"

const suffixInfo = "/"
const suffixStats = "/stats"

type BeatInfo struct {
	Beat     string `json:"beat"`
	Hostname string `json:"hostname"`
	Name     string `json:"name"`
	UUID     string `json:"uuid"`
	Version  string `json:"version"`
}

type BeatStats struct {
	Beat     map[string]interface{} `json:"beat"`
	FileBeat interface{}            `json:"filebeat"`
	Libbeat  interface{}            `json:"libbeat"`
	System   interface{}            `json:"system"`
}

type Beat struct {
	URL string `toml:"url"`

	CollectBeatStats     bool `toml:"collect_beat_stats"`
	CollectLibbeatStats  bool `toml:"collect_libbeat_stats"`
	CollectSystemStats   bool `toml:"collect_system_stats"`
	CollectFilebeatStats bool `toml:"collect_filebeat_stats"`

	Username   string            `toml:"username"`
	Password   string            `toml:"password"`
	Method     string            `toml:"method"`
	Headers    map[string]string `toml:"headers"`
	HostHeader string            `toml:"host_header"`
	Timeout    internal.Duration `toml:"timeout"`

	tls.ClientConfig
	client *http.Client
}

func NewBeat() *Beat {
	return &Beat{
		URL:                  "http://127.0.0.1:5066",
		CollectBeatStats:     true,
		CollectLibbeatStats:  true,
		CollectSystemStats:   true,
		CollectFilebeatStats: true,
		Method:               "GET",
		Headers:              make(map[string]string),
		HostHeader:           "",
		Timeout:              internal.Duration{Duration: time.Second * 5},
	}
}

func (beat *Beat) Description() string {
	return description
}

func (beat *Beat) SampleConfig() string {
	return sampleConfig
}

// createHttpClient create a clients to access API
func (beat *Beat) createHttpClient() (*http.Client, error) {
	tlsConfig, err := beat.ClientConfig.TLSConfig()
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: beat.Timeout.Duration,
	}

	return client, nil
}

// gatherJsonData query the data source and parse the response JSON
func (beat *Beat) gatherJsonData(url string, value interface{}) error {

	var method string
	if beat.Method != "" {
		method = beat.Method
	} else {
		method = "GET"
	}

	request, err := http.NewRequest(method, url, nil)
	if err != nil {
		return err
	}

	if (beat.Username != "") || (beat.Password != "") {
		request.SetBasicAuth(beat.Username, beat.Password)
	}
	for header, value := range beat.Headers {
		request.Header.Add(header, value)
	}
	if beat.HostHeader != "" {
		request.Host = beat.HostHeader
	}

	response, err := beat.client.Do(request)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	err = json.NewDecoder(response.Body).Decode(value)
	if err != nil {
		return err
	}

	return nil
}

func (beat *Beat) gatherInfoTags(url string) (map[string]string, error) {
	beatInfo := &BeatInfo{}

	err := beat.gatherJsonData(url, beatInfo)
	if err != nil {
		return nil, err
	}

	tags := map[string]string{
		"beat_id":      beatInfo.UUID,
		"beat_name":    beatInfo.Name,
		"beat_host":    beatInfo.Hostname,
		"beat_version": beatInfo.Version,
	}

	return tags, nil
}

func (beat *Beat) gatherStats(accumulator telegraf.Accumulator) error {
	beatStats := &BeatStats{}

	infoUrl, err := url.Parse(beat.URL + suffixInfo)
	if err != nil {
		return err
	}
	statsUrl, err := url.Parse(beat.URL + suffixStats)
	if err != nil {
		return err
	}

	tags, err := beat.gatherInfoTags(infoUrl.String())
	if err != nil {
		return err
	}

	err = beat.gatherJsonData(statsUrl.String(), beatStats)
	if err != nil {
		return err
	}

	if beat.CollectBeatStats {
		flattenerBeat := jsonparser.JSONFlattener{}
		err := flattenerBeat.FlattenJSON("", beatStats.Beat)
		if err != nil {
			return err
		}
		accumulator.AddFields("beat", flattenerBeat.Fields, tags)
	}

	if beat.CollectFilebeatStats {
		flattenerBeat := jsonparser.JSONFlattener{}
		err := flattenerBeat.FlattenJSON("", beatStats.FileBeat)
		if err != nil {
			return err
		}
		accumulator.AddFields("beat_filebeat", flattenerBeat.Fields, tags)
	}

	if beat.CollectLibbeatStats {
		flattenerLibbeat := jsonparser.JSONFlattener{}
		err := flattenerLibbeat.FlattenJSON("", beatStats.Libbeat)
		if err != nil {
			return err
		}
		accumulator.AddFields("beat_libbeat", flattenerLibbeat.Fields, tags)
	}

	if beat.CollectSystemStats {
		flattenerSystem := jsonparser.JSONFlattener{}
		err := flattenerSystem.FlattenJSON("", beatStats.System)
		if err != nil {
			return err
		}
		accumulator.AddFields("beat_system", flattenerSystem.Fields, tags)
	}

	return nil
}

func (beat *Beat) Gather(accumulator telegraf.Accumulator) error {
	if beat.client == nil {
		client, err := beat.createHttpClient()

		if err != nil {
			return err
		}
		beat.client = client
	}

	err := beat.gatherStats(accumulator)
	if err != nil {
		return err
	}

	return nil
}

func init() {
	inputs.Add("beat", func() telegraf.Input {
		return NewBeat()
	})
}
