package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"github.com/malice-plugins/pkgs/database"
	"github.com/malice-plugins/pkgs/utils"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// IndexResponse holds the json reply from Elasticsearch
type IndexResponse struct {
	Id      string `json:"_id"`
	Index   string `json:"_index"`
	Version int64  `json:"_version"`
	Result  string `json:"result"`
}

// Database is the elasticsearch malice database object
type Database struct {
	Host     string                 `json:"host,omitempty"`
	Port     string                 `json:"port,omitempty"`
	URL      string                 `json:"url,omitempty"`
	Username string                 `json:"username,omitempty"`
	Password string                 `json:"password,omitempty"`
	Index    string                 `json:"index,omitempty"`
	Plugins  map[string]interface{} `json:"plugins,omitempty"`
}

var (
	defaultIndex string
	defaultHost  string
	defaultPort  string
	defaultURL   string
)

func init() {
	defaultIndex = utils.Getopt("MALICE_ELASTICSEARCH_INDEX", "malice")
	defaultHost = utils.Getopt("MALICE_ELASTICSEARCH_HOST", "localhost")
	defaultPort = utils.Getopt("MALICE_ELASTICSEARCH_PORT", "9200")
}

// getURL with the following order of precedence
// - user input (cli)
// - user ENV
// - sane defaults
func (db *Database) getURL() {

	// If not set use defaults
	if len(strings.TrimSpace(db.Index)) == 0 {
		db.Index = defaultIndex
	}
	if len(strings.TrimSpace(db.Host)) == 0 {
		db.Host = defaultHost
	}
	if len(strings.TrimSpace(db.Port)) == 0 {
		db.Port = defaultPort
	}

	// If user set URL param use it
	if len(strings.TrimSpace(db.URL)) == 0 {
		// If running in docker use `elasticsearch`
		if _, exists := os.LookupEnv("MALICE_IN_DOCKER"); exists {
			db.URL = utils.Getopt("MALICE_ELASTICSEARCH_URL", "elasticsearch:"+db.Port)
			log.WithField("elasticsearch_url", db.URL).Debug("running malice in docker")
			return
		}

		db.URL = utils.Getopt("MALICE_ELASTICSEARCH_URL", db.Host+":"+db.Port)
	}
}

// newClient creates an Elasticsearch 8 client for db.URL
func (db *Database) newClient() (*elastic.Client, error) {

	raw := db.URL
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}

	cfg := elastic.Config{
		Addresses: []string{raw},
		Username:  utils.Getopts(db.Username, "MALICE_ELASTICSEARCH_USERNAME", ""),
		Password:  utils.Getopts(db.Password, "MALICE_ELASTICSEARCH_PASSWORD", ""),
	}

	client, err := elastic.NewClient(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create elasticsearch client")
	}

	return client, nil
}

// Init initalizes ElasticSearch for use with malice
func (db *Database) Init() error {

	// Create URL from host/port
	db.getURL()

	// Test connection to ElasticSearch
	err := db.TestConnection()
	if err != nil {
		return errors.Wrap(err, "failed to connect to database")
	}

	client, err := db.newClient()
	if err != nil {
		return errors.Wrap(err, "failed to create elasticsearch client")
	}

	existsResp, err := client.Indices.Exists([]string{db.Index})
	if err != nil {
		return errors.Wrap(err, "failed to check if index exists")
	}
	exists := existsResp.StatusCode == http.StatusOK
	existsResp.Body.Close()

	if !exists {
		// Index does not exist yet.
		createResp, err := client.Indices.Create(db.Index, client.Indices.Create.WithBody(strings.NewReader(mapping)))
		if err != nil {
			return errors.Wrapf(err, "failed to create index: %s", db.Index)
		}
		defer createResp.Body.Close()

		if createResp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(createResp.Body)
			log.Errorf("index creation not acknowledged: %d %s", createResp.StatusCode, strings.TrimSpace(string(body)))
		} else {
			log.Debugf("created index %s", db.Index)
		}
	} else {
		log.Debugf("index %s already exists", db.Index)
	}

	return nil
}

// TestConnection tests the ElasticSearch connection
func (db *Database) TestConnection() error {

	// Create URL from host/port
	db.getURL()

	// connect to ElasticSearch where --link elasticsearch was using via malice in Docker
	client, err := db.newClient()
	if err != nil {
		return errors.Wrap(err, "failed to create elasticsearch client")
	}

	// Ping the Elasticsearch server
	log.Debugf("attempting to PING to: %s", db.URL)
	resp, err := client.Ping()
	if err != nil {
		return errors.Wrap(err, "failed to ping elasticsearch")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.Errorf("failed to ping elasticsearch: %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	log.WithFields(log.Fields{
		"code": resp.StatusCode,
		"url":  db.URL,
	}).Debug("elasticSearch connection successful")

	return nil
}

// WaitForConnection waits for connection to Elasticsearch to be ready
func (db *Database) WaitForConnection(ctx context.Context, timeout int) error {

	var err error

	secondsWaited := 0

	connCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	log.Debug("===> trying to connect to elasticsearch")
	for {
		// Try to connect to Elasticsearch
		select {
		case <-connCtx.Done():
			return errors.Wrapf(err, "connecting to elasticsearch timed out after %d seconds", secondsWaited)
		default:
			err = db.TestConnection()
			if err == nil {
				log.Debugf("elasticsearch came online after %d seconds", secondsWaited)
				return nil
			}
			// not ready yet
			secondsWaited++
			log.Debug(" * could not connect to elasticsearch (sleeping for 1 second)")
			time.Sleep(1 * time.Second)
		}
	}
}

// StoreFileInfo inserts initial sample info into database creating a placeholder for it
func (db *Database) StoreFileInfo(sample map[string]interface{}) (IndexResponse, error) {

	if len(db.Plugins) == 0 {
		return IndexResponse{}, errors.New("Database.Plugins is empty (you must set this field to use this function)")
	}

	// Test connection to ElasticSearch
	err := db.TestConnection()
	if err != nil {
		return IndexResponse{}, errors.Wrap(err, "failed to connect to database")
	}

	client, err := db.newClient()
	if err != nil {
		return IndexResponse{}, errors.Wrap(err, "failed to create elasticsearch client")
	}

	// NOTE: I am not setting ID because I want to be able to re-scan files with updated signatures in the future
	fInfo := map[string]interface{}{
		// "id":      sample.SHA256,
		"file":      sample,
		"plugins":   db.Plugins,
		"scan_date": time.Now().Format(time.RFC3339Nano),
	}

	body, err := json.Marshal(fInfo)
	if err != nil {
		return IndexResponse{}, errors.Wrap(err, "failed to marshal file info")
	}

	resp, err := client.Index(db.Index, bytes.NewReader(body), client.Index.WithOpType("index"))
	if err != nil {
		return IndexResponse{}, errors.Wrap(err, "failed to index file info")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return IndexResponse{}, errors.Errorf("failed to index file info: %d %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var out IndexResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return IndexResponse{}, errors.Wrap(err, "failed to decode index response")
	}

	log.WithFields(log.Fields{
		"id":    out.Id,
		"index": out.Index,
	}).Debug("indexed sample")

	return out, nil
}

// StoreHash stores a hash into the database that has been queried via intel-plugins
func (db *Database) StoreHash(hash string) (IndexResponse, error) {

	if len(db.Plugins) == 0 {
		return IndexResponse{}, errors.New("Database.Plugins is empty (you must set this field to use this function)")
	}

	hashType, err := utils.GetHashType(hash)
	if err != nil {
		return IndexResponse{}, errors.Wrapf(err, "unable to detect hash type: %s", hash)
	}

	// Test connection to ElasticSearch
	err = db.TestConnection()
	if err != nil {
		return IndexResponse{}, errors.Wrap(err, "failed to connect to database")
	}

	client, err := db.newClient()
	if err != nil {
		return IndexResponse{}, errors.Wrap(err, "failed to create elasticsearch client")
	}

	scan := map[string]interface{}{
		// "id":      sample.SHA256,
		"file": map[string]interface{}{
			hashType: hash,
		},
		"plugins":   db.Plugins,
		"scan_date": time.Now().Format(time.RFC3339Nano),
	}

	body, err := json.Marshal(scan)
	if err != nil {
		return IndexResponse{}, errors.Wrap(err, "failed to marshal hash doc")
	}

	resp, err := client.Index(db.Index, bytes.NewReader(body), client.Index.WithOpType("create"))
	if err != nil {
		return IndexResponse{}, errors.Wrapf(err, "unable to index hash: %s", hash)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return IndexResponse{}, errors.Errorf("unable to index hash: %s (status %d %s)", hash, resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var out IndexResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return IndexResponse{}, errors.Wrap(err, "failed to decode index response")
	}

	log.WithFields(log.Fields{
		"id":    out.Id,
		"index": out.Index,
	}).Debug("indexed sample")

	return out, nil
}

// StorePluginResults stores a plugin's results in the database by updating
// the placeholder created by the call to StoreFileInfo
func (db *Database) StorePluginResults(results database.PluginResults) error {

	// Test connection to ElasticSearch
	err := db.TestConnection()
	if err != nil {
		return errors.Wrap(err, "failed to connect to database")
	}

	client, err := db.newClient()
	if err != nil {
		return errors.Wrap(err, "failed to create elasticsearch client")
	}

	// get sample db record
	getResp, err := client.Get(db.Index, results.ID)
	if err != nil {
		return errors.Wrapf(err, "failed to get sample with id: %s", results.ID)
	}
	defer getResp.Body.Close()

	// 404 not found -> create a new document with the plugin results
	if getResp.StatusCode == http.StatusNotFound {
		return db.createPluginDoc(client, results)
	}
	if getResp.StatusCode != http.StatusOK {
		return errors.Errorf("failed to get sample with id: %s (status %d)", results.ID, getResp.StatusCode)
	}

	var getSample struct {
		Index   string `json:"_index"`
		Id      string `json:"_id"`
		Version int64  `json:"_version"`
		Found   bool   `json:"found"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&getSample); err != nil {
		return errors.Wrapf(err, "failed to decode sample with id: %s", results.ID)
	}

	if getSample.Found {
		log.Debugf("got document %s in version %d from index %s\n", getSample.Id, getSample.Version, getSample.Index)
		// The ES _update API expects the partial document wrapped in a "doc" field
		updateBody := map[string]interface{}{
			"doc": map[string]interface{}{
				"scan_date": time.Now().Format(time.RFC3339Nano),
				"plugins": map[string]interface{}{
					results.Category: map[string]interface{}{
						results.Name: results.Data,
					},
				},
			},
		}

		body, err := json.Marshal(updateBody)
		if err != nil {
			return errors.Wrap(err, "failed to marshal plugin results")
		}

		updateResp, err := client.Update(db.Index, getSample.Id, bytes.NewReader(body),
			client.Update.WithRetryOnConflict(3),
			client.Update.WithRefresh("wait_for"),
		)
		if err != nil {
			return errors.Wrapf(err, "failed to update sample with id: %s", results.ID)
		}
		defer updateResp.Body.Close()

		if updateResp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(updateResp.Body)
			return errors.Errorf("failed to update sample with id: %s (status %d %s)", results.ID, updateResp.StatusCode, strings.TrimSpace(string(b)))
		}

		var update struct {
			Id      string `json:"_id"`
			Version int64  `json:"_version"`
		}
		if err := json.NewDecoder(updateResp.Body).Decode(&update); err != nil {
			return errors.Wrapf(err, "failed to decode update response for id: %s", results.ID)
		}

		log.Debugf("updated version of sample %q is now %d\n", update.Id, update.Version)

	} else {
		// ID not found so create new document with `index` command
		return db.createPluginDoc(client, results)
	}

	return nil
}

// createPluginDoc creates a new document holding a plugin's results
func (db *Database) createPluginDoc(client *elastic.Client, results database.PluginResults) error {

	scan := map[string]interface{}{
		"plugins": map[string]interface{}{
			results.Category: map[string]interface{}{
				results.Name: results.Data,
			},
		},
		"scan_date": time.Now().Format(time.RFC3339Nano),
	}

	body, err := json.Marshal(scan)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal plugin doc with id: %s", results.ID)
	}

	resp, err := client.Index(db.Index, bytes.NewReader(body), client.Index.WithOpType("index"))
	if err != nil {
		return errors.Wrapf(err, "failed to create new sample plugin doc with id: %s", results.ID)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("failed to create new sample plugin doc with id: %s (status %d %s)", results.ID, resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var out IndexResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return errors.Wrapf(err, "failed to decode index response for id: %s", results.ID)
	}

	log.WithFields(log.Fields{
		"id":    out.Id,
		"index": out.Index,
	}).Debug("indexed sample")

	return nil
}
