package paxi

import (
	"encoding/json"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ailidani/paxi/log"
)

// http request header names
const (
	HTTPClientID  = "Id"
	HTTPCommandID = "Cid"
	HTTPTimestamp = "Timestamp"
	HTTPNodeID    = "Id"
)

// serve serves the http REST API request from clients
func (n *node) http() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handleRoot)
	mux.HandleFunc("/b", n.handleBytesRequest)
	mux.HandleFunc("/history", n.handleHistory)
	mux.HandleFunc("/crash", n.handleCrash)
	mux.HandleFunc("/drop", n.handleDrop)
	// http string should be in form of ":8080"
	httpURL, err := url.Parse(config.HTTPAddrs[n.id])
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + httpURL.Port()
	h2s := &http2.Server{
		MaxConcurrentStreams: 2_000,
	}
	n.server = &http.Server{
		Addr:    port,
		Handler: h2c.NewHandler(mux, h2s),
	}
	log.Info("http server starting on ", port)
	log.Fatal(n.server.ListenAndServe())
}

func (n *node) handleRoot(w http.ResponseWriter, r *http.Request) {
	var req Request
	var cmd Command
	var err error

	// get all http headers
	req.Properties = make(map[string]string)
	for k := range r.Header {
		if k == HTTPClientID {
			cmd.ClientID = ID(r.Header.Get(HTTPClientID))
			continue
		}
		if k == HTTPCommandID {
			cmd.CommandID, err = strconv.Atoi(r.Header.Get(HTTPCommandID))
			if err != nil {
				log.Error(err)
			}
			continue
		}
		req.Properties[k] = r.Header.Get(k)
	}

	// get command key and value
	if len(r.URL.Path) > 1 {
		i, err := strconv.Atoi(r.URL.Path[1:])
		if err != nil {
			http.Error(w, "invalid path", http.StatusBadRequest)
			log.Error(err)
			return
		}
		cmd.Key = Key(i)
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Error("error reading body: ", err)
				http.Error(w, "cannot read body", http.StatusBadRequest)
				return
			}
			cmd.Value = Value(body)
		}
	} else {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("error reading body: ", err)
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		_ = json.Unmarshal(body, &cmd)
	}

	req.Command = cmd
	req.Timestamp = time.Now().UnixNano()
	req.NodeID = n.id // TODO does this work when forward twice
	req.c = make(chan Reply, 1)

	n.MessageChan <- req

	reply := <-req.c

	if reply.Err != nil {
		http.Error(w, reply.Err.Error(), http.StatusInternalServerError)
		return
	}

	// set all http headers
	w.Header().Set(HTTPClientID, string(reply.Command.ClientID))
	w.Header().Set(HTTPCommandID, strconv.Itoa(reply.Command.CommandID))
	for k, v := range reply.Properties {
		w.Header().Set(k, v)
	}

	_, err = io.WriteString(w, string(reply.Value))
	if err != nil {
		log.Error(err)
	}
}

// this endpoint (/b) only accept PUT or POST http request
func (n *node) handleBytesRequest(w http.ResponseWriter, r *http.Request) {
	var req BytesRequest

	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.Error(w, "unknown handler for this http method", http.StatusBadRequest)
		log.Error("unknown handler for this http method")
		return
	}

	// get all http headers
	req.Properties = make(map[string]string)
	for k := range r.Header {
		req.Properties[k] = r.Header.Get(k)
	}

	// read the payload (command)
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error("error reading body: ", err)
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}

	req.Command = body
	req.Timestamp = time.Now().Unix()
	req.NodeID = n.id
	req.c = make(chan Reply, 1)

	// send request to replica, wait for the response
	n.MessageChan <- req
	reply := <-req.c

	if reply.Err != nil {
		http.Error(w, reply.Err.Error(), http.StatusInternalServerError)
		return
	}

	// set all http headers
	for k, v := range reply.Properties {
		w.Header().Set(k, v)
	}

	// send the response
	_, err = io.WriteString(w, string(reply.Value))
	if err != nil {
		log.Error(err)
	}
}

func (n *node) handleHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(HTTPNodeID, string(n.id))
	k, err := strconv.Atoi(r.URL.Query().Get("key"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}
	h := n.Database.History(Key(k))
	b, _ := json.Marshal(h)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err)
	}
}

func (n *node) handleCrash(w http.ResponseWriter, r *http.Request) {
	t, err := strconv.Atoi(r.URL.Query().Get("t"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide time", http.StatusBadRequest)
		return
	}
	n.Socket.Crash(t)
	// timer := time.NewTimer(time.Duration(t) * time.Second)
	// go func() {
	// 	n.server.Close()
	// 	<-timer.C
	// 	log.Error(n.server.ListenAndServe())
	// }()
}

func (n *node) handleDrop(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	t, err := strconv.Atoi(r.URL.Query().Get("t"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide time", http.StatusBadRequest)
		return
	}
	n.Drop(ID(id), t)
}
