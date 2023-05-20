package opaxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/valyala/fasthttp"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httputil"
)

// TODO: deprecate this

// Client overwrites read and write operation with generic request
// all requests are sent as POST request to http://ip:port/b with
// command as []byte in the http body
type Client struct {
	*paxi.HTTPClient
}

func NewClient(id paxi.ID) *Client {
	c := &Client{
		HTTPClient: paxi.NewHTTPClient(id),
	}
	return c
}

func (c *Client) Get(key paxi.Key) (paxi.Value, error) {
	c.HTTPClient.CID++

	keyBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyBytes, uint32(key))
	gr := paxi.GenericCommand{
		CommandID: uint32(c.HTTPClient.CID),
		Key:       keyBytes,
	}
	gcb := gr.ToBytesCommand()
	ret, err := c.makeGenericRESTCall(gcb)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) Put(key paxi.Key, value paxi.Value) error {
	_, err := c.PutRet(key, value)
	return err
}

func (c *Client) Put2(key paxi.Key, value paxi.Value) (interface{}, error) {
	return c.PutRet(key, value)
}

func (c *Client) PutRet(key paxi.Key, value paxi.Value) (paxi.Value, error) {
	c.HTTPClient.CID++

	keyBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyBytes, uint32(key))
	gr := paxi.GenericCommand{
		CommandID: uint32(c.HTTPClient.CID),
		Key:       keyBytes,
		Value:     value,
	}
	gcb := gr.ToBytesCommand()
	ret, err := c.makeGenericRESTCall(gcb)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) getURL(id paxi.ID) string {
	if id == "" {
		for id = range c.HTTP {
			if c.ID == "" || id.Zone() == c.ID.Zone() {
				break
			}
		}

		i := rand.Intn(len(c.HTTP))
		for id = range c.HTTP {
			if i == 0 {
				break
			}
			i--
		}
	}
	return c.HTTP[id] + "/b"
}

func (c *Client) makeGenericRESTCall(bodyRaw []byte) ([]byte, error) {
	httpReq, err := http.NewRequest(http.MethodPost, c.getURL(c.ID), bytes.NewBuffer(bodyRaw))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	httpReq.Header.Set(paxi.HTTPClientID, string(c.ID))

	rep, err := c.NativeHTTPClient.Do(httpReq)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer rep.Body.Close()

	if rep.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(rep.Body)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		log.Debugf("node=%v type=%s cmd=%x", c.ID, http.MethodPost, bodyRaw)
		return b, nil
	}

	// http call failed
	dump, _ := httputil.DumpResponse(rep, true)
	log.Debugf("%q", dump)
	return nil, errors.New(rep.Status)
}

func (c *Client) makeGenericRESTCallWithFastHTTP(bodyRaw []byte) ([]byte, error) {
	httpReq := fasthttp.AcquireRequest()
	httpResp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseResponse(httpResp)
		fasthttp.ReleaseRequest(httpReq)
	}()

	httpReq.SetRequestURI(c.getURL(c.ID))
	httpReq.Header.SetMethod(fasthttp.MethodPost)
	httpReq.Header.Set(paxi.HTTPClientID, string(c.ID))
	httpReq.SetBody(bodyRaw)

	err := c.LeaderClient.Do(httpReq, httpResp)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// http call failed
	if httpResp.StatusCode() != fasthttp.StatusOK {
		log.Debugf("failed response: %q", httpResp.Body())
		return nil, errors.New(fmt.Sprintf("failed response: %q", httpResp.Body()))
	}

	rawResponse := make([]byte, len(httpResp.Body()))
	copy(rawResponse, httpResp.Body())
	log.Debugf("node=%v type=%s cmd=%x", c.ID, http.MethodPost, bodyRaw)
	return rawResponse, nil
}
