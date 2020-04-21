package paxi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"

	"github.com/ailidani/paxi/lib"
	"github.com/ailidani/paxi/log"
)

// Client interface provides get and put for key value store
//client接口：实现get、put操作
type Client interface {
	Get(Key) (Value, error)
	Put(Key, Value) error
}

// AdminClient interface provides fault injection opeartion
// AdminClient接口：consensus、crash、drop、partition
type AdminClient interface {
	Consensus(Key) bool
	Crash(ID, int)
	Drop(ID, ID, int)
	Partition(int, ...ID)
}

// HTTPClient inplements Client interface with REST API
//httpclient结构体：node地址、http（客户端服务器地址）、clientID(客户端id与本地站点中的服务器使用相同的id)、
type HTTPClient struct {
	//config.Addrs 所有地址
	Addrs  map[ID]string
	//onfig.HTTPAddrs,
	HTTP   map[ID]string
	ID     ID  // client id use the same id as servers in local site
	N      int // total number of nodes
	LocalN int // number of nodes in local zone

	CID int // command id
	*http.Client
}

// NewHTTPClient creates a new Client from config
func NewHTTPClient(id ID) *HTTPClient {
	c := &HTTPClient{
		ID:     id,
		N:      len(config.Addrs),
		Addrs:  config.Addrs,
		HTTP:   config.HTTPAddrs,
		Client: &http.Client{},
	}
	if id != "" {
		i := 0
		for node := range c.Addrs {
			if node.Zone() == id.Zone() {
				i++
			}
		}
		c.LocalN = i
	}

	return c
}

// Get gets value of given key (use REST)
// Default implementation of Client interface
func (c *HTTPClient) Get(key Key) (Value, error) {
	//commandID==CID
	c.CID++
	v, _, err := c.RESTGet(c.ID, key)
	return v, err
}

// Put puts new key value pair and return previous value (use REST)
// Default implementation of Client interface
func (c *HTTPClient) Put(key Key, value Value) error {
	c.CID++
	_, _, err := c.RESTPut(c.ID, key, value)
	return err
}

//GetURL():用id和key生成URL
func (c *HTTPClient) GetURL(id ID, key Key) string {
	if id == "" {
		for id = range c.HTTP {
			if c.ID == "" || id.Zone() == c.ID.Zone() {
				break
			}
		}
	}
	return c.HTTP[id] + "/" + strconv.Itoa(int(key))
}

// rest accesses server's REST API with url = http://ip:port/key
// if value == nil, it's a read
func (c *HTTPClient) rest(id ID, key Key, value Value) (Value, map[string]string, error) {
	// get url
	url := c.GetURL(id, key)

	method := http.MethodGet
	var body io.Reader
	if value != nil {
		method = http.MethodPut
		body = bytes.NewBuffer(value)
	}
	//生成请求：在URL地址上对数据body进行method操作，method为http读操作或者http写操作。
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Error(err)
		return nil, nil, err
	}
	req.Header.Set(HTTPClientID, string(c.ID))
	req.Header.Set(HTTPCommandID, strconv.Itoa(c.CID))
	// r.Header.Set(HTTPTimestamp, strconv.FormatInt(time.Now().UnixNano(), 10))

	//client执行request
	rep, err := c.Client.Do(req)
	if err != nil {
		log.Error(err)
		return nil, nil, err
	}
	defer rep.Body.Close()

	// 获取request的headers
	metadata := make(map[string]string)
	for k := range rep.Header {
		metadata[k] = rep.Header.Get(k)
	}

	if rep.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(rep.Body)
		if err != nil {
			log.Error(err)
			return nil, metadata, err
		}
		if value == nil {
			log.Debugf("node=%v type=%s key=%v value=%x", id, method, key, Value(b))
		} else {
			log.Debugf("node=%v type=%s key=%v value=%x", id, method, key, value)
		}
		return Value(b), metadata, nil
	}

	// http call failed
	dump, _ := httputil.DumpResponse(rep, true)
	log.Debugf("%q", dump)
	return nil, metadata, errors.New(rep.Status)
}

// RESTGet issues a http call to node and return value and headers
func (c *HTTPClient) RESTGet(id ID, key Key) (Value, map[string]string, error) {
	return c.rest(id, key, nil)
}

// RESTPut puts new value as http.request body and return previous value
func (c *HTTPClient) RESTPut(id ID, key Key, value Value) (Value, map[string]string, error) {
	return c.rest(id, key, value)
}

func (c *HTTPClient) json(id ID, key Key, value Value) (Value, error) {
	url := c.HTTP[id]
	cmd := Command{
		Key:       key,
		Value:     value,
		ClientID:  c.ID,
		CommandID: c.CID,
	}
	data, err := json.Marshal(cmd)
	res, err := c.Client.Post(url, "json", bytes.NewBuffer(data))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(res.Body)
		log.Debugf("key=%v value=%x", key, Value(b))
		return Value(b), nil
	}
	dump, _ := httputil.DumpResponse(res, true)
	log.Debugf("%q", dump)
	return nil, errors.New(res.Status)
}

// JSONGet posts get request in json format to server url
func (c *HTTPClient) JSONGet(key Key) (Value, error) {
	return c.json(c.ID, key, nil)
}

// JSONPut posts put request in json format to server url
func (c *HTTPClient) JSONPut(key Key, value Value) (Value, error) {
	return c.json(c.ID, key, value)
}

// QuorumGet concurrently read values from majority nodes
//同时从majority个结点中读value
func (c *HTTPClient) QuorumGet(key Key) ([]Value, []map[string]string) {
	return c.MultiGet(c.N/2+1, key)
}

// MultiGet concurrently read values from n nodes
//同时从n个结点中读取value
func (c *HTTPClient) MultiGet(n int, key Key) ([]Value, []map[string]string) {
	valueC := make(chan Value)
	metaC := make(chan map[string]string)
	i := 0
	//将http中的request放入通道
	for id := range c.HTTP {
		go func(id ID) {
			v, meta, err := c.rest(id, key, nil)
			if err != nil {
				log.Error(err)
				return
			}
			valueC <- v
			metaC <- meta
		}(id)
		i++
		if i >= n {
			break
		}
	}

	//将通道中的数据追加入value数组和mate数组
	values := make([]Value, 0)
	metas := make([]map[string]string, 0)
	for ; i > 0; i-- {
		values = append(values, <-valueC)
		metas = append(metas, <-metaC)
	}
	return values, metas
}

//从本地N/2个 结点中读取value
func (c *HTTPClient) LocalQuorumGet(key Key) ([]Value, []map[string]string) {
	valueC := make(chan Value)
	metaC := make(chan map[string]string)
	i := 0
	for id := range c.HTTP {
		//判断是否是本地
		if c.ID.Zone() != id.Zone() {
			continue
		}
		i++
		//判断从本地node中的读取数量，只读一半
		if i > c.LocalN/2 {
			break
		}
		//将value数据和meta数据放入通道中
		go func(id ID) {
			v, meta, err := c.rest(id, key, nil)
			if err != nil {
				log.Error(err)
				return
			}
			valueC <- v
			metaC <- meta
		}(id)
	}

	//将通道中的数据加入到value数组和meta数组中
	values := make([]Value, 0)
	metas := make([]map[string]string, 0)
	for ; i >= 0; i-- {
		values = append(values, <-valueC)
		metas = append(metas, <-metaC)
	}
	return values, metas
}

// QuorumPut concurrently write values to majority of nodes
// TODO get headers
//将kv写入一半的结点中
func (c *HTTPClient) QuorumPut(key Key, value Value) {
	var wait sync.WaitGroup
	i := 0
	for id := range c.HTTP {
		i++
		if i > c.N/2 {
			break
		}
		wait.Add(1)
		go func(id ID) {
			c.rest(id, key, value)
			wait.Done()
		}(id)
	}
	wait.Wait()
}

// Consensus collects /history/key from every node and compare their values
//检查每个结点的history，比较他们的值
func (c *HTTPClient) Consensus(k Key) bool {
	h := make(map[ID][]Value)
	for id, url := range c.HTTP {
		h[id] = make([]Value, 0)
		r, err := c.Client.Get(url + "/history?key=" + strconv.Itoa(int(k)))
		if err != nil {
			log.Error(err)
			continue
		}
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error(err)
			continue
		}
		holder := h[id]
		err = json.Unmarshal(b, &holder)
		if err != nil {
			log.Error(err)
			continue
		}
		h[id] = holder
		log.Debugf("node=%v key=%v h=%v", id, k, holder)
	}
	n := 0
	for _, v := range h {
		if len(v) > n {
			n = len(v)
		}
	}
	for i := 0; i < n; i++ {
		set := make(map[string]struct{})
		for id := range c.HTTP {
			if len(h[id]) > i {
				set[string(h[id][i])] = struct{}{}
			}
		}
		if len(set) > 1 {
			return false
		}
	}
	return true
}

// Crash stops the node for t seconds then recover
// node crash forever if t < 0
//crash停止node t秒后恢复，当t>0时，结点不能运行
func (c *HTTPClient) Crash(id ID, t int) {
	url := c.HTTP[id] + "/crash?t=" + strconv.Itoa(t)
	r, err := c.Client.Get(url)
	if err != nil {
		log.Error(err)
		return
	}
	r.Body.Close()
}

// Drop drops every message send for t seconds 将发送的每个message丢弃t秒
func (c *HTTPClient) Drop(from, to ID, t int) {
	url := c.HTTP[from] + "/drop?id=" + string(to) + "&t=" + strconv.Itoa(t)
	r, err := c.Client.Get(url)
	if err != nil {
		log.Error(err)
		return
	}
	r.Body.Close()
}

// Partition cuts the network between nodes for t seconds 分区将节点之间的网络切断t秒
func (c *HTTPClient) Partition(t int, nodes ...ID) {
	s := lib.NewSet()
	for _, id := range nodes {
		s.Add(id)
	}
	for from := range c.Addrs {
		if !s.Has(from) {
			for _, to := range nodes {
				c.Drop(from, to, t)
			}
		}
	}
}
