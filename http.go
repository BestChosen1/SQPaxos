package paxi

import (
	"encoding/json"
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
	//ServeMux类型是HTTP请求的多路转接器。它会将每一个接收的请求的URL与一个注册模式的列表进行匹配，并调用和URL最匹配的模式的处理器
	mux := http.NewServeMux()
	//func (mux *ServeMux) Handle(pattern string, handler Handler):Handle注册HTTP处理器handler和对应的模式pattern
	mux.HandleFunc("/", n.handleRoot)
	mux.HandleFunc("/history", n.handleHistory)
	mux.HandleFunc("/crash", n.handleCrash)
	mux.HandleFunc("/drop", n.handleDrop)
	// http string should be in form of ":8080"
	//func Parse(rawurl string) (url *URL, err error):Parse函数解析rawurl为一个URL结构体，rawurl可以是绝对地址，也可以是相对地址
	url, err := url.Parse(config.HTTPAddrs[n.id])
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + url.Port()
	n.server = &http.Server{
		Addr:    port,
		Handler: mux,
	}
	log.Info("http server starting on ", port)
	log.Fatal(n.server.ListenAndServe())
}

//handleRoot方法：
func (n *node) handleRoot(w http.ResponseWriter, r *http.Request) {
	var req Request
	var cmd Command
	var err error

	// get all http headers获取所有输入参数http.request的header类型并存储到properties数组中
	req.Properties = make(map[string]string)
	for k := range r.Header {
		//根据request的http头判断request类型
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
	//
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
		json.Unmarshal(body, &cmd)
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

//处理history：
func (n *node) handleHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(HTTPNodeID, string(n.id))
	k, err := strconv.Atoi(r.URL.Query().Get("key"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide key", http.StatusBadRequest)
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
	//	//Query方法解析RawQuery字段并返回其表示的Values类型键值对。get("id")返回t对应的值value保存在idS中
	id := r.URL.Query().Get("id")
	t, err := strconv.Atoi(r.URL.Query().Get("t"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide time", http.StatusBadRequest)
		return
	}
	n.Drop(ID(id), t)
}
