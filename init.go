package paxi

import (
	"flag"
	"net/http"

	"github.com/ailidani/paxi/log"
)

// Init setup paxi package
//初始化设置paxi包
func Init() {
	//解析标签、设置log、加载配置
	flag.Parse()
	log.Setup()
	config.Load()
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000
}
