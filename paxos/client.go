package paxos

import (
	"strconv"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Client overwrites read operation for Paxos
type Client struct {
	*paxi.HTTPClient
	ballot paxi.Ballot
}

//client是HTTPclient类型
func NewClient(id paxi.ID) *Client {
	return &Client{
		HTTPClient: paxi.NewHTTPClient(id),
	}
}

// Get implements paxi.Client interface
// there are three reading modes:
// (1) read as normal command
// (2) read from leader with current ballot number
// (3) read from quorum of replicas with barrier从具有屏障的副本的仲裁中读取屏障
//三种读模式
func (c *Client) Get(key paxi.Key) (paxi.Value, error) {
	c.HTTPClient.CID++
	switch *read {
	case "leader":
		return c.readLeader(key)
	case "quorum":
		return c.readQuorum(key)
	case "any":
		return c.readAny(key)
	default:
		return c.HTTPClient.Get(key)
	}
}

//写
func (c *Client) Put(key paxi.Key, value paxi.Value) error {
	c.HTTPClient.CID++
	//restput函数返回：(Value, map[string]string, error).meta[k]=rep.Header.Get(k)
	_, meta, err := c.RESTPut(c.ID, key, value)
	if err == nil {
		//根据request的header（string类型）生成ballot（int型）
		b := paxi.NewBallotFromString(meta[HTTPHeaderBallot])
		//更新c.ballot
		if b > c.ballot {
			c.ballot = b
		}
	}

	return err
}

//readleader():
func (c *Client) readLeader(key paxi.Key) (paxi.Value, error) {
	if c.ballot == 0 {
		//restGet():返回value，mate，err。mate是header[][]。本地读取
		v, meta, err := c.HTTPClient.RESTGet(c.ID, key)
		c.ballot = paxi.NewBallotFromString(meta[HTTPHeaderBallot])
		return v, err
	}
	// check ballot number 远程读取
	v, meta, err := c.HTTPClient.RESTGet(c.ballot.ID(), key)
	b := paxi.NewBallotFromString(meta[HTTPHeaderBallot])
	//如果b比最新的ballot高，更新ballot
	if b > c.ballot {
		c.ballot = b
	}
	return v, err
}

func (c *Client) readQuorum(key paxi.Key) (paxi.Value, error) {
	majority := c.N/2 + 1
	//阻碍
	barrier := -1
	numReachedBarrier := 0
	numInProgress := 0
	var value paxi.Value

	// quorum read
	values, metadatas := c.QuorumGet(key)
	for i, v := range values {
		//Stoic():string转int
		slot, err := strconv.Atoi(metadatas[i][HTTPHeaderSlot])
		if err != nil {
			log.Error(err)
			continue
		}
		//inProgress bool 类型
		inProgress, err := strconv.ParseBool(metadatas[i][HTTPHeaderInProgress])
		if err != nil {
			log.Error(err)
			continue
		}
		if inProgress {
			numInProgress++
		}
		//slot比barrier大：重新赋值barrier，
		if slot > barrier {
			barrier = slot
			numReachedBarrier = 1
			value = v
		} else if slot == barrier {
			numReachedBarrier++
		}
	}

	// wait for slot to be executed by any node 等待任何节点执行插槽
	for numInProgress > 0 && numReachedBarrier < majority {
		// read from random node
		_, metadata, err := c.HTTPClient.RESTGet("", key)
		if err != nil {
			return nil, err
		}
		// get executed slot
		execute, err := strconv.Atoi(metadata[HTTPHeaderExecute])
		if err != nil {
			log.Error(err)
			continue
		}
		if execute >= barrier {
			break
		}

		// get highest accepted slot
		slot, err := strconv.Atoi(metadata[HTTPHeaderSlot])
		if err != nil {
			log.Error(err)
			continue
		}
		if slot >= barrier {
			numReachedBarrier++
		}
	}

	return value, nil
}

func (c *Client) readAny(key paxi.Key) (paxi.Value, error) {
	v, _, err := c.HTTPClient.RESTGet(c.ID, key)
	return v, err
}
