package server

import (
	"context"
	"fmt"
	"github.com/WuKongIM/WuKongIM/internal/datasource"
	"github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/app/middlewares/client/sd"
	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/hertz-contrib/registry/nacos/v2"
	"net/http"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// Datasource Datasource
type Datasource struct {
	s *Server
	c *client.Client
}

// NewDatasource 创建一个数据源
func NewDatasource(s *Server) datasource.IDatasource {
	c, err := client.NewClient()
	if err != nil {
		panic(err)
	}
	r, err := nacos.NewDefaultNacosResolver()
	if err != nil {
		panic(err)
	}
	c.Use(sd.Discovery(r))
	return &Datasource{
		s: s,
		c: c,
	}
}

func (d *Datasource) GetChannelInfo(channelID string, channelType uint8) (wkdb.ChannelInfo, error) {
	result, err := d.requestCMD("getChannelInfo", map[string]interface{}{
		"channel_id":   channelID,
		"channel_type": channelType,
	})
	if err != nil {
		return wkdb.EmptyChannelInfo, err
	}
	var channelInfoResp channelInfoResp
	err = wkutil.ReadJSONByByte([]byte(result), &channelInfoResp)
	if err != nil {
		return wkdb.EmptyChannelInfo, err
	}
	channelInfo := channelInfoResp.toChannelInfo()
	channelInfo.ChannelId = channelID
	channelInfo.ChannelType = channelType
	return wkdb.EmptyChannelInfo, nil

}

// GetSubscribers 获取频道的订阅者
func (d *Datasource) GetSubscribers(channelID string, channelType uint8) ([]string, error) {

	result, err := d.requestCMD("getSubscribers", map[string]interface{}{
		"channel_id":   channelID,
		"channel_type": channelType,
	})
	if err != nil {
		return nil, err
	}
	var subscribers []string
	err = wkutil.ReadJSONByByte([]byte(result), &subscribers)
	if err != nil {
		return nil, err
	}
	return subscribers, nil
}

// GetBlacklist 获取频道的黑名单
func (d *Datasource) GetBlacklist(channelID string, channelType uint8) ([]string, error) {

	result, err := d.requestCMD("getBlacklist", map[string]interface{}{
		"channel_id":   channelID,
		"channel_type": channelType,
	})
	if err != nil {
		return nil, err
	}

	var blacklists []string
	err = wkutil.ReadJSONByByte([]byte(result), &blacklists)
	if err != nil {
		return nil, err
	}
	return blacklists, nil
}

// GetWhitelist 获取频道的白明单
func (d *Datasource) GetWhitelist(channelID string, channelType uint8) ([]string, error) {

	result, err := d.requestCMD("getWhitelist", map[string]interface{}{
		"channel_id":   channelID,
		"channel_type": channelType,
	})
	if err != nil {
		return nil, err
	}
	var whitelists []string
	err = wkutil.ReadJSONByByte([]byte(result), &whitelists)
	if err != nil {
		return nil, err
	}
	return whitelists, nil
}

// GetSystemUIDs 获取系统账号
func (d *Datasource) GetSystemUIDs() ([]string, error) {
	result, err := d.requestCMD("getSystemUIDs", map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	var uids []string
	err = wkutil.ReadJSONByByte([]byte(result), &uids)
	if err != nil {
		return nil, err
	}
	return uids, nil
}

func (d *Datasource) requestCMD(cmd string, param map[string]interface{}) (string, error) {
	dataMap := map[string]interface{}{
		"cmd": cmd,
	}
	if param != nil {
		dataMap["data"] = param
	}
	resp, err := d.post([]byte(wkutil.ToJSON(dataMap)))
	if err != nil {
		return "", err
	}
	if resp.StatusCode() != http.StatusOK {
		return "", fmt.Errorf("http状态码错误！[%d]", resp.StatusCode())
	}

	return string(resp.Body()), nil
}

func (d *Datasource) post(body []byte) (*protocol.Response, error) {
	req, res := &protocol.Request{}, &protocol.Response{}
	req.SetMethod(consts.MethodPost)
	req.SetRequestURI(d.s.opts.Datasource.Addr)
	req.Header.Add("Content-Type", "application/json")
	req.SetBody(body)
	req.SetOptions(config.WithSD(true))
	err := d.c.Do(context.Background(), req, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type channelInfoResp struct {
	Large   int `json:"large"`   // 是否是超大群
	Ban     int `json:"ban"`     // 是否封禁频道（封禁后此频道所有人都将不能发消息，除了系统账号）
	Disband int `json:"disband"` // 是否解散频道
}

func (c channelInfoResp) toChannelInfo() *wkdb.ChannelInfo {
	return &wkdb.ChannelInfo{
		Large: c.Large == 1,
		Ban:   c.Ban == 1,
	}
}
