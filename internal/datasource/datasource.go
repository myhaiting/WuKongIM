package datasource

import "github.com/WuKongIM/WuKongIM/pkg/wkdb"

// IDatasource 数据源第三方应用可以提供
type IDatasource interface {
	// 获取订阅者
	GetSubscribers(channelID string, channelType uint8) ([]string, error)
	// 获取黑名单
	GetBlacklist(channelID string, channelType uint8) ([]string, error)
	// 获取白名单
	GetWhitelist(channelID string, channelType uint8) ([]string, error)
	// 获取系统账号的uid集合 系统账号可以给任何人发消息
	GetSystemUIDs() ([]string, error)
	// 获取频道信息
	GetChannelInfo(channelID string, channelType uint8) (wkdb.ChannelInfo, error)
}
