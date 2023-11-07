package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/WuKongIM/WuKongIM/internal/server/cluster/pb"
	"github.com/WuKongIM/WuKongIM/internal/server/cluster/rpc"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Cluster struct {
	multiRaft *MultiRaft // 节点之间的raft服务
	opts      *Options
	wklog.Log

	clusterManager *ClusterManager // 分布式配置管理

	leaderID atomic.Uint64 // 领导ID

	// slotRaftServer *multiraft.Server // slot 多组raft服务

	stopChan chan struct{}

	grpcServer *rpc.Server

	peerGRPCClient *PeerGRPCClient
}

func New(opts *Options) *Cluster {

	err := opts.load()
	if err != nil {
		panic(err)
	}
	c := &Cluster{
		Log:      wklog.NewWKLog(fmt.Sprintf("Cluster[%d]", opts.PeerID)),
		stopChan: make(chan struct{}),
		opts:     opts,
	}

	err = os.MkdirAll(opts.DataDir, 0755)
	if err != nil {
		c.Panic("mkdir data dir is error", zap.Error(err))
	}
	clusterPath := path.Join(opts.DataDir, "cluster.json")

	// -------------------- grpc server --------------------
	c.peerGRPCClient = NewPeerGRPCClient(c)
	c.grpcServer = rpc.NewServer(wkproto.New(), opts.GRPCEvent, opts.GRPCAddr)

	// -------------------- 分布式配置管理者 --------------------
	clusterManagerOpts := NewClusterManagerOptions()
	clusterManagerOpts.ConfigPath = clusterPath
	clusterManagerOpts.SlotCount = opts.SlotCount
	clusterManagerOpts.ReplicaCount = opts.ReplicaCount
	clusterManagerOpts.PeerID = opts.PeerID
	clusterManagerOpts.GRPCServerAddr = opts.GRPCServerAddr
	clusterManagerOpts.APIServerAddr = opts.APIServerAddr
	clusterManagerOpts.GetSlotState = func(slot uint32) SlotState {
		if c.multiRaft.IsStarted(slot) {
			return SlotStateStarted
		}
		return SlotStateNotStart
	}
	c.clusterManager = NewClusterManager(clusterManagerOpts)

	// // 领导改变
	// raftOpts.LeaderChange = func(newLeaderID, oldLeaderID uint64) {
	// 	c.leaderID.Store(newLeaderID)
	// 	if opts.LeaderChange != nil {
	// 		if newLeaderID != oldLeaderID {
	// 			opts.LeaderChange(newLeaderID)
	// 		}
	// 	}
	// 	c.clusterManager.SetLeaderID(newLeaderID)
	// }
	// // 应用日志
	// raftOpts.OnApply = func(m []raftpb.Entry) error {
	// 	return c.nodeApply(m)
	// }

	// -------------------- multi raft --------------------
	multiRaftOpts := NewMultiRaftOptions()
	multiRaftOpts.DataDir = opts.DataDir
	multiRaftOpts.ListenAddr = opts.Addr
	multiRaftOpts.ServerAddr = opts.ServerAddr
	multiRaftOpts.PeerID = opts.PeerID
	multiRaftOpts.Peers = opts.Peers
	multiRaftOpts.SlotCount = opts.SlotCount
	multiRaftOpts.OnApplyForPeer = c.onNodeApply
	multiRaftOpts.OnApplyForSlot = c.opts.OnSlotApply
	multiRaftOpts.OnLeaderChanged = func(slot uint32, leaderID uint64) {
		if slot == PeerShardID {
			fmt.Println("OnLeaderChanged----->", leaderID)
			if leaderID == c.opts.PeerID {
				c.bootstrap()
			}
			c.leaderID.Store(leaderID)
			if opts.LeaderChange != nil {
				opts.LeaderChange(leaderID)
			}
			c.clusterManager.SetLeaderID(leaderID)
		} else {
			c.clusterManager.SetSlotLeader(slot, leaderID)
		}

	}
	c.multiRaft = NewMultiRaft(multiRaftOpts)

	return c
}

func (c *Cluster) Start() error {

	c.grpcServer.Start()

	var err error
	err = c.clusterManager.Start()
	if err != nil {
		return err
	}

	// if len(c.opts.Peers) > 0 {
	// 	for _, peer := range c.opts.Peers {
	// 		if peer.ID == c.opts.NodeID {
	// 			continue
	// 		}
	// 		err = c.transporter.AddPeer(peer)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// }
	err = c.multiRaft.Start()
	if err != nil {
		return err
	}

	go c.loopClusterConfig()

	return nil
}

func (c *Cluster) Stop() {

	close(c.stopChan)

	c.multiRaft.Stop()

	c.clusterManager.Stop()

}

func (c *Cluster) bootstrap() {

	peers := c.clusterManager.GetPeers()
	if len(peers) == 0 && len(c.opts.Peers) > 0 {
		pbPeers := make([]*pb.Peer, 0)
		for _, p := range c.opts.Peers {
			pbPeers = append(pbPeers, &pb.Peer{
				PeerID:         p.ID,
				ServerAddr:     p.ServerAddr,
				GrpcServerAddr: p.GRPCServerAddr,
			})
		}
		err := c.requestUpdateClusterConfig(&pb.Cluster{
			Peers:        pbPeers,
			SlotCount:    uint32(c.opts.SlotCount),
			ReplicaCount: uint32(c.opts.ReplicaCount),
		})
		if err != nil {
			c.Panic("bootstrap requestUpdateConfig error", zap.Error(err))
			return
		}
	}
}

func (c *Cluster) loopClusterConfig() {
	for {
		select {
		case clusterReady := <-c.clusterManager.readyChan:
			if clusterReady.AllocateSlotSet != nil {
				c.requestAllocateSlotSet(clusterReady.AllocateSlotSet)
			}
			if clusterReady.SlotActions != nil {
				c.handleSlotActions(clusterReady.SlotActions)
			}
			if clusterReady.UpdatePeer != nil {
				c.requestUpdatePeer(clusterReady.UpdatePeer)
			}
			if clusterReady.SlotLeaderRelationSet != nil {
				c.requestUpdateSlotLeaderRelationSet(clusterReady.SlotLeaderRelationSet)
			}

		case <-c.stopChan:
			return
		}
	}
}

func (c *Cluster) handleSlotActions(actions []*SlotAction) {
	if len(actions) == 0 {
		return
	}
	for _, action := range actions {
		if action.Action == SlotActionStart {
			slot := c.clusterManager.GetSlot(action.SlotID)
			if slot != nil && !c.multiRaft.IsStarted(slot.Slot) {
				c.startSlot(slot)
			}

		}
	}
}

func (c *Cluster) requestUpdatePeer(peer *pb.Peer) {
	req := pb.NewCMDReq(uint32(pb.CMDUpdatePeerConfig))
	param, err := peer.Marshal()
	if err != nil {
		c.Error("peer marshal error", zap.Error(err))
		return
	}
	req.Param = param
	data, err := req.Marshal()
	if err != nil {
		c.Error("cmd request marshal error", zap.Error(err))
		return
	}
	err = c.ProposeToPeer(data)
	if err != nil {
		c.Error("request add peer propose error", zap.Error(err))
		return
	}
}

func (c *Cluster) requestUpdateClusterConfig(cluster *pb.Cluster) error {

	req := pb.NewCMDReq(uint32(pb.CMDUpdateClusterConfig))
	param, err := cluster.Marshal()
	if err != nil {
		c.Error("cluster marshal error", zap.Error(err))
		return err
	}
	req.Param = param
	data, err := req.Marshal()
	if err != nil {
		c.Error("cmd request marshal error", zap.Error(err))
		return err
	}
	err = c.ProposeToPeer(data)
	if err != nil {
		c.Error("request add peer propose error", zap.Error(err))
		return err
	}
	return nil
}

func (c *Cluster) requestUpdateSlotLeaderRelationSet(slotLeaderRelationSet *pb.SlotLeaderRelationSet) {
	if slotLeaderRelationSet == nil || len(slotLeaderRelationSet.SlotLeaderRelations) == 0 {
		return
	}
	req := pb.NewCMDReq(uint32(pb.CMDUpdateSlotLeaderRelationSet))
	param, err := slotLeaderRelationSet.Marshal()
	if err != nil {
		c.Error("slotLeaderRelationSet marshal error", zap.Error(err))
		return
	}
	req.Param = param
	data, err := req.Marshal()
	if err != nil {
		c.Error("cmd request marshal error", zap.Error(err))
		return
	}
	err = c.ProposeToPeer(data)
	if err != nil {
		c.Error("request add peer propose error", zap.Error(err))
		return
	}
	c.clusterManager.UpdatedSlotLeaderRelations(slotLeaderRelationSet)

}

func (c *Cluster) requestAllocateSlotSet(allocateSlotSet *pb.AllocateSlotSet) {
	if len(allocateSlotSet.AllocateSlots) == 0 {
		return
	}
	req := pb.NewCMDReq(pb.CMDAllocateSlot.Uint32())

	param, err := allocateSlotSet.Marshal()
	if err != nil {
		c.Error("request allocate slot marshal error", zap.Error(err))
		return
	}
	req.Param = param
	data, err := req.Marshal()
	if err != nil {
		c.Error("request init slot marshal error", zap.Error(err))
		return
	}
	err = c.ProposeToPeer(data)
	if err != nil {
		c.Error("request init slot propose error", zap.Error(err))
		return
	}
}

func (c *Cluster) ProposeToPeer(data []byte) error {
	return c.multiRaft.SyncProposeToPeer(data)
}

func (c *Cluster) SyncProposeToSlot(slotID uint32, data []byte) ([]byte, error) {
	slot := c.clusterManager.GetSlot(slotID)
	if slot == nil {
		return nil, fmt.Errorf("not sync propose reason is slot nil ")
	}
	exist := false
	for _, peerID := range slot.Peers {
		if c.opts.PeerID == peerID {
			exist = true
			break
		}
	}
	if exist {
		return c.multiRaft.SyncProposeToSlot(slotID, data)
	}
	leader := c.clusterManager.GetLeaderPeer(slotID)
	if leader == nil {
		c.Error("not sync propose reason is leader nil ", zap.Uint32("slotID", slotID))
		return nil, fmt.Errorf("not sync propose reason is leader nil ")
	}
	resp, err := c.sendSyncProposeToLeader(leader.PeerID, &rpc.SendSyncProposeReq{
		Slot: slotID,
		Data: data,
	})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (c *Cluster) sendSyncProposeToLeader(peerID uint64, req *rpc.SendSyncProposeReq) (*rpc.SendSyncProposeResp, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), c.opts.GRPCSendTimeout)
	data, _ := req.Marshal()
	resp, err := c.peerGRPCClient.SendCMD(timeoutCtx, peerID, &rpc.CMDReq{
		Cmd:  rpc.CMDType_SendSyncPropose,
		Data: data,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if resp.Status != rpc.Status_Success {
		return nil, fmt.Errorf("send sendSyncProposeToLeader fail")
	}
	sendSyncProposeResp := &rpc.SendSyncProposeResp{}
	err = sendSyncProposeResp.Unmarshal(resp.Data)
	if err != nil {
		return nil, err
	}
	return sendSyncProposeResp, nil
}

func (c *Cluster) GetOnePeer(v string) *pb.Peer {
	slotID := c.getSlotID(v)
	return c.clusterManager.GetOnePeerBySlotID(slotID)
}

func (c *Cluster) GetPeer(peerID uint64) *pb.Peer {
	return c.clusterManager.GetPeer(peerID)
}

// 当前节点是否可以处理该内容
func (c *Cluster) InPeer(v string) bool {
	return c.inPeer(v)
}
func (c *Cluster) inPeer(v string) bool {
	slotID := c.getSlotID(v)
	slot := c.clusterManager.GetSlot(slotID)
	if slot == nil {
		return false
	}
	for _, peerID := range slot.Peers {
		if c.opts.PeerID == peerID {
			return true
		}
	}
	return false
}

// BelongPeer 是否属于当前节点
func (c *Cluster) BelongPeer(v string) (bool, error) {
	leader := c.GetLeaderPeer(v)
	if leader == nil {
		return false, errors.New("leader is nil")
	}
	fmt.Println("BelongPeer---->", leader)
	return leader.PeerID == c.opts.PeerID, nil
}

// GetLeaderPeer 获取slot的leader节点
func (c *Cluster) GetLeaderPeer(v string) *pb.Peer {
	slotID := c.getSlotID(v)
	fmt.Println("slotID----->", slotID, v)
	return c.clusterManager.GetLeaderPeer(slotID)
}

func (c *Cluster) getSlotID(v string) uint32 {
	return wkutil.GetSlotNum(int(c.clusterManager.GetSlotCount()), v)
}
func (c *Cluster) GetSlotID(v string) uint32 {
	return wkutil.GetSlotNum(int(c.clusterManager.GetSlotCount()), v)
}

func (c *Cluster) SendConnectRequest(peerID uint64, req *rpc.ConnectReq) (*rpc.ConnectResp, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), c.opts.GRPCSendTimeout)

	data, _ := req.Marshal()
	resp, err := c.peerGRPCClient.SendCMD(timeoutCtx, peerID, &rpc.CMDReq{
		Cmd:  rpc.CMDType_SendConnectReq,
		Data: data,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if resp.Status != rpc.Status_Success {
		return nil, fmt.Errorf("send connect request fail")
	}
	connectResp := &rpc.ConnectResp{}
	err = connectResp.Unmarshal(resp.Data)
	if err != nil {
		return nil, err
	}
	return connectResp, nil
}

func (c *Cluster) ConnectWrite(peerID uint64, req *rpc.ConnectWriteReq) (rpc.Status, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), c.opts.GRPCSendTimeout)
	data, _ := req.Marshal()
	resp, err := c.peerGRPCClient.SendCMD(timeoutCtx, peerID, &rpc.CMDReq{
		Cmd:  rpc.CMDType_ConnWrite,
		Data: data,
	})
	cancel()
	if err != nil {
		return rpc.Status_Error, err
	}

	return resp.Status, nil
}

func (c *Cluster) ConnPing(peerID uint64, req *rpc.ConnPingReq) (rpc.Status, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), c.opts.GRPCSendTimeout)
	data, _ := req.Marshal()
	resp, err := c.peerGRPCClient.SendCMD(timeoutCtx, peerID, &rpc.CMDReq{
		Cmd:  rpc.CMDType_ConnPing,
		Data: data,
	})
	cancel()
	if err != nil {
		return 0, err
	}
	return resp.Status, nil
}

func (c *Cluster) ForwardSendPacketReq(peerID uint64, req *rpc.ForwardSendPacketReq) (*rpc.ForwardSendPacketResp, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), c.opts.GRPCSendTimeout)
	data, _ := req.Marshal()
	resp, err := c.peerGRPCClient.SendCMD(timeoutCtx, peerID, &rpc.CMDReq{
		Cmd:  rpc.CMDType_ForwardSendPacket,
		Data: data,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if resp.Status != rpc.Status_Success {
		return nil, fmt.Errorf("send forwardSendPacketReq fail")
	}
	forwardSendPacketResp := &rpc.ForwardSendPacketResp{}
	err = forwardSendPacketResp.Unmarshal(resp.Data)
	if err != nil {
		return nil, err
	}
	return forwardSendPacketResp, nil
}

func (c *Cluster) ForwardRecvPacketReq(peerID uint64, data []byte) error {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), c.opts.GRPCSendTimeout)
	resp, err := c.peerGRPCClient.SendCMD(timeoutCtx, peerID, &rpc.CMDReq{
		Cmd:  rpc.CMDType_ForwardRecvPacket,
		Data: data,
	})
	cancel()
	if err != nil {
		return err
	}
	if resp.Status != rpc.Status_Success {
		return fmt.Errorf("send forwardRecvPacketReq fail")
	}
	return nil
}

func (c *Cluster) ForwardRecvackPacketReq(peerID uint64, req *rpc.RecvacksReq) error {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), c.opts.GRPCSendTimeout)
	data, _ := req.Marshal()
	resp, err := c.peerGRPCClient.SendCMD(timeoutCtx, peerID, &rpc.CMDReq{
		Cmd:  rpc.CMDType_ForwardRecvackPacket,
		Data: data,
	})
	cancel()
	if err != nil {
		return err
	}
	if resp.Status != rpc.Status_Success {
		return fmt.Errorf("send forwardRecvackPacketReq fail")
	}
	return nil
}
