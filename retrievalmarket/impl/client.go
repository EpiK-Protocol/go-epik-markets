package retrievalimpl

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hannahhoward/go-pubsub"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	versionedfsm "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
	"github.com/filecoin-project/go-multistore"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-statemachine/fsm"

	"github.com/filecoin-project/go-fil-markets/discovery"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/clientstates"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/dtutils"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/migrations"
	rmnet "github.com/filecoin-project/go-fil-markets/retrievalmarket/network"
	"github.com/filecoin-project/go-fil-markets/shared"
)

var log = logging.Logger("retrieval")

// Client is the production implementation of the RetrievalClient interface
type Client struct {
	network      rmnet.RetrievalMarketNetwork
	dataTransfer datatransfer.Manager
	multiStore   *multistore.MultiStore
	node         retrievalmarket.RetrievalClientNode
	dealIDGen    *shared.TimeCounter

	subscribers          *pubsub.PubSub
	readySub             *pubsub.PubSub
	resolver             discovery.PeerResolver
	stateMachines        fsm.Group
	migrateStateMachines func(context.Context) error

	// Guards concurrent access to Retrieve method
	retrieveLk  sync.Mutex
	checkEvents *lru.ARCCache
}

type internalEvent struct {
	evt   retrievalmarket.ClientEvent
	state retrievalmarket.ClientDealState
}

type checkEvent struct {
	start  time.Time
	dealID retrievalmarket.DealID
}

func dispatcher(evt pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	ie, ok := evt.(internalEvent)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb, ok := subscriberFn.(retrievalmarket.ClientSubscriber)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb(ie.evt, ie.state)
	return nil
}

var _ retrievalmarket.RetrievalClient = &Client{}

// NewClient creates a new retrieval client
func NewClient(network rmnet.RetrievalMarketNetwork, multiStore *multistore.MultiStore, dataTransfer datatransfer.Manager, node retrievalmarket.RetrievalClientNode, resolver discovery.PeerResolver, ds datastore.Batching) (retrievalmarket.RetrievalClient, error) {
	checkEvents, err := lru.NewARC(10000)
	if err != nil {
		return nil, err
	}

	c := &Client{
		network:      network,
		multiStore:   multiStore,
		dataTransfer: dataTransfer,
		node:         node,
		resolver:     resolver,
		dealIDGen:    shared.NewTimeCounter(),
		subscribers:  pubsub.New(dispatcher),
		readySub:     pubsub.New(shared.ReadyDispatcher),
		checkEvents:  checkEvents,
	}
	retrievalMigrations, err := migrations.ClientMigrations.Build()
	if err != nil {
		return nil, err
	}
	c.stateMachines, c.migrateStateMachines, err = versionedfsm.NewVersionedFSM(ds, fsm.Parameters{
		Environment:     &clientDealEnvironment{c},
		StateType:       retrievalmarket.ClientDealState{},
		StateKeyField:   "Status",
		Events:          clientstates.ClientEvents,
		StateEntryFuncs: clientstates.ClientStateEntryFuncs,
		FinalityStates:  clientstates.ClientFinalityStates,
		Notifier:        c.notifySubscribers,
	}, retrievalMigrations, "2")
	if err != nil {
		return nil, err
	}
	err = dataTransfer.RegisterVoucherResultType(&retrievalmarket.DealResponse{})
	if err != nil {
		return nil, err
	}
	err = dataTransfer.RegisterVoucherResultType(&migrations.DealResponse0{})
	if err != nil {
		return nil, err
	}
	err = dataTransfer.RegisterVoucherType(&retrievalmarket.DealProposal{}, nil)
	if err != nil {
		return nil, err
	}
	err = dataTransfer.RegisterVoucherType(&migrations.DealProposal0{}, nil)
	if err != nil {
		return nil, err
	}
	err = dataTransfer.RegisterVoucherType(&retrievalmarket.DealPayment{}, nil)
	if err != nil {
		return nil, err
	}
	err = dataTransfer.RegisterVoucherType(&migrations.DealPayment0{}, nil)
	if err != nil {
		return nil, err
	}
	dataTransfer.SubscribeToEvents(dtutils.ClientDataTransferSubscriber(c.stateMachines))
	transportConfigurer := dtutils.TransportConfigurer(network.ID(), &clientStoreGetter{c})
	err = dataTransfer.RegisterTransportConfigurer(&retrievalmarket.DealProposal{}, transportConfigurer)
	if err != nil {
		return nil, err
	}
	err = dataTransfer.RegisterTransportConfigurer(&migrations.DealProposal0{}, transportConfigurer)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Start initialized the Client, performing relevant database migrations
func (c *Client) Start(ctx context.Context) error {
	go func() {
		err := c.migrateStateMachines(ctx)
		if err != nil {
			log.Errorf("Migrating retrieval client state machines: %s", err.Error())
		}
		if err := c.restartDeals(ctx); err != nil {
			log.Errorf("Failed to restart retrieve deals: %w", err)
		}
		err = c.readySub.Publish(err)
		if err != nil {
			log.Warnf("Publish retrieval client ready event: %s", err.Error())
		}

		go c.loop(ctx)
	}()
	return nil
}

// OnReady registers a listener for when the client has finished starting up
func (c *Client) OnReady(ready shared.ReadyFunc) {
	c.readySub.Subscribe(ready)
}

func (c *Client) restartDeals(ctx context.Context) error {
	var deals []retrievalmarket.ClientDealState
	err := c.stateMachines.List(&deals)
	if err != nil {
		return err
	}

	for _, deal := range deals {
		if c.stateMachines.IsTerminated(deal) {
			continue
		}

		// err = c.addMultiaddrs(ctx, retrievalmarket.RetrievalPeer{Address: deal.MinerWallet,
		// 	PieceCID: deal.PieceCID,
		// })
		// if err != nil {
		// 	return err
		// }

		// err = c.stateMachines.Send(deal.ID, retrievalmarket.ClientEventDataTransferError, xerrors.Errorf("deal data transfer stalled (client restart)"))
		err := c.CancelDeal(deal.ID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) loop(ctx context.Context) error {
	for {

		ebt := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			ebt.Stop()
			return nil
		case <-ebt.C:
			c.checkTimeOut()
		}
	}
}

func (c *Client) checkTimeOut() error {
	keys := c.checkEvents.Keys()
	for _, rk := range keys {
		v, _ := c.checkEvents.Get(rk)
		event := v.(*checkEvent)
		var deal retrievalmarket.ClientDealState
		err := c.stateMachines.GetSync(context.TODO(), event.dealID, &deal)
		if err != nil {
			return err
		}
		if c.stateMachines.IsTerminated(deal) {
			c.checkEvents.Remove(rk)
			log.Warnf("retrievel client remove check events: %s, status:%d", rk, deal.Status)
		} else {
			if time.Now().Sub(event.start) > 30*time.Minute {
				// err := c.stateMachines.Send(event.state.ID, retrievalmarket.ClientEventDataTransferError, xerrors.Errorf("deal state timeout error"))
				err := c.CancelDeal(deal.ID)
				if err != nil {
					return err
				}
				c.checkEvents.Remove(rk)
				log.Warnf("retrievel client timeout check events: %s, status:%d", rk, deal.Status)
			}
		}
	}
	return nil
}

// Stop ends deal processing on a StorageClient
func (c *Client) Stop() error {
	return c.stateMachines.Stop(context.TODO())
}

// FindProviders uses PeerResolver interface to locate a list of providers who may have a given payload CID.
func (c *Client) FindProviders(payloadCID cid.Cid) []retrievalmarket.RetrievalPeer {
	peers, err := c.resolver.GetPeers(payloadCID)
	if err != nil {
		log.Errorf("failed to get peers: %s", err)
		return []retrievalmarket.RetrievalPeer{}
	}
	return peers
}

/*
Query sends a retrieval query to a specific retrieval provider, to determine
if the provider can serve a retrieval request and what its specific parameters for
the request are.

The client creates a new `RetrievalQueryStream` for the chosen peer ID,
and calls `WriteQuery` on it, which constructs a data-transfer message and writes it to the Query stream.
*/
func (c *Client) Query(ctx context.Context, p retrievalmarket.RetrievalPeer, payloadCID cid.Cid, params retrievalmarket.QueryParams) (retrievalmarket.QueryResponse, error) {
	err := c.addMultiaddrs(ctx, p)
	if err != nil {
		log.Warn(err)
		return retrievalmarket.QueryResponseUndefined, err
	}
	s, err := c.network.NewQueryStream(p.ID)
	if err != nil {
		log.Warn(err)
		return retrievalmarket.QueryResponseUndefined, err
	}
	defer s.Close()

	err = s.WriteQuery(retrievalmarket.Query{
		PayloadCID:  payloadCID,
		QueryParams: params,
	})
	if err != nil {
		log.Warn(err)
		return retrievalmarket.QueryResponseUndefined, err
	}

	return s.ReadQueryResponse()
}

/*
Retrieve initiates the retrieval deal flow, which involves multiple requests and responses

To start this processes, the client creates a new `RetrievalDealStream`.  Currently, this connection is
kept open through the entire deal until completion or failure.  Make deals pauseable as well as surviving
a restart is a planned future feature.

Retrieve should be called after using FindProviders and Query are used to identify an appropriate provider to
retrieve the deal from. The parameters identified in Query should be passed to Retrieve to ensure the
greatest likelihood the provider will accept the deal

When called, the client takes the following actions:

1. Creates a deal ID using the next value from its `storedCounter`.

2. Constructs a `DealProposal` with deal terms

3. Tells its statemachine to begin tracking this deal state by dealID.

4. Constructs a `blockio.SelectorVerifier` and adds it to its dealID-keyed map of block verifiers.

5. Triggers a `ClientEventOpen` event on its statemachine.

From then on, the statemachine controls the deal flow in the client. Other components may listen for events in this flow by calling
`SubscribeToEvents` on the Client. The Client handles consuming blocks it receives from the provider, via `ConsumeBlocks` function

Documentation of the client state machine can be found at https://godoc.org/github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/clientstates
*/
func (c *Client) Retrieve(ctx context.Context, payloadCID cid.Cid, params retrievalmarket.Params, totalFunds abi.TokenAmount, p retrievalmarket.RetrievalPeer, clientWallet address.Address, minerWallet address.Address, storeID *multistore.StoreID) (retrievalmarket.DealID, error) {
	c.retrieveLk.Lock()
	defer c.retrieveLk.Unlock()

	// Check if there's already an active retrieval deal with the same peer
	// for the same payload CID
	err := c.checkForActiveDeal(payloadCID, p.ID)
	if err != nil {
		return 0, err
	}

	err = c.addMultiaddrs(ctx, p)
	if err != nil {
		return 0, err
	}

	// make sure the store is loadable
	if storeID != nil {
		_, err = c.multiStore.Get(*storeID)
		if err != nil {
			return 0, err
		}
	}

	next := c.dealIDGen.Next()
	dealID := retrievalmarket.DealID(next)
	dealState := retrievalmarket.ClientDealState{
		DealProposal: retrievalmarket.DealProposal{
			PayloadCID: payloadCID,
			ID:         dealID,
			Params:     params,
		},
		TotalFunds:       totalFunds,
		ClientWallet:     clientWallet,
		MinerWallet:      minerWallet,
		TotalReceived:    0,
		CurrentInterval:  params.PaymentInterval,
		BytesPaidFor:     0,
		PaymentRequested: abi.NewTokenAmount(0),
		FundsSpent:       abi.NewTokenAmount(0),
		Status:           retrievalmarket.DealStatusNew,
		Sender:           p.ID,
		UnsealFundsPaid:  big.Zero(),
		StoreID:          storeID,
	}

	// start the deal processing
	err = c.stateMachines.Begin(dealState.ID, &dealState)
	if err != nil {
		return 0, err
	}

	err = c.stateMachines.Send(dealState.ID, retrievalmarket.ClientEventOpen)
	if err != nil {
		return 0, err
	}

	c.checkEvents.Add(dealID, &checkEvent{
		start:  time.Now(),
		dealID: dealID,
	})

	return dealID, nil
}

// Check if there's already an active retrieval deal with the same peer
// for the same payload CID
func (c *Client) checkForActiveDeal(payloadCID cid.Cid, pid peer.ID) error {
	var deals []retrievalmarket.ClientDealState
	err := c.stateMachines.List(&deals)
	if err != nil {
		return err
	}

	for _, deal := range deals {
		match := deal.Sender == pid && deal.PayloadCID == payloadCID
		active := !clientstates.IsFinalityState(deal.Status)
		if match && active {
			msg := fmt.Sprintf("there is an active retrieval deal with peer %s ", pid)
			msg += fmt.Sprintf("for payload CID %s ", payloadCID)
			msg += fmt.Sprintf("(retrieval deal ID %d, state %s) - ",
				deal.ID, retrievalmarket.DealStatuses[deal.Status])
			msg += "existing deal must be cancelled before starting a new retrieval deal"
			err := xerrors.Errorf(msg)
			return err
		}
	}
	return nil
}

func (c *Client) notifySubscribers(eventName fsm.EventName, state fsm.StateType) {
	evt := eventName.(retrievalmarket.ClientEvent)
	ds := state.(retrievalmarket.ClientDealState)
	_ = c.subscribers.Publish(internalEvent{evt, ds})
}

func (c *Client) addMultiaddrs(ctx context.Context, p retrievalmarket.RetrievalPeer) error {
	tok, _, err := c.node.GetChainHead(ctx)
	if err != nil {
		return err
	}
	maddrs, err := c.node.GetKnownAddresses(ctx, p, tok)
	if err != nil {
		return err
	}
	if len(maddrs) > 0 {
		c.network.AddAddrs(p.ID, maddrs)
	}
	return nil
}

// SubscribeToEvents allows another component to listen for events on the RetrievalClient
// in order to track deals as they progress through the deal flow
func (c *Client) SubscribeToEvents(subscriber retrievalmarket.ClientSubscriber) retrievalmarket.Unsubscribe {
	return retrievalmarket.Unsubscribe(c.subscribers.Subscribe(subscriber))
}

// V1

// TryRestartInsufficientFunds attempts to restart any deals stuck in the insufficient funds state
// after funds are added to a given payment channel
func (c *Client) TryRestartInsufficientFunds(paymentChannel address.Address) error {
	var deals []retrievalmarket.ClientDealState
	err := c.stateMachines.List(&deals)
	if err != nil {
		return err
	}
	for _, deal := range deals {
		if deal.Status == retrievalmarket.DealStatusInsufficientFunds && deal.PaymentInfo.PayCh == paymentChannel {
			if err := c.stateMachines.Send(deal.ID, retrievalmarket.ClientEventRecheckFunds); err != nil {
				return err
			}
		}
	}
	return nil
}

// CancelDeal attempts to cancel an in progress deal
func (c *Client) CancelDeal(dealID retrievalmarket.DealID) error {
	return c.stateMachines.Send(dealID, retrievalmarket.ClientEventCancel)
}

// GetDeal returns a given deal by deal ID, if it exists
func (c *Client) GetDeal(dealID retrievalmarket.DealID) (retrievalmarket.ClientDealState, error) {
	var out retrievalmarket.ClientDealState
	if err := c.stateMachines.Get(dealID).Get(&out); err != nil {
		return retrievalmarket.ClientDealState{}, err
	}
	return out, nil
}

// ListDeals lists all known retrieval deals
func (c *Client) ListDeals() (map[retrievalmarket.DealID]retrievalmarket.ClientDealState, error) {
	var deals []retrievalmarket.ClientDealState
	err := c.stateMachines.List(&deals)
	if err != nil {
		return nil, err
	}
	dealMap := make(map[retrievalmarket.DealID]retrievalmarket.ClientDealState)
	for _, deal := range deals {
		dealMap[deal.ID] = deal
	}
	return dealMap, nil
}

var _ clientstates.ClientDealEnvironment = &clientDealEnvironment{}

type clientDealEnvironment struct {
	c *Client
}

// Node returns the node interface for this deal
func (c *clientDealEnvironment) Node() retrievalmarket.RetrievalClientNode {
	return c.c.node
}

func (c *clientDealEnvironment) OpenDataTransfer(ctx context.Context, to peer.ID, proposal *retrievalmarket.DealProposal, legacy bool) (datatransfer.ChannelID, error) {
	sel := shared.AllSelector()
	if proposal.SelectorSpecified() {
		var err error
		sel, err = retrievalmarket.DecodeNode(proposal.Selector)
		if err != nil {
			return datatransfer.ChannelID{}, xerrors.Errorf("selector is invalid: %w", err)
		}
	}

	var vouch datatransfer.Voucher = proposal
	if legacy {
		vouch = &migrations.DealProposal0{
			PayloadCID: proposal.PayloadCID,
			ID:         proposal.ID,
			Params0: migrations.Params0{
				Selector:                proposal.Selector,
				PieceCID:                proposal.PieceCID,
				PricePerByte:            proposal.PricePerByte,
				PaymentInterval:         proposal.PaymentInterval,
				PaymentIntervalIncrease: proposal.PaymentIntervalIncrease,
				UnsealPrice:             proposal.UnsealPrice,
			},
		}
	}
	return c.c.dataTransfer.OpenPullDataChannel(ctx, to, vouch, proposal.PayloadCID, sel)
}

func (c *clientDealEnvironment) SendDataTransferVoucher(ctx context.Context, channelID datatransfer.ChannelID, payment *retrievalmarket.DealPayment, legacy bool) error {
	var vouch datatransfer.Voucher = payment
	if legacy {
		vouch = &migrations.DealPayment0{
			ID:             payment.ID,
			PaymentChannel: payment.PaymentChannel,
			PaymentVoucher: payment.PaymentVoucher,
		}
	}
	return c.c.dataTransfer.SendVoucher(ctx, channelID, vouch)
}

func (c *clientDealEnvironment) CloseDataTransfer(ctx context.Context, channelID datatransfer.ChannelID) error {
	// When we close the data transfer, we also send a cancel message to the peer.
	// Make sure we don't wait too long to send the message.
	ctx, cancel := context.WithTimeout(ctx, shared.CloseDataTransferTimeout)
	defer cancel()

	err := c.c.dataTransfer.CloseDataTransferChannel(ctx, channelID)
	if shared.IsCtxDone(err) {
		log.Warnf("failed to send cancel data transfer channel %s to provider within timeout %s",
			channelID, shared.CloseDataTransferTimeout)
		return nil
	}
	return err
}

type clientStoreGetter struct {
	c *Client
}

func (csg *clientStoreGetter) Get(otherPeer peer.ID, dealID retrievalmarket.DealID) (*multistore.Store, error) {
	var deal retrievalmarket.ClientDealState
	err := csg.c.stateMachines.Get(dealID).Get(&deal)
	if err != nil {
		return nil, err
	}
	if deal.StoreID == nil {
		return nil, nil
	}
	return csg.c.multiStore.Get(*deal.StoreID)
}

// ClientFSMParameterSpec is a valid set of parameters for a client deal FSM - used in doc generation
var ClientFSMParameterSpec = fsm.Parameters{
	Environment:     &clientDealEnvironment{},
	StateType:       retrievalmarket.ClientDealState{},
	StateKeyField:   "Status",
	Events:          clientstates.ClientEvents,
	StateEntryFuncs: clientstates.ClientStateEntryFuncs,
}
