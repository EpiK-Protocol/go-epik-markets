package retrievalimpl

import (
	"context"
	"errors"
	"time"

	"github.com/hannahhoward/go-pubsub"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	versionedfsm "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
	"github.com/filecoin-project/go-multistore"
	"github.com/filecoin-project/go-statemachine/fsm"

	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/askstore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/dtutils"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/providerstates"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/requestvalidation"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/migrations"
	rmnet "github.com/filecoin-project/go-fil-markets/retrievalmarket/network"
	"github.com/filecoin-project/go-fil-markets/shared"
)

// RetrievalProviderOption is a function that configures a retrieval provider
type RetrievalProviderOption func(p *Provider)

// DealDecider is a function that makes a decision about whether to accept a deal
type DealDecider func(ctx context.Context, state retrievalmarket.ProviderDealState) (bool, string, error)

// Provider is the production implementation of the RetrievalProvider interface
type Provider struct {
	multiStore           *multistore.MultiStore
	dataTransfer         datatransfer.Manager
	node                 retrievalmarket.RetrievalProviderNode
	network              rmnet.RetrievalMarketNetwork
	requestValidator     *requestvalidation.ProviderRequestValidator
	revalidator          *requestvalidation.ProviderRevalidator
	minerAddress         address.Address
	pieceStore           piecestore.PieceStore
	readySub             *pubsub.PubSub
	subscribers          *pubsub.PubSub
	stateMachines        fsm.Group
	migrateStateMachines func(context.Context) error
	dealDecider          DealDecider
	askStore             retrievalmarket.AskStore
	disableNewDeals      bool
	checkEvents          *lru.ARCCache
	validateTime         time.Time
}

type internalProviderEvent struct {
	evt   retrievalmarket.ProviderEvent
	state retrievalmarket.ProviderDealState
}

type checkProviderEvent struct {
	start      time.Time
	identifier retrievalmarket.ProviderDealIdentifier
}

func providerDispatcher(evt pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	ie, ok := evt.(internalProviderEvent)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb, ok := subscriberFn.(retrievalmarket.ProviderSubscriber)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb(ie.evt, ie.state)
	return nil
}

var _ retrievalmarket.RetrievalProvider = new(Provider)

// DealDeciderOpt sets a custom protocol
func DealDeciderOpt(dd DealDecider) RetrievalProviderOption {
	return func(provider *Provider) {
		provider.dealDecider = dd
	}
}

// DisableNewDeals disables setup for v1 deal protocols
func DisableNewDeals() RetrievalProviderOption {
	return func(provider *Provider) {
		provider.disableNewDeals = true
	}
}

// NewProvider returns a new retrieval Provider
func NewProvider(minerAddress address.Address,
	node retrievalmarket.RetrievalProviderNode,
	network rmnet.RetrievalMarketNetwork,
	pieceStore piecestore.PieceStore,
	multiStore *multistore.MultiStore,
	dataTransfer datatransfer.Manager,
	ds datastore.Batching,
	opts ...RetrievalProviderOption,
) (retrievalmarket.RetrievalProvider, error) {

	checkEvents, cerr := lru.NewARC(2 * retrievalmarket.MaxRetrieveParallelNum)
	if cerr != nil {
		return nil, cerr
	}

	p := &Provider{
		multiStore:   multiStore,
		dataTransfer: dataTransfer,
		node:         node,
		network:      network,
		minerAddress: minerAddress,
		pieceStore:   pieceStore,
		subscribers:  pubsub.New(providerDispatcher),
		readySub:     pubsub.New(shared.ReadyDispatcher),
		checkEvents:  checkEvents,
		validateTime: time.Now().Add(5 * time.Minute),
	}

	err := shared.MoveKey(ds, "retrieval-ask", "retrieval-ask/latest")
	if err != nil {
		return nil, err
	}
	askStore, err := askstore.NewAskStore(namespace.Wrap(ds, datastore.NewKey("retrieval-ask")), datastore.NewKey("latest"))
	if err != nil {
		return nil, err
	}
	p.askStore = askStore

	retrievalMigrations, err := migrations.ProviderMigrations.Build()
	if err != nil {
		return nil, err
	}
	p.stateMachines, p.migrateStateMachines, err = versionedfsm.NewVersionedFSM(ds, fsm.Parameters{
		Environment:     &providerDealEnvironment{p},
		StateType:       retrievalmarket.ProviderDealState{},
		StateKeyField:   "Status",
		Events:          providerstates.ProviderEvents,
		StateEntryFuncs: providerstates.ProviderStateEntryFuncs,
		FinalityStates:  providerstates.ProviderFinalityStates,
		Notifier:        p.notifySubscribers,
	}, retrievalMigrations, versioning.VersionKey("1"))
	if err != nil {
		return nil, err
	}
	p.Configure(opts...)
	p.requestValidator = requestvalidation.NewProviderRequestValidator(&providerValidationEnvironment{p})
	transportConfigurer := dtutils.TransportConfigurer(network.ID(), &providerStoreGetter{p})
	p.revalidator = requestvalidation.NewProviderRevalidator(&providerRevalidatorEnvironment{p})

	if p.disableNewDeals {
		err = p.dataTransfer.RegisterVoucherType(&migrations.DealProposal0{}, p.requestValidator)
		if err != nil {
			return nil, err
		}
		err = p.dataTransfer.RegisterRevalidator(&migrations.DealPayment0{}, p.revalidator)
		if err != nil {
			return nil, err
		}
	} else {
		err = p.dataTransfer.RegisterVoucherType(&retrievalmarket.DealProposal{}, p.requestValidator)
		if err != nil {
			return nil, err
		}
		err = p.dataTransfer.RegisterVoucherType(&migrations.DealProposal0{}, p.requestValidator)
		if err != nil {
			return nil, err
		}

		err = p.dataTransfer.RegisterRevalidator(&retrievalmarket.DealPayment{}, p.revalidator)
		if err != nil {
			return nil, err
		}
		err = p.dataTransfer.RegisterRevalidator(&migrations.DealPayment0{}, requestvalidation.NewLegacyRevalidator(p.revalidator))
		if err != nil {
			return nil, err
		}

		err = p.dataTransfer.RegisterVoucherResultType(&retrievalmarket.DealResponse{})
		if err != nil {
			return nil, err
		}

		err = p.dataTransfer.RegisterTransportConfigurer(&retrievalmarket.DealProposal{}, transportConfigurer)
		if err != nil {
			return nil, err
		}
	}
	err = p.dataTransfer.RegisterVoucherResultType(&migrations.DealResponse0{})
	if err != nil {
		return nil, err
	}
	err = p.dataTransfer.RegisterTransportConfigurer(&migrations.DealProposal0{}, transportConfigurer)
	if err != nil {
		return nil, err
	}
	dataTransfer.SubscribeToEvents(dtutils.ProviderDataTransferSubscriber(p.stateMachines))
	return p, nil
}

// Stop stops handling incoming requests.
func (p *Provider) Stop() error {
	return p.network.StopHandlingRequests()
}

// Start begins listening for deals on the given host.
// Start must be called in order to accept incoming deals.
func (p *Provider) Start(ctx context.Context) error {
	go func() {
		err := p.migrateStateMachines(ctx)
		if err != nil {
			log.Errorf("Migrating retrieval provider state machines: %s", err.Error())
		}
		if err := p.restartDeals(ctx); err != nil {
			log.Errorf("Failed to restart retrieve provider deals: %w", err)
		}
		err = p.readySub.Publish(err)
		if err != nil {
			log.Warnf("Publish retrieval provider ready event: %s", err.Error())
		}

		go p.loop(ctx)
	}()
	return p.network.SetDelegate(p)
}

func (p *Provider) restartDeals(ctx context.Context) error {
	deals := p.ListDeals()
	for _, deal := range deals {
		if p.stateMachines.IsTerminated(deal) {
			continue
		}

		err := p.stateMachines.Send(deal.Identifier(), retrievalmarket.ProviderEventClientCancelled)
		if err != nil {
			return err
		}
		log.Warnf("change retrieve provider deal status when restart: %v", deal.Identifier())
	}
	return nil
}

func (p *Provider) loop(ctx context.Context) error {
	for {

		ebt := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			ebt.Stop()
			return nil
		case <-ebt.C:
			p.checkTimeOut()
		}
	}
}

func (p *Provider) checkTimeOut() error {
	keys := p.checkEvents.Keys()
	// log.Warnf("retrievel provider check timeout: %d", len(keys))
	for _, rk := range keys {
		v, _ := p.checkEvents.Get(rk)
		event := v.(*checkProviderEvent)
		var deal retrievalmarket.ProviderDealState
		err := p.stateMachines.GetSync(context.TODO(), event.identifier, &deal)
		if err != nil {
			return err
		}

		if p.stateMachines.IsTerminated(deal) {
			p.checkEvents.Remove(rk)
			log.Warnf("retrievel provider timeout check remove check events: %s, status:%d", rk, deal.Status)
		} else {
			if time.Now().Sub(event.start) > 35*time.Minute {
				err := p.stateMachines.Send(deal.Identifier(), retrievalmarket.ProviderEventClientCancelled)
				if err != nil {
					return err
				}
				p.checkEvents.Remove(rk)
				log.Warnf("retrievel provider timeout check events: %s, status:%d", rk, deal.Status)
			}
		}
	}
	return nil
}

// OnReady registers a listener for when the provider has finished starting up
func (p *Provider) OnReady(ready shared.ReadyFunc) {
	p.readySub.Subscribe(ready)
}

func (p *Provider) notifySubscribers(eventName fsm.EventName, state fsm.StateType) {
	evt := eventName.(retrievalmarket.ProviderEvent)
	ds := state.(retrievalmarket.ProviderDealState)
	_ = p.subscribers.Publish(internalProviderEvent{evt, ds})
}

// SubscribeToEvents listens for events that happen related to client retrievals
func (p *Provider) SubscribeToEvents(subscriber retrievalmarket.ProviderSubscriber) retrievalmarket.Unsubscribe {
	return retrievalmarket.Unsubscribe(p.subscribers.Subscribe(subscriber))
}

// GetAsk returns the current deal parameters this provider accepts
func (p *Provider) GetAsk() *retrievalmarket.Ask {
	return p.askStore.GetAsk()
}

// SetAsk sets the deal parameters this provider accepts
func (p *Provider) SetAsk(ask *retrievalmarket.Ask) {
	err := p.askStore.SetAsk(ask)

	if err != nil {
		log.Warnf("Error setting retrieval ask: %w", err)
	}
}

// ListDeals lists all known retrieval deals
func (p *Provider) ListDeals() map[retrievalmarket.ProviderDealIdentifier]retrievalmarket.ProviderDealState {
	var deals []retrievalmarket.ProviderDealState
	_ = p.stateMachines.List(&deals)
	dealMap := make(map[retrievalmarket.ProviderDealIdentifier]retrievalmarket.ProviderDealState)
	for _, deal := range deals {
		// log.Warnf("provider list retrieve deals: %v, %v, %v", deal.ID, deal.Receiver, deal.Status)
		dealMap[retrievalmarket.ProviderDealIdentifier{Receiver: deal.Receiver, DealID: deal.ID}] = deal
	}
	return dealMap
}

/*
HandleQueryStream is called by the network implementation whenever a new message is received on the query protocol

A Provider handling a retrieval `Query` does the following:

1. Get the node's chain head in order to get its miner worker address.

2. Look in its piece store to determine if it can serve the given payload CID.

3. Combine these results with its existing parameters for retrieval deals to construct a `retrievalmarket.QueryResponse` struct.

4. Writes this response to the `Query` stream.

The connection is kept open only as long as the query-response exchange.
*/
func (p *Provider) HandleQueryStream(stream rmnet.RetrievalQueryStream) {
	defer stream.Close()
	query, err := stream.ReadQuery()
	if err != nil {
		return
	}

	ask := p.GetAsk()

	answer := retrievalmarket.QueryResponse{
		Status:                     retrievalmarket.QueryResponseUnavailable,
		PieceCIDFound:              retrievalmarket.QueryItemUnavailable,
		MinPricePerByte:            ask.PricePerByte,
		MaxPaymentInterval:         ask.PaymentInterval,
		MaxPaymentIntervalIncrease: ask.PaymentIntervalIncrease,
		UnsealPrice:                ask.UnsealPrice,
	}

	ctx := context.TODO()

	tok, _, err := p.node.GetChainHead(ctx)
	if err != nil {
		log.Errorf("Retrieval query: GetChainHead: %s", err)
		return
	}

	paymentAddress, err := p.node.GetMinerWorkerAddress(ctx, p.minerAddress, tok)
	if err != nil {
		log.Errorf("Retrieval query: Lookup Payment Address: %s", err)
		answer.Status = retrievalmarket.QueryResponseError
		answer.Message = err.Error()
	} else {
		answer.PaymentAddress = paymentAddress

		pieceCID := cid.Undef
		if query.PieceCID != nil {
			pieceCID = *query.PieceCID
		}
		pieceInfo, err := getPieceInfoFromCid(p.pieceStore, query.PayloadCID, pieceCID)

		if err == nil && len(pieceInfo.Deals) > 0 {
			answer.Status = retrievalmarket.QueryResponseAvailable
			// TODO: get price, look for already unsealed ref to reduce work
			answer.Size = uint64(pieceInfo.Deals[0].Length) // TODO: verify on intermediate
			answer.PieceCIDFound = retrievalmarket.QueryItemAvailable
		}

		if err != nil && !xerrors.Is(err, retrievalmarket.ErrNotFound) {
			log.Errorf("Retrieval query: GetRefs: %s", err)
			answer.Status = retrievalmarket.QueryResponseError
			answer.Message = err.Error()
		}
	}

	if time.Now().Before(p.validateTime) {
		// log.Warnf("Retrieval query: retrieve not available.")
		answer.Status = retrievalmarket.QueryResponseUnavailable
		answer.Message = "retrieve not available"
	} else {
		// log.Infof("Retrieval query: parallel number: %d", p.checkEvents.Len())
		if p.checkEvents.Len() > retrievalmarket.MaxRetrieveParallelNum {
			log.Warnf("Retrieval query: out of max parallel number: %d", p.checkEvents.Len())
			answer.Status = retrievalmarket.QueryResponseUnavailable
			answer.Message = "out of max parallel number"
		}
	}

	if err := stream.WriteQueryResponse(answer); err != nil {
		log.Errorf("Retrieval query: WriteCborRPC: %s", err)
		return
	}
}

// Configure reconfigures a provider after initialization
func (p *Provider) Configure(opts ...RetrievalProviderOption) {
	for _, opt := range opts {
		opt(p)
	}
}

// ProviderFSMParameterSpec is a valid set of parameters for a provider FSM - used in doc generation
var ProviderFSMParameterSpec = fsm.Parameters{
	Environment:     &providerDealEnvironment{},
	StateType:       retrievalmarket.ProviderDealState{},
	StateKeyField:   "Status",
	Events:          providerstates.ProviderEvents,
	StateEntryFuncs: providerstates.ProviderStateEntryFuncs,
}
