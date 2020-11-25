package network

import (
	"bufio"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/libp2p/go-libp2p-core/mux"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
)

type DealStream struct {
	p        peer.ID
	rw       mux.MuxedStream
	buffered *bufio.Reader
	reporter Reporter
}

var _ RetrievalDealStream = (*DealStream)(nil)

func (d *DealStream) ReadDealProposal() (retrievalmarket.DealProposal, error) {
	var ds retrievalmarket.DealProposal

	cr := NewCountReader(d.buffered)
	if d.reporter != nil {
		defer d.reporter("retrieval", "inbound", cr.Count())
	}
	if err := ds.UnmarshalCBOR(cr); err != nil {
		log.Warn(err)
		return retrievalmarket.DealProposalUndefined, err
	}
	return ds, nil
}

func (d *DealStream) WriteDealProposal(dp retrievalmarket.DealProposal) error {
	cw := NewCountWriter(d.rw)
	if d.reporter != nil {
		defer d.reporter("retrieval", "outbound", cw.Count())
	}
	return cborutil.WriteCborRPC(cw, &dp)
}

func (d *DealStream) ReadDealResponse() (retrievalmarket.DealResponse, error) {
	var dr retrievalmarket.DealResponse

	cr := NewCountReader(d.buffered)
	if d.reporter != nil {
		defer d.reporter("retrieval", "inbound", cr.Count())
	}
	if err := dr.UnmarshalCBOR(cr); err != nil {
		return retrievalmarket.DealResponseUndefined, err
	}
	return dr, nil
}

func (d *DealStream) WriteDealResponse(dr retrievalmarket.DealResponse) error {
	cw := NewCountWriter(d.rw)
	if d.reporter != nil {
		defer d.reporter("retrieval", "outbound", cw.Count())
	}
	return cborutil.WriteCborRPC(cw, &dr)
}

func (d *DealStream) ReadDealPayment() (retrievalmarket.DealPayment, error) {
	var ds retrievalmarket.DealPayment

	cr := NewCountReader(d.rw)
	if d.reporter != nil {
		defer d.reporter("retrieval", "inbound", cr.Count())
	}
	if err := ds.UnmarshalCBOR(cr); err != nil {
		return retrievalmarket.DealPaymentUndefined, err
	}
	return ds, nil
}

func (d *DealStream) WriteDealPayment(dpy retrievalmarket.DealPayment) error {
	cw := NewCountWriter(d.rw)
	if d.reporter != nil {
		defer d.reporter("retrieval", "outbound", cw.Count())
	}
	return cborutil.WriteCborRPC(cw, &dpy)
}

func (d *DealStream) Receiver() peer.ID {
	return d.p
}

func (d *DealStream) Close() error {
	return d.rw.Close()
}
