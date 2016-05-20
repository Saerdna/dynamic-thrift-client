package client

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
)

type DynamicClient struct {
	Transport       thrift.TTransport
	ProtocolFactory thrift.TProtocolFactory
	InputProtocol   thrift.TProtocol
	OutputProtocol  thrift.TProtocol
	SeqId           int32

	Idl *Dynamic
}

func NewDynamicClientFactory(t thrift.TTransport, f thrift.TProtocolFactory, idlPath string) (c *DynamicClient, err error) {
	c = &DynamicClient{Transport: t,
		ProtocolFactory: f,
		InputProtocol:   f.GetProtocol(t),
		OutputProtocol:  f.GetProtocol(t),
		SeqId:           0,
	}
	c.Idl, err = NewDynamic(idlPath)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func NewDynamicClientProtocol(t thrift.TTransport, iprot thrift.TProtocol, oprot thrift.TProtocol, idlPath string) (c *DynamicClient, err error) {
	c = &DynamicClient{Transport: t,
		ProtocolFactory: nil,
		InputProtocol:   iprot,
		OutputProtocol:  oprot,
		SeqId:           0,
	}
	c.Idl, err = NewDynamic(idlPath)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (p *DynamicClient) CallApi(service string, api string, args map[string]interface{}) (r map[string]interface{}, err error) {
	if err = p.send(service, api, args); err != nil {
		return
	}
	return p.recv(service, api)
}

func (p *DynamicClient) send(service, api string, args map[string]interface{}) (err error) {
	oprot := p.OutputProtocol
	if oprot == nil {
		oprot = p.ProtocolFactory.GetProtocol(p.Transport)
		p.OutputProtocol = oprot
	}
	p.SeqId++
	if err = oprot.WriteMessageBegin(api, thrift.CALL, p.SeqId); err != nil {
		return
	}
	if err = p.Idl.Write(service, api, oprot, args); err != nil {
		return
	}
	if err = oprot.WriteMessageEnd(); err != nil {
		return
	}
	return oprot.Flush()
}

func (p *DynamicClient) recv(service string, api string) (value map[string]interface{}, err error) {
	iprot := p.InputProtocol
	if iprot == nil {
		iprot = p.ProtocolFactory.GetProtocol(p.Transport)
		p.InputProtocol = iprot
	}
	_, mTypeId, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return
	}
	if mTypeId == thrift.EXCEPTION {
		error3 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
		var error4 error
		error4, err = error3.Read(iprot)
		if err != nil {
			return
		}
		if err = iprot.ReadMessageEnd(); err != nil {
			return
		}
		err = error4
		return
	}
	if p.SeqId != seqId {
		err = thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, fmt.Sprintf("%s failed: out of sequence response", api))
		return
	}
	value = make(map[string]interface{})
	if err = p.Idl.Read(service, api, iprot, value); err != nil {
		return
	}
	if err = iprot.ReadMessageEnd(); err != nil {
		return
	}
	return
}
