package client

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/samuel/go-thrift/parser"
	"reflect"
	"strings"
)

type Dynamic struct {
	FilePath   string
	ThriftIDLs map[string]*parser.Thrift
}

func NewDynamic(filepath string) (c *Dynamic, err error) {
	p := parser.Parser{}
	c = &Dynamic{}
	c.ThriftIDLs, c.FilePath, err = p.ParseFile(filepath)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (s *Dynamic) getThriftType(t string) thrift.TType {
	switch strings.ToUpper(t) {
	case "BOOL":
		return thrift.BOOL
	case "BYTE":
		return thrift.BYTE
	case "I16":
		return thrift.I16
	case "I32":
		return thrift.I32
	case "I64":
		return thrift.I64
	case "STRING":
		return thrift.STRING
	case "MAP":
		return thrift.MAP
	case "SET":
		return thrift.SET
	case "LIST":
		return thrift.LIST
	case "STRUCT":
		return thrift.STRUCT
	default:
		if _, ok := s.ThriftIDLs[s.FilePath].Structs[t]; ok {
			return thrift.STRUCT
		}
		if _, ok := s.ThriftIDLs[s.FilePath].Enums[t]; ok {
			return thrift.I32
		}
		return thrift.STOP
	}
}

func (s *Dynamic) Write(service string, api string, oprot thrift.TProtocol, args map[string]interface{}) (err error) {
	serviceItem, ok := s.ThriftIDLs[s.FilePath].Services[service]
	if !ok {
		return fmt.Errorf("service:%s not found in thrift idl:%s", service, s.FilePath)
	}
	method, ok := serviceItem.Methods[api]
	if !ok {
		return fmt.Errorf("method:%s not found in service:%s, thrift idl:%s", api, service, s.FilePath)
	}
	if err = oprot.WriteStructBegin(fmt.Sprintf("%s_args", api)); err != nil {
		return fmt.Errorf("%T write struct begin error: %s", s, err)
	}
	for i := 0; i < len(method.Arguments); i++ {
		// to do check default value
		if _, exists := args[method.Arguments[i].Name]; !exists && method.Arguments[i].Optional == false {
			return fmt.Errorf("%s not exists in %s method's Arguments", method.Arguments[i].Name, method.Name)
		}
		if err = s.writeFields(method.Arguments[i], oprot, args[method.Arguments[i].Name]); err != nil {
			return
		}
	}
	if err = oprot.WriteFieldStop(); err != nil {
		return fmt.Errorf("write field stop error: %s", err)
	}
	if err := oprot.WriteStructEnd(); err != nil {
		return fmt.Errorf("write struct stop error: %s", err)
	}
	return nil
}

func (s *Dynamic) writeStruct(Struct *parser.Struct, oprot thrift.TProtocol, input interface{}) (err error) {
	arg, ok := input.(map[string]interface{})
	if !ok {
		return fmt.Errorf("struct error")
	}
	if err = oprot.WriteStructBegin(Struct.Name); err != nil {
		return fmt.Errorf("write struct:%s begin error: %s", Struct.Name, err)
	}
	for i := 0; i < len(Struct.Fields); i++ {
		if err = s.writeFields(Struct.Fields[i], oprot, arg[Struct.Fields[i].Name]); err != nil {
			return err
		}
	}
	if err = oprot.WriteFieldStop(); err != nil {
		return fmt.Errorf("write field stop error: %s", err)
	}
	if err = oprot.WriteStructEnd(); err != nil {
		return fmt.Errorf("write struct stop error: %s", err)
	}
	return nil
}

func (s *Dynamic) writeBaseType(valueTType thrift.TType, oprot thrift.TProtocol, arg interface{}) (err error) {
	switch valueTType {
	case thrift.BOOL:
		if err = oprot.WriteBool(reflect.ValueOf(arg).Bool()); err != nil {
			return fmt.Errorf("write bool value error:%s", err)
		}
	case thrift.I16:
		if err = oprot.WriteI16(int16(reflect.ValueOf(arg).Int())); err != nil {
			return fmt.Errorf("write i16 value error:%s", err)
		}
	case thrift.I32:
		if err = oprot.WriteI32(int32(reflect.ValueOf(arg).Int())); err != nil {
			return fmt.Errorf("write int32 value error:%s", err)
		}
	case thrift.I64:
		if err = oprot.WriteI64(int64(reflect.ValueOf(arg).Int())); err != nil {
			return fmt.Errorf("write int64 value error:%s", err)
		}
	case thrift.STRING:
		if err = oprot.WriteString(string(reflect.ValueOf(arg).String())); err != nil {
			return fmt.Errorf("write string value error:%s", err)
		}
	case thrift.DOUBLE:
		if err = oprot.WriteDouble(float64(reflect.ValueOf(arg).Float())); err != nil {
			return fmt.Errorf("write double value error: %s", err)
		}
	case thrift.BYTE:
		if err = oprot.WriteByte(byte(reflect.ValueOf(arg).Int())); err != nil {
			return fmt.Errorf("write byte value error: %s", err)
		}
	default:
		return fmt.Errorf("unsupport value type:%d", valueTType)
	}
	return nil
}

func (s *Dynamic) writeStringMap(f *parser.Field, oprot thrift.TProtocol, arg interface{}) (err error) {
	m, ok := arg.(map[string]interface{})
	if !ok {
		return fmt.Errorf("map bad format (%+v)", arg)
	}
	valueTType := s.getThriftType(f.Type.ValueType.Name)
	if valueTType == thrift.STOP {
		return fmt.Errorf("map value type unknonw, %d:%s:%s", f.ID, f.Name, f.Type.ValueType.Name)
	}
	if err = oprot.WriteMapBegin(thrift.STRING, valueTType, len(m)); err != nil {
		return fmt.Errorf("error writing map begin: %s", err)
	}
	for k, v := range m {
		if err = oprot.WriteString(string(k)); err != nil {
			return fmt.Errorf("map:%d:%s key write error: %s", f.ID, f.Name, err)
		}
		if err = s.writeBaseType(valueTType, oprot, v); err != nil {
			return err
		}
	}
	return nil
}
func (s *Dynamic) WriteI32Map(f *parser.Field, oprot thrift.TProtocol, arg interface{}) (err error) {
	return fmt.Errorf("unsupport map<int32>")
}
func (s *Dynamic) writeMap(f *parser.Field, oprot thrift.TProtocol, arg interface{}) (err error) {
	keyTType := s.getThriftType(f.Type.KeyType.Name)
	switch keyTType {
	case thrift.STRING:
		if err = s.writeStringMap(f, oprot, arg); err != nil {
			return err
		}
	case thrift.I32:
		if err = s.WriteI32Map(f, oprot, arg); err != nil {
			return err
		}
	}
	if err := oprot.WriteMapEnd(); err != nil {
		return fmt.Errorf("error writing map end: %s", err)
	}
	return nil
}

func (s *Dynamic) writeList(f *parser.Field, oprot thrift.TProtocol, arg interface{}) (err error) {
	valueTType := s.getThriftType(f.Type.ValueType.Name)
	if err := oprot.WriteListBegin(valueTType, reflect.ValueOf(arg).Len()); err != nil {
		return fmt.Errorf("error writing list begin: %s", err)
	}
	for i := 0; i < reflect.ValueOf(arg).Len(); i++ {
		if valueTType == thrift.STRUCT {
			if err = s.writeStruct(s.ThriftIDLs[s.FilePath].Structs[f.Type.ValueType.Name], oprot, reflect.ValueOf(arg).Index(i)); err != nil {
				return fmt.Errorf("write map struct value error:%s", err)
			}
		} else {
			if err = s.writeBaseType(valueTType, oprot, reflect.ValueOf(arg).Index(i).Interface()); err != nil {
				return err
			}
		}
	}
	if err := oprot.WriteListEnd(); err != nil {
		return fmt.Errorf("error writing list end: %s", err)
	}
	return nil
}

func (s *Dynamic) writeSet(f *parser.Field, oprot thrift.TProtocol, arg interface{}) (err error) {
	return nil
}

// byte has some problem, reflect get byte Type is uint8, thrift is int8
// double has problem, too
// binary also has problem too, reflect get Type nothing...
func (s *Dynamic) writeFields(f *parser.Field, oprot thrift.TProtocol, arg interface{}) (err error) {
	if arg == nil {
		if f.Optional == true {
			return nil
		} else {
			return fmt.Errorf("field is required, %d:%s", f.ID, f.Name)
		}
	}
	/* check type
	fType := strings.ToUpper(f.Type.Name)
	if strings.ToUpper(reflect.TypeOf(arg).Name()) != fType {
		return fmt.Errorf("field:%s type should %s not %s", f.Name, fType, reflect.TypeOf(arg).Name())
	}
	*/
	fTType := s.getThriftType(f.Type.Name)
	if err = oprot.WriteFieldBegin(f.Name, fTType, int16(f.ID)); err != nil {
		return fmt.Errorf("write field begin error %d:%s: %s", s, f.ID, f.Name, err)
	}
	if fTType == thrift.MAP {
		if err = s.writeMap(f, oprot, arg); err != nil {
			return err
		}
	} else if fTType == thrift.SET {
		if err = s.writeSet(f, oprot, arg); err != nil {
			return err
		}
	} else if fTType == thrift.LIST {
		if err = s.writeList(f, oprot, arg); err != nil {
			return err
		}
	} else if fTType == thrift.STRUCT {
		if err = s.writeStruct(s.ThriftIDLs[s.FilePath].Structs[f.Type.Name], oprot, arg); err != nil {
			return err
		}
	} else if err = s.writeBaseType(fTType, oprot, arg); err != nil {
		return err
	}
	if err = oprot.WriteFieldEnd(); err != nil {
		return fmt.Errorf("write field end error %d:%s: %s", f.ID, f.Name, err)
	}
	return nil
}
func (s *Dynamic) readBaseType(ttype *parser.Type, iprot thrift.TProtocol) (v interface{}, err error) {
	rtnTType := s.getThriftType(ttype.Name)
	switch rtnTType {
	case thrift.BOOL:
		return iprot.ReadBool()
	case thrift.BYTE:
		return iprot.ReadByte()
	case thrift.I16:
		return iprot.ReadI16()
	case thrift.I32:
		return iprot.ReadI32()
	case thrift.I64:
		return iprot.ReadI64()
	case thrift.STRING:
		return iprot.ReadString()
	case thrift.DOUBLE:
		return iprot.ReadDouble()
	case thrift.MAP:
	case thrift.LIST:
	case thrift.SET:
	case thrift.STRUCT:
	default:
		return nil, fmt.Errorf("unsupport type")
	}
	return nil, nil
}
func (s *Dynamic) Read(service string, api string, iprot thrift.TProtocol, value map[string]interface{}) (err error) {
	serviceItem, ok := s.ThriftIDLs[s.FilePath].Services[service]
	if !ok {
		return fmt.Errorf("service:%s not found in thrift idl:%s", service, s.FilePath)
	}
	method, ok := serviceItem.Methods[api]
	if !ok {
		return fmt.Errorf("method:%s not found in service:%s, thrift idl:%s", api, service, s.FilePath)
	}
	if _, err := iprot.ReadStructBegin(); err != nil {
		return fmt.Errorf("service:%s api:%s read error: %s", service, api, err)
	}
	var v interface{}
	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return fmt.Errorf("field %d read error: %s", fieldId, err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		if v, err = s.readBaseType(method.ReturnType, iprot); err != nil {
			return err
		}
		value[fmt.Sprintf("%s_result", method.Name)] = v
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return fmt.Errorf("service:%s api:%s read struct end error: %s", service, api, err)
	}
	return nil
}
