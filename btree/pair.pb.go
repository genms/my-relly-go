// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.17.1
// source: pair.proto

package btree

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Pair struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   []byte `protobuf:"bytes,1,opt,name=Key,proto3" json:"Key,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=Value,proto3" json:"Value,omitempty"`
}

func (x *Pair) Reset() {
	*x = Pair{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pair_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Pair) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Pair) ProtoMessage() {}

func (x *Pair) ProtoReflect() protoreflect.Message {
	mi := &file_pair_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Pair.ProtoReflect.Descriptor instead.
func (*Pair) Descriptor() ([]byte, []int) {
	return file_pair_proto_rawDescGZIP(), []int{0}
}

func (x *Pair) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *Pair) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

var File_pair_proto protoreflect.FileDescriptor

var file_pair_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x70, 0x61, 0x69, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x62, 0x74,
	0x72, 0x65, 0x65, 0x22, 0x2e, 0x0a, 0x04, 0x50, 0x61, 0x69, 0x72, 0x12, 0x10, 0x0a, 0x03, 0x4b,
	0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x4b, 0x65, 0x79, 0x12, 0x14, 0x0a,
	0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x56, 0x61,
	0x6c, 0x75, 0x65, 0x42, 0x0a, 0x5a, 0x08, 0x2e, 0x2f, 0x3b, 0x62, 0x74, 0x72, 0x65, 0x65, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pair_proto_rawDescOnce sync.Once
	file_pair_proto_rawDescData = file_pair_proto_rawDesc
)

func file_pair_proto_rawDescGZIP() []byte {
	file_pair_proto_rawDescOnce.Do(func() {
		file_pair_proto_rawDescData = protoimpl.X.CompressGZIP(file_pair_proto_rawDescData)
	})
	return file_pair_proto_rawDescData
}

var file_pair_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_pair_proto_goTypes = []interface{}{
	(*Pair)(nil), // 0: btree.Pair
}
var file_pair_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_pair_proto_init() }
func file_pair_proto_init() {
	if File_pair_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pair_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Pair); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pair_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pair_proto_goTypes,
		DependencyIndexes: file_pair_proto_depIdxs,
		MessageInfos:      file_pair_proto_msgTypes,
	}.Build()
	File_pair_proto = out.File
	file_pair_proto_rawDesc = nil
	file_pair_proto_goTypes = nil
	file_pair_proto_depIdxs = nil
}
