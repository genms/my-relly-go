// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.17.1
// source: meta.proto

package table

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

type Meta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Version          int32    `protobuf:"varint,1,opt,name=Version,proto3" json:"Version,omitempty"`
	NumCols          int32    `protobuf:"varint,2,opt,name=NumCols,proto3" json:"NumCols,omitempty"`
	NumKeyElems      int32    `protobuf:"varint,3,opt,name=NumKeyElems,proto3" json:"NumKeyElems,omitempty"`
	ColNames         []string `protobuf:"bytes,4,rep,name=ColNames,proto3" json:"ColNames,omitempty"`
	UniqueIndicesStr []string `protobuf:"bytes,5,rep,name=UniqueIndicesStr,proto3" json:"UniqueIndicesStr,omitempty"`
}

func (x *Meta) Reset() {
	*x = Meta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_meta_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Meta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Meta) ProtoMessage() {}

func (x *Meta) ProtoReflect() protoreflect.Message {
	mi := &file_meta_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Meta.ProtoReflect.Descriptor instead.
func (*Meta) Descriptor() ([]byte, []int) {
	return file_meta_proto_rawDescGZIP(), []int{0}
}

func (x *Meta) GetVersion() int32 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *Meta) GetNumCols() int32 {
	if x != nil {
		return x.NumCols
	}
	return 0
}

func (x *Meta) GetNumKeyElems() int32 {
	if x != nil {
		return x.NumKeyElems
	}
	return 0
}

func (x *Meta) GetColNames() []string {
	if x != nil {
		return x.ColNames
	}
	return nil
}

func (x *Meta) GetUniqueIndicesStr() []string {
	if x != nil {
		return x.UniqueIndicesStr
	}
	return nil
}

var File_meta_proto protoreflect.FileDescriptor

var file_meta_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x6d, 0x65, 0x74, 0x61, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x74, 0x61,
	0x62, 0x6c, 0x65, 0x22, 0xa4, 0x01, 0x0a, 0x04, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x18, 0x0a, 0x07,
	0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x56,
	0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x18, 0x0a, 0x07, 0x4e, 0x75, 0x6d, 0x43, 0x6f, 0x6c,
	0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x4e, 0x75, 0x6d, 0x43, 0x6f, 0x6c, 0x73,
	0x12, 0x20, 0x0a, 0x0b, 0x4e, 0x75, 0x6d, 0x4b, 0x65, 0x79, 0x45, 0x6c, 0x65, 0x6d, 0x73, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x4e, 0x75, 0x6d, 0x4b, 0x65, 0x79, 0x45, 0x6c, 0x65,
	0x6d, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x43, 0x6f, 0x6c, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x18, 0x04,
	0x20, 0x03, 0x28, 0x09, 0x52, 0x08, 0x43, 0x6f, 0x6c, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x12, 0x2a,
	0x0a, 0x10, 0x55, 0x6e, 0x69, 0x71, 0x75, 0x65, 0x49, 0x6e, 0x64, 0x69, 0x63, 0x65, 0x73, 0x53,
	0x74, 0x72, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09, 0x52, 0x10, 0x55, 0x6e, 0x69, 0x71, 0x75, 0x65,
	0x49, 0x6e, 0x64, 0x69, 0x63, 0x65, 0x73, 0x53, 0x74, 0x72, 0x42, 0x0a, 0x5a, 0x08, 0x2e, 0x2f,
	0x3b, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_meta_proto_rawDescOnce sync.Once
	file_meta_proto_rawDescData = file_meta_proto_rawDesc
)

func file_meta_proto_rawDescGZIP() []byte {
	file_meta_proto_rawDescOnce.Do(func() {
		file_meta_proto_rawDescData = protoimpl.X.CompressGZIP(file_meta_proto_rawDescData)
	})
	return file_meta_proto_rawDescData
}

var file_meta_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_meta_proto_goTypes = []interface{}{
	(*Meta)(nil), // 0: table.Meta
}
var file_meta_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_meta_proto_init() }
func file_meta_proto_init() {
	if File_meta_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_meta_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Meta); i {
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
			RawDescriptor: file_meta_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_meta_proto_goTypes,
		DependencyIndexes: file_meta_proto_depIdxs,
		MessageInfos:      file_meta_proto_msgTypes,
	}.Build()
	File_meta_proto = out.File
	file_meta_proto_rawDesc = nil
	file_meta_proto_goTypes = nil
	file_meta_proto_depIdxs = nil
}