// Code generated by "stringer -type=DataType"; DO NOT EDIT.

package crudite

import "strconv"

const _DataType_name = "PNCounterLWWSetpCounternCounteraSetrSet"

var _DataType_index = [...]uint8{0, 9, 15, 23, 31, 35, 39}

func (i DataType) String() string {
	if i < 0 || i >= DataType(len(_DataType_index)-1) {
		return "DataType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _DataType_name[_DataType_index[i]:_DataType_index[i+1]]
}
