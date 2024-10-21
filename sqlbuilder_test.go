package sqlbuilder

import (
	"errors"
	"reflect"
	"testing"
)

func TestEscapeStr(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "default - text without escape",
			args: args{s: "abc"},
			want: "abc",
		},
		{
			name: "backslash",
			args: args{s: `ab` + string(Backslash) + `c`},
			want: `ab\` + string(Backslash) + `c`,
		},
		{
			name: "null byte",
			args: args{s: `ab` + string(AsciiNull) + `c`},
			want: `ab\` + string(AsciiNull) + `c`,
		},
		{
			name: "CARRIAGE_RETURN",
			args: args{s: `ab` + string(CarriageReturn) + `c`},
			want: `ab\` + string(CarriageReturn) + `c`,
		},
		{
			name: "NEW_LINE",
			args: args{s: `ab` + string(NewLine) + `c`},
			want: `ab\` + string(NewLine) + `c`,
		},
		{
			name: "CTRL_Z",
			args: args{s: `ab` + string(CtrlZ) + `c`},
			want: `ab\` + string(CtrlZ) + `c`,
		},
		{
			name: "SINGLE_QUOTE",
			args: args{s: `ab` + string(SingleQuote) + `c`},
			want: `ab\` + string(SingleQuote) + `c`,
		},
		{
			name: "DOUBLE_QUOTE",
			args: args{s: `ab` + string(DoubleQuote) + `c`},
			want: `ab\` + string(DoubleQuote) + `c`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapeStr(tt.args.s); got != tt.want {
				t.Errorf("EscapeStr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueriesBuild(t *testing.T) {
	type args struct {
		data         [][]string
		tblName      string
		maxAllowPack int
	}
	tests := []struct {
		name        string
		args        args
		wantQueries []string
		estimateErr error
		wantErr     bool
	}{
		{
			name: "empty pool",
			args: args{
				data:         [][]string{},
				tblName:      "REPLACE INTO `foo` VALUES",
				maxAllowPack: 1024,
			},
			wantQueries: nil,
			estimateErr: errors.New(EmptyData),
			wantErr:     true,
		},
		{
			name: "1 element pool",
			args: args{
				data:         [][]string{{`a`, `b`, `c`}},
				tblName:      "`foo`",
				maxAllowPack: 2048,
			},
			wantQueries: []string{"INSERT INTO `foo` VALUES('a','b','c')"},
			estimateErr: nil,
			wantErr:     false,
		},
		{
			name: "5 element pool",
			args: args{
				data: [][]string{
					{`1`, `product`, `description`},
					{`2`, `product`, `description`},
					{`3`, `product`, `description`},
					{`4`, `product`, `description`},
					{`5`, `product`, `description`},
				},
				tblName:      "`foo`",
				maxAllowPack: 128,
			},
			wantQueries: []string{
				"INSERT INTO `foo` VALUES('1','product','description'),('2','product','description'),('3','product','description')",
				"INSERT INTO `foo` VALUES('4','product','description'),('5','product','description')",
			},
			estimateErr: errors.New(EmptyData),
			wantErr:     true,
		},
		{
			name: "skip empty data",
			args: args{
				data: [][]string{
					{`1`, `b`, `c`},
					{},
					{`3`, `b`, `c`},
				},
				tblName:      "`foo`",
				maxAllowPack: 1024,
			},
			wantQueries: []string{
				"INSERT INTO `foo` VALUES('1','b','c'),('3','b','c')",
			},
			estimateErr: errors.New(EmptyData),
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotQueries, err := QueriesBuild(tt.args.data, tt.args.tblName, uint64(tt.args.maxAllowPack))
			if (err != nil) && tt.wantErr {
				if err.Error() != tt.estimateErr.Error() {
					t.Errorf("QueriesBuild() error = %v, estimateErr = %v", err, tt.estimateErr)
					return
				}
			}
			if !(reflect.DeepEqual(tt.wantQueries, gotQueries)) {
				t.Errorf(
					"\nQueriesBuild() = %v\nwant: %v\n error: %v\n out len: %d\nwant: %d",
					gotQueries, tt.wantQueries, err, len(tt.wantQueries), len(tt.wantQueries))
			}
		})
	}
}

func TestRowBuild(t *testing.T) {
	type args struct {
		inslc []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty slice",
			args: args{inslc: []string{}},
			want: "",
		},
		{
			name: "one element slice",
			args: args{inslc: []string{`a`}},
			want: "('a')",
		},
		{
			name: "several element slice",
			args: args{inslc: []string{`a`, `b`, `c`}},
			want: "('a','b','c')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := rowBuild(tt.args.inslc); got != tt.want {
				t.Errorf("RowBuild() = %v\n want %v", got, tt.want)
			}
		})
	}
}
