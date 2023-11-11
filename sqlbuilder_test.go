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
		tblname      string
		maxallowpack int
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
				tblname:      "REPLACE INTO `foo` VALUES",
				maxallowpack: 1024,
			},
			wantQueries: nil,
			estimateErr: errors.New("data is empty - nothing to build"),
			wantErr:     true,
		},
		{
			name: "1 element pool",
			args: args{
				data:         [][]string{{`a`, `b`, `c`}},
				tblname:      "REPLACE INTO `foo` VALUES",
				maxallowpack: 2048,
			},
			wantQueries: []string{"REPLACE INTO `foo` VALUES ('a','b','c')"},
			estimateErr: nil,
			wantErr:     false,
		},
		{
			name: "5 element pool",
			args: args{
				data: [][]string{
					{`a`, `b`, `c`},
					{`a`, `b`, `c`},
					{`a`, `b`, `c`},
					{`a`, `b`, `c`},
					{`a`, `b`, `c`},
				},
				tblname:      "REPLACE INTO `foo` VALUES",
				maxallowpack: 1024,
			},
			wantQueries: []string{
				"REPLACE INTO `foo` VALUES ('a','b','c'),('a','b','c'),('a','b','c'),('a','b','c'),('a','b','c')",
			},
			estimateErr: errors.New("data is empty"),
			wantErr:     true,
		},
		{
			name: "rowBuild error - empty data in first slice",
			args: args{
				data: [][]string{
					{},
					{`a`, `b`, `c`},
					{`a`, `b`, `c`},
				},
				tblname:      "REPLACE INTO `foo` VALUES",
				maxallowpack: 1024,
			},
			wantQueries: nil,
			estimateErr: errors.New("row can't be built from empty data"),
			wantErr:     true,
		},
		{
			name: "rowBuild error - empty data in non first slice",
			args: args{
				data: [][]string{
					{`a`, `b`, `c`},
					{},
					{`a`, `b`, `c`},
				},
				tblname:      "REPLACE INTO `foo` VALUES",
				maxallowpack: 1024,
			},
			wantQueries: nil,
			estimateErr: errors.New("row can't be built from empty data"),
			wantErr:     true,
		},
		{
			name: "long pool",
			args: args{
				data: [][]string{
					{`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`},
					{`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`},
					{`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`},
					{`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`},
					{`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`},
				},
				tblname:      "REPLACE INTO `foo` VALUES",
				maxallowpack: 1024,
			},
			wantQueries: []string{
				"REPLACE INTO `foo` VALUES ('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.'),('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.'),('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.'),('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.')",
				"REPLACE INTO `foo` VALUES ('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.'),('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.','Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.')",
			},
			estimateErr: errors.New("query is too big - max_allowed_packet limit is 1024"),
			wantErr:     true,
		},
		{
			name: "too muh small allowed packet",
			args: args{
				data: [][]string{
					{`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`},
					{`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`},
					{`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`},
					{`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`},
					{`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`, `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec metus.`},
				},
				tblname:      "REPLACE INTO `foo` VALUES",
				maxallowpack: 32,
			},
			wantQueries: nil,
			estimateErr: errors.New("query is too big - maxallowedpacket is 32"),
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotQueries, err := QueriesBuild(tt.args.data, tt.args.tblname, uint64(tt.args.maxallowpack))
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
