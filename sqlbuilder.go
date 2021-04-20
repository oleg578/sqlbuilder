package sqlbuilder

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	Backslash      = '\\'
	AsciiNull      = '\x00'
	CarriageReturn = '\r'
	NewLine        = '\n'
	CtrlZ          = '\x1A'
	SingleQuote    = '\''
	DoubleQuote    = '"'
)

//https://mariadb.com/kb/en/server-system-variables/#max_allowed_packet
const (
	MINALLOWEDPACKETLEN = 1024
	MAXALLOWEDPACKETLEN = 1073741824
)

func EscapeStr(s string) string {
	in := strings.NewReader(s)
	out := strings.Builder{}
	for {
		r, _, err := in.ReadRune()
		if err == io.EOF {
			break
		}
		switch r {
		case Backslash:
			out.WriteRune(Backslash)
			out.WriteRune(r)
		case AsciiNull:
			out.WriteRune(Backslash)
			out.WriteRune(r)
		case CarriageReturn:
			out.WriteRune(Backslash)
			out.WriteRune(r)
		case NewLine:
			out.WriteRune(Backslash)
			out.WriteRune(r)
		case CtrlZ:
			out.WriteRune(Backslash)
			out.WriteRune(r)
		case SingleQuote:
			out.WriteRune(Backslash)
			out.WriteRune(r)
		case DoubleQuote:
			out.WriteRune(Backslash)
			out.WriteRune(r)
		default:
			out.WriteRune(r)
		}
	}
	return out.String()
}

func QueriesBuild(data [][]string, tblname string, maxallowedpack int) (queries []string, err error) {
	if maxallowedpack < MINALLOWEDPACKETLEN {
		err = errors.New("max_allowed_packet can't be less than 1024")
		return nil, err
	}
	// test MAXALLOWEDPACKETLEN
	if maxallowedpack > MAXALLOWEDPACKETLEN {
		err = errors.New("max_allowed_packet is too big")
		return nil, err
	}
	// nothing todo
	if len(data) == 0 {
		err = errors.New("data is empty")
		return nil, err
	}
	tblname = "`" + tblname + "`"
	SQLQuery := "REPLACE INTO " + tblname + " VALUES "
	outsql := &strings.Builder{}
	outsql.WriteString(SQLQuery)
	outsql.WriteString(RowBuild(data[0]))
	if outsql.Len() > maxallowedpack {
		err = fmt.Errorf("query is too big - max_allowed_packet limit is %d", maxallowedpack)
		return nil, err
	}
	// all data processed - nothing todo
	if len(data) == 1 {
		queries = append(queries, outsql.String())
		return
	}
	for i := 1; i < len(data); i++ {
		r := RowBuild(data[i])
		if (outsql.Len() + len(r) + 1) >= maxallowedpack {
			queries = append(queries, outsql.String())
			outsql.Reset()
			outsql.WriteString(SQLQuery)
			outsql.WriteString(RowBuild(data[i]))
		}
		outsql.WriteString(",")
		outsql.WriteString(RowBuild(data[i]))
	}
	queries = append(queries, outsql.String())
	return
}

func RowBuild(inslc []string) string {
	if len(inslc) == 0 {
		return ""
	}
	wr := &strings.Builder{}
	wr.WriteString("('" + EscapeStr(inslc[0]) + "'")
	for _, r := range inslc[1:] {
		wr.WriteString(",'")
		wr.WriteString(EscapeStr(r))
		wr.WriteString("'")
	}
	wr.WriteString(")")
	return wr.String()
}
