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

func escapeStr(s string) string {
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

func QueriesBuild(
	data [][]string,
	querytemplate string,
	maxallowedpack uint64) (queries []string, err error) {
	// nothing to do
	if len(data) == 0 {
		err = errors.New("data is empty")
		return nil, err
	}
	SQLQuery := querytemplate + " "
	outsql := &strings.Builder{}
	outsql.WriteString(SQLQuery)
	outsql.WriteString(rowBuild(data[0]))
	if uint64(outsql.Len()) > maxallowedpack {
		err = fmt.Errorf("query is too big - max_allowed_packet limit is %d", maxallowedpack)
		return nil, err
	}
	// all data processed - nothing to do
	if len(data) == 1 {
		queries = append(queries, outsql.String())
		return
	}
	for i := 1; i < len(data); i++ {
		r := rowBuild(data[i])
		if uint64(outsql.Len()+len(r)+1) >= maxallowedpack {
			queries = append(queries, outsql.String())
			outsql.Reset()
			outsql.WriteString(SQLQuery)
			outsql.WriteString(rowBuild(data[i]))
		}
		outsql.WriteString(",")
		outsql.WriteString(rowBuild(data[i]))
	}
	queries = append(queries, outsql.String())
	return
}

func rowBuild(inslc []string) string {
	if len(inslc) == 0 {
		return ""
	}
	wr := &strings.Builder{}
	wr.WriteString("('" + escapeStr(inslc[0]) + "'")
	for _, r := range inslc[1:] {
		wr.WriteString(",'")
		wr.WriteString(escapeStr(r))
		wr.WriteString("'")
	}
	wr.WriteString(")")
	return wr.String()
}
