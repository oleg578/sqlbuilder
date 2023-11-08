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
	// if data is empty we can't do anything and return error
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

func quoteStr(s string) string {
	return "'" + escapeStr(s) + "'"
}

func rowBuild(inslc []string) (string, error) {
	if len(inslc) == 0 {
		return "", errors.New("empty data")
	}
	wr := &strings.Builder{}
	//open parenthesis
	wr.WriteString("(")
	// add a first element, escaped and quoted
	wr.WriteString(quoteStr(escapeStr(inslc[0])))
	// add all other elements, escaped and quoted
	for _, r := range inslc[1:] {
		wr.WriteString(",")
		wr.WriteString(quoteStr(escapeStr(r)))
	}
	// close parenthesis
	wr.WriteString(")")
	return wr.String(), nil
}
