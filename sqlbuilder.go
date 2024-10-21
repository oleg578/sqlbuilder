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

const (
	EmptyData   = "empty data"
	TooBigQuery = "maxAllowedPack exceeded"
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

func QueriesBuild(data [][]string, tbl string, mxp uint64) (queries []string, err error) {
	qTmpl := fmt.Sprintf("INSERT INTO %s VALUES", tbl)
	mxp-- // some magic :) may be the reason is '\n' in protocol
	// nothing to do
	if len(data) == 0 {
		return nil, errors.New(EmptyData)
	}
	// too small maxAllowedPack
	if uint64(len(qTmpl)) >= mxp {
		return nil, errors.New(TooBigQuery)
	}
	qStr := &strings.Builder{}
	qStr.WriteString(qTmpl)
	values, errPreparedValue := rowBuild(data[0])
	if errPreparedValue != nil {
		return nil, errPreparedValue
	}
	qStr.WriteString(values)
	if uint64(qStr.Len()) > mxp {
		return nil, errors.New(TooBigQuery)
	}
	// build all queries from 1st element
	for i := 1; i < len(data); i++ {
		if len(data[i]) == 0 {
			continue // skip empty
		}
		rowStr, errRowBuild := rowBuild(data[i])
		if errRowBuild != nil {
			return nil, errRowBuild
		}
		if uint64(qStr.Len()+len(rowStr)+1) >= mxp {
			queries = append(queries, qStr.String())
			qStr.Reset()
			qStr.WriteString(qTmpl + rowStr)
			continue
		}
		qStr.WriteString(",")
		qStr.WriteString(rowStr)
	}
	queries = append(queries, qStr.String())
	return
}

func quoteStr(s string) string {
	return "'" + escapeStr(s) + "'"
}

func rowBuild(strs []string) (string, error) {
	if len(strs) == 0 {
		return "", errors.New(EmptyData)
	}
	wr := &strings.Builder{}
	// open parenthesis
	wr.WriteString("(")
	// add a first element, escaped and quoted
	wr.WriteString(quoteStr(escapeStr(strs[0])))
	// add all other elements, escaped and quoted
	for _, r := range strs[1:] {
		wr.WriteString(",")
		wr.WriteString(quoteStr(escapeStr(r)))
	}
	// close parenthesis
	wr.WriteString(")")
	return wr.String(), nil
}
