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
	queryTemplate string,
	maxAllowedPack uint64,
) (queries []string, err error) {
	// if data is empty, we can't do anything and return error
	if len(data) == 0 {
		err = errors.New("empty data")
		return nil, err
	}
	// add space at the end of query
	SQLQuery := queryTemplate + " "
	outValuesString := &strings.Builder{}
	outValuesString.WriteString(SQLQuery)
	preparedValue, errPreparedValue := rowBuild(data[0])
	if errPreparedValue != nil {
		return nil, errPreparedValue
	}
	outValuesString.WriteString(preparedValue)
	// check if the query length limit is reached
	if uint64(outValuesString.Len()) > maxAllowedPack {
		err = fmt.Errorf(
			"query is too big - maxallowedpacket is %d", maxAllowedPack)
		return nil, err
	}
	// all data processed - nothing to do
	if len(data) == 1 {
		queries = append(queries, outValuesString.String())
		return queries, nil
	}
	// build all queries from 1st element
	for i := 1; i < len(data); i++ {
		rowString, errRowBuild := rowBuild(data[i])
		if errRowBuild != nil {
			return nil, errRowBuild
		}
		if uint64(outValuesString.Len()+len(rowString)+1) >= maxAllowedPack {
			queries = append(queries, outValuesString.String())
			outValuesString.Reset()
			outValuesString.WriteString(SQLQuery)
			outValuesString.WriteString(rowString)
		}
		outValuesString.WriteString(",")
		outValuesString.WriteString(rowString)
	}
	queries = append(queries, outValuesString.String())
	return
}

func quoteStr(s string) string {
	return "'" + escapeStr(s) + "'"
}

func rowBuild(strs []string) (string, error) {
	if len(strs) == 0 {
		return "", errors.New("empty data")
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
