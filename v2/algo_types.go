package binance

import "github.com/btcnash/go-binance/v2/futures"

// Algo type-safety aliases for the legacy top-level compatibility entry.
type Decimal = futures.Decimal
type OptionalDecimal = futures.OptionalDecimal
type OrderID = futures.OrderID
type AlgoOrderID = futures.AlgoOrderID
type OptionalOrderID = futures.OptionalOrderID

func ParseDecimal(text string) (Decimal, error) { return futures.ParseDecimal(text) }
func MustParseDecimal(text string) Decimal      { return futures.MustParseDecimal(text) }
func NewOptionalDecimal(value Decimal) OptionalDecimal {
	return futures.NewOptionalDecimal(value)
}
func NewOptionalOrderID(value OrderID) (OptionalOrderID, error) {
	return futures.NewOptionalOrderID(value)
}
