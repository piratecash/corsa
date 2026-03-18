package protocol

type OrderSide string

const (
	OrderSideBuy  OrderSide = "buy"
	OrderSideSell OrderSide = "sell"
)

type Order struct {
	ID       string
	Base     string
	Quote    string
	Side     OrderSide
	Price    string
	Amount   string
	Creator  string
	Sequence uint64
}
