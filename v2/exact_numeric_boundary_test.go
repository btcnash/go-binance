package binance_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	binance "github.com/btcnash/go-binance/v2"
	"github.com/btcnash/go-binance/v2/common"
	"github.com/btcnash/go-binance/v2/delivery"
	"github.com/btcnash/go-binance/v2/futures"
	"github.com/btcnash/go-binance/v2/options"
	"github.com/btcnash/go-binance/v2/portfolio"
)

func mustJSON[T any](t *testing.T, raw string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatalf("json.Unmarshal(%T): %v", v, err)
	}
	return v
}

func TestExactNumericResponseBoundary(t *testing.T) {
	const dec = "123456789.123456789"
	const tiny = "0.1234567890123456789"
	const big = "9007199254740993"

	indicator := mustJSON[futures.IndicatorInfo](t, `{"value":123456789.123456789,"triggerValue":9007199254740993}`)
	if indicator.Value != dec || indicator.TriggerValue != big {
		t.Fatalf("futures indicator lost token: %#v", indicator)
	}
	deliveryPrice := mustJSON[futures.DeliveryPrice](t, `{"deliveryPrice":123456789.123456789}`)
	if deliveryPrice.DeliveryPrice != dec {
		t.Fatalf("delivery price = %q", deliveryPrice.DeliveryPrice)
	}
	bracket := mustJSON[futures.Bracket](t, `{"notionalCap":9007199254740993,"notionalFloor":0,"maintMarginRatio":0.1234567890123456789,"cum":123456789.123456789}`)
	if bracket.NotionalCap != big || bracket.NotionalFloor != "0" || bracket.MaintMarginRatio != tiny || bracket.Cum != dec {
		t.Fatalf("futures bracket lost token: %#v", bracket)
	}
	blvt := mustJSON[futures.WsBLVTInfoEvent](t, `{"m":123456789.123456789,"n":"0.1234567890123456789","l":9007199254740993,"f":"-0.000000000000000001"}`)
	if blvt.Issued != dec || blvt.Nav != tiny || blvt.Leverage != big || blvt.FundingRate != "-0.000000000000000001" {
		t.Fatalf("BLVT lost token: %#v", blvt)
	}
	contract := mustJSON[futures.WsContractInfoBracket](t, `{"bnf":0,"bnc":9007199254740993,"mmr":0.1234567890123456789,"cf":123456789.123456789}`)
	if contract.NotionalFloor != "0" || contract.NotionalCap != big || contract.MaintMarginRatio != tiny || contract.Cum != dec {
		t.Fatalf("contract bracket lost token: %#v", contract)
	}

	greek := mustJSON[options.WsGreek](t, `{"d":"123456789.123456789","t":"0.1234567890123456789","g":"-0.000000000000000001","v":"9007199254740993"}`)
	if greek.Delta != dec || greek.Theta != tiny || greek.Gamma != "-0.000000000000000001" || greek.Vega != big {
		t.Fatalf("greek = %#v", greek)
	}
	var strict options.WsGreek
	if err := json.Unmarshal([]byte(`{"d":1,"t":"2","g":"3","v":"4"}`), &strict); err == nil {
		t.Fatal("options Greek accepted numeric d; want strict JSON string")
	}

	cm := mustJSON[portfolio.CMBracket](t, `{"qtyCap":9007199254740993,"qtyFloor":0,"maintMarginRatio":0.1234567890123456789,"cum":123456789.123456789}`)
	if cm.QtyCap != big || cm.QtyFloor != "0" || cm.MaintMarginRatio != tiny || cm.Cum != dec {
		t.Fatalf("CM bracket lost token: %#v", cm)
	}
	neg := mustJSON[portfolio.NegativeBalanceExchangeDetail](t, `{"negativeBalance":123456789.123456789,"negativeMaxThreshold":9007199254740993}`)
	if neg.NegativeBalance != dec || neg.NegativeMaxThreshold != big {
		t.Fatalf("negative balance lost token: %#v", neg)
	}
	um := mustJSON[portfolio.Bracket](t, `{"notionalCap":9007199254740993,"notionalFloor":0,"maintMarginRatio":0.1234567890123456789,"cum":123456789.123456789}`)
	if um.NotionalCap != big || um.NotionalFloor != "0" || um.MaintMarginRatio != tiny || um.Cum != dec {
		t.Fatalf("UM bracket lost token: %#v", um)
	}
	pi := mustJSON[portfolio.Indicator](t, `{"value":0.1234567890123456789,"triggerValue":9007199254740993}`)
	if pi.Value != tiny || pi.TriggerValue != big {
		t.Fatalf("portfolio indicator lost token: %#v", pi)
	}
	pp := mustJSON[portfolio.WsPosition](t, `{"bep":123456789.123456789}`)
	if pp.BreakEvenPrice != dec {
		t.Fatalf("portfolio bep = %q", pp.BreakEvenPrice)
	}
	pp = mustJSON[portfolio.WsPosition](t, `{"bep":"0.1234567890123456789"}`)
	if pp.BreakEvenPrice != tiny {
		t.Fatalf("portfolio quoted bep = %q", pp.BreakEvenPrice)
	}

	balance := mustJSON[binance.AssetBalance](t, `{"asset":"USDT","free":123456789.123456789,"locked":9007199254740993}`)
	if balance.Free != dec || balance.Locked != big {
		t.Fatalf("asset balance lost token: %#v", balance)
	}
	trade := mustJSON[binance.TradeInfoVo](t, `{"btc":9007199254740993,"btcFutures":123456789.123456789,"btcMargin":0.1234567890123456789,"busd":0,"busdFutures":1e-18,"busdMargin":1E+18}`)
	if trade.Btc != big || trade.BtcFutures != dec || trade.BtcMargin != tiny || trade.Busd != "0" || trade.BusdFutures != "1e-18" || trade.BusdMargin != "1E+18" {
		t.Fatalf("trade info lost token: %#v", trade)
	}

	lb := mustJSON[futures.LeverageBracket](t, `{"symbol":"BTCUSDT","notionalCoef":"1.5","brackets":[]}`)
	if lb.NotionalCoef == nil || *lb.NotionalCoef != "1.5" {
		t.Fatalf("notionalCoef = %#v", lb.NotionalCoef)
	}

	coinm := mustJSON[delivery.WsUserDataEvent](t, `{"e":"ACCOUNT_UPDATE","E":9007199254740993}`)
	if coinm.Time != 9007199254740993 {
		t.Fatalf("COIN-M time = %d", coinm.Time)
	}
	coinm = mustJSON[delivery.WsUserDataEvent](t, `{"e":"ACCOUNT_UPDATE","E":"9007199254740993"}`)
	if coinm.Time != 9007199254740993 {
		t.Fatalf("COIN-M quoted time = %d", coinm.Time)
	}

	level := common.PriceLevel{Price: dec, Quantity: "0.000000000000000001"}
	price, qty, err := level.Parse()
	if err != nil || price.String() != dec || qty.String() != "0.000000000000000001" {
		t.Fatalf("PriceLevel.Parse() = %s, %s, %v", price, qty, err)
	}

}

func TestExchangeInfoFiltersPreserveUnknownRaw(t *testing.T) {
	const raw = `{"filterType":"FUTURE_FILTER","value":9.324}`

	spotExchange := mustJSON[binance.ExchangeFilter](t, raw)
	if spotExchange.FilterType != "FUTURE_FILTER" || spotExchange.Raw != raw {
		t.Fatalf("spot exchange filter = %#v", spotExchange)
	}
	futExchange := mustJSON[futures.ExchangeFilter](t, raw)
	if futExchange.FilterType != "FUTURE_FILTER" || futExchange.Raw != raw {
		t.Fatalf("futures exchange filter = %#v", futExchange)
	}
	delExchange := mustJSON[delivery.ExchangeFilter](t, raw)
	if delExchange.FilterType != "FUTURE_FILTER" || delExchange.Raw != raw {
		t.Fatalf("delivery exchange filter = %#v", delExchange)
	}

	spot := mustJSON[binance.Filter](t, raw)
	if spot.FilterType != "FUTURE_FILTER" || spot.Raw != raw {
		t.Fatalf("spot unknown filter = %#v", spot)
	}
	fut := mustJSON[futures.Filter](t, raw)
	if fut.FilterType != "FUTURE_FILTER" || fut.Raw != raw {
		t.Fatalf("futures unknown filter = %#v", fut)
	}
	del := mustJSON[delivery.Filter](t, raw)
	if del.FilterType != "FUTURE_FILTER" || del.Raw != raw {
		t.Fatalf("delivery unknown filter = %#v", del)
	}
	opt := mustJSON[options.Filter](t, raw)
	if opt.FilterType != "FUTURE_FILTER" || opt.Raw != raw {
		t.Fatalf("options unknown filter = %#v", opt)
	}

	spotKnown := mustJSON[binance.Filter](t, `{"filterType":"LOT_SIZE","minQty":"0.000000000000000001","maxQty":"123456789.123456789","stepSize":"0.000000000000000001"}`)
	if spotKnown.MinQty != "0.000000000000000001" || spotKnown.MaxQty != "123456789.123456789" || spotKnown.StepSize != "0.000000000000000001" {
		t.Fatalf("spot known filter = %#v", spotKnown)
	}
	futKnown := mustJSON[futures.Filter](t, `{"filterType":"PERCENT_PRICE","multiplierUp":"1.123456789012345678","multiplierDown":"0.123456789012345678","multiplierDecimal":"18"}`)
	if futKnown.MultiplierUp != "1.123456789012345678" || futKnown.MultiplierDown != "0.123456789012345678" || futKnown.MultiplierDecimal != "18" {
		t.Fatalf("futures known filter = %#v", futKnown)
	}
	delKnown := mustJSON[delivery.Filter](t, `{"filterType":"PRICE_FILTER","minPrice":"0.000000000000000001","maxPrice":"123456789.123456789","tickSize":"0.000000000000000001"}`)
	if delKnown.MinPrice != "0.000000000000000001" || delKnown.MaxPrice != "123456789.123456789" || delKnown.TickSize != "0.000000000000000001" {
		t.Fatalf("delivery known filter = %#v", delKnown)
	}
	optKnown := mustJSON[options.Filter](t, `{"filterType":"LOT_SIZE","minQty":"0.000000000000000001","maxQty":"123456789.123456789","stepSize":"0.000000000000000001"}`)
	if optKnown.MinQty != "0.000000000000000001" || optKnown.MaxQty != "123456789.123456789" || optKnown.StepSize != "0.000000000000000001" {
		t.Fatalf("options known filter = %#v", optKnown)
	}

	ei := mustJSON[delivery.ExchangeInfo](t, `{"symbols":[{"underlyingSubType":["DEFI","HOT"]}]}`)
	if len(ei.Symbols) != 1 || len(ei.Symbols[0].UnderlyingSubType) != 2 || ei.Symbols[0].UnderlyingSubType[1] != "HOT" {
		t.Fatalf("underlyingSubType = %#v", ei.Symbols)
	}
}

func TestFinancialSettersPreserveExactRequestText(t *testing.T) {
	const exact = "123456789.123456789"
	const tiny = "0.000000000000000001"

	seen := make(map[string]map[string]string)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm(%s): %v", r.URL.Path, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		values := make(map[string]string)
		for key, list := range r.Form {
			if len(list) != 0 {
				values[key] = list[0]
			}
		}
		seen[r.URL.Path] = values
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/sapi/v1/lending/daily/purchase":
			_, _ = w.Write([]byte(`{"purchaseId":1}`))
		case "/sapi/v1/sub-account/futures/transfer":
			_, _ = w.Write([]byte(`{"tranId":1}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()

	client := binance.NewClient("key", "secret")
	client.BaseURL = server.URL
	ctx := context.Background()

	if _, err := client.NewCreateFuturesAlgoVpOrderService().Symbol("BTCUSDT").Side(binance.SideTypeBuy).Quantity(exact).Urgency(binance.FuturesAlgoUrgencyTypeMedium).LimitPrice(tiny).Do(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NewCreateFuturesAlgoTwapOrderService().Symbol("BTCUSDT").Side(binance.SideTypeBuy).Quantity(exact).Duration(60).LimitPrice(tiny).Do(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NewAddLiquidityPreviewService().PoolId(2).OperationType(binance.LiquidityOperationTypeCombination).QuoteAsset("USDT").QuoteQty(exact).Do(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NewGetSwapQuoteService().QuoteAsset("USDT").BaseAsset("BUSD").QuoteQty(exact).Do(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NewSwapService().QuoteAsset("USDT").BaseAsset("BUSD").QuoteQty(exact).Do(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NewAddLiquidityService().PoolId(2).OperationType(binance.LiquidityOperationTypeCombination).QuoteAsset("USDT").QuoteQty(exact).Do(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NewRemoveLiquidityService().PoolId(2).OperationType(binance.LiquidityOperationTypeCombination).AddAesst("USDT").ShareAmount(exact).Do(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NewPurchaseSavingsFlexibleProductService().ProductId("BTC001").Amount(exact).Do(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.NewRedeemSavingsFlexibleProductService().ProductId("BTC001").Amount(exact).Type("FAST").Do(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NewSubAccountFuturesTransferV1Service().Email("a@example.com").Asset("USDT").Amount(exact).TransferType(1).Do(ctx); err != nil {
		t.Fatal(err)
	}

	want := []struct{ path, key, value string }{
		{"/sapi/v1/algo/futures/newOrderVp", "quantity", exact},
		{"/sapi/v1/algo/futures/newOrderVp", "limitPrice", tiny},
		{"/sapi/v1/algo/futures/newOrderTwap", "quantity", exact},
		{"/sapi/v1/algo/futures/newOrderTwap", "limitPrice", tiny},
		{"/sapi/v1/bswap/addLiquidityPreview", "quoteQty", exact},
		{"/sapi/v1/bswap/quote", "quoteQty", exact},
		{"/sapi/v1/bswap/swap", "quoteQty", exact},
		{"/sapi/v1/bswap/liquidityAdd", "quantity", exact},
		{"/sapi/v1/bswap/liquidityRemove", "shareAmount", exact},
		{"/sapi/v1/lending/daily/purchase", "amount", exact},
		{"/sapi/v1/lending/daily/redeem", "amount", exact},
		{"/sapi/v1/sub-account/futures/transfer", "amount", exact},
	}
	for _, item := range want {
		if got := seen[item.path][item.key]; got != item.value {
			t.Fatalf("%s %s = %q, want exact %q", item.path, item.key, got, item.value)
		}
	}
}

func TestOptionsBatchReturnsTypedResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"orderId":1,"symbol":"BTC-TEST-C","price":"1.000000000000000001","quantity":"0.01"},
			{"code":1002,"msg":"bad request"}
		]`))
	}))
	defer server.Close()

	client := options.NewClient("key", "secret")
	client.BaseURL = server.URL
	ctx := context.Background()

	order := client.NewCreateOrderService().
		Symbol("BTC-TEST-C").
		Side(options.SideTypeBuy).
		Type(options.OrderTypeLimit).
		Quantity("0.01").
		Price("1.000000000000000001")
	created, err := client.NewCreateBatchOrdersService().OrderList([]*options.CreateOrderService{order}).Do(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertBatchResults(t, created)

	cancelled, err := client.NewCancelBatchOrdersService().Symbol("BTC-TEST-C").OrderIds([]int64{1, 2}).Do(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertBatchResults(t, cancelled)
}

func assertBatchResults(t *testing.T, got []options.BatchOrderResult) {
	t.Helper()
	if len(got) != 2 {
		t.Fatalf("batch result len = %d", len(got))
	}
	if got[0].Order == nil || got[0].Error != nil || got[0].Order.Price != "1.000000000000000001" {
		t.Fatalf("batch order result = %#v", got[0])
	}
	if got[1].Order != nil || got[1].Error == nil || got[1].Error.Code != 1002 {
		t.Fatalf("batch error result = %#v", got[1])
	}
}
