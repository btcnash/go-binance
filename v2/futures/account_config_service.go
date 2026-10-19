package futures

import (
	"context"
	"encoding/json"
	"fmt"
)

// AccountConfigService get futures account configuration
type AccountConfigService struct {
	c *Client
}

// AccountConfig define futures account configuration
type AccountConfig struct {
	FeeTier           int   `json:"feeTier"`          // Account commission tier
	CanTrade          bool  `json:"canTrade"`         // If can trade
	CanDeposit        bool  `json:"canDeposit"`       // If can transfer in asset
	CanWithdraw       bool  `json:"canWithdraw"`      // If can transfer out asset
	DualSidePosition  bool  `json:"dualSidePosition"` // If dual side position is enabled
	UpdateTime        int64 `json:"updateTime"`       // Reserved property
	MultiAssetsMargin bool  `json:"multiAssetsMargin"`
	TradeGroupId      int   `json:"tradeGroupId"`
}

// UnmarshalJSON preserves the distinction between a known single-asset mode
// and a response that did not provide an asset mode.
func (c *AccountConfig) UnmarshalJSON(data []byte) error {
	type accountConfig AccountConfig
	response := struct {
		accountConfig
		MultiAssetsMargin *bool `json:"multiAssetsMargin"`
	}{accountConfig: accountConfig(*c)}
	if err := json.Unmarshal(data, &response); err != nil {
		return err
	}
	if response.MultiAssetsMargin == nil {
		return fmt.Errorf("%w: missing or null multiAssetsMargin", ErrInvalidAccountConfigurationResponse)
	}
	response.accountConfig.MultiAssetsMargin = *response.MultiAssetsMargin
	*c = AccountConfig(response.accountConfig)
	return nil
}

// Do send request
func (s *AccountConfigService) Do(ctx context.Context, opts ...RequestOption) (*AccountConfig, error) {
	r := &request{
		method:   "GET",
		endpoint: "/fapi/v1/accountConfig",
		secType:  secTypeSigned,
	}
	data, _, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res := new(AccountConfig)
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}
