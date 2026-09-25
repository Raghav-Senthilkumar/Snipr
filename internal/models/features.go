package models

import "time"

// FeatureOrder matches ValSparks / predict.py FEATURE_ORDER exactly.
var FeatureOrder = []string{
	"msg_count",
	"unique_users",
	"avg_msg_len",
	"max_repeat_count",
	"unique_norm_msgs",
	"entropy_raw",
	"hype_score",
	"repeat_ratio",
}

// FeatureVector is one non-empty 24-second chat window, ready for the ML model.
type FeatureVector struct {
	Channel         string    `json:"channel"`
	WindowStart     time.Time `json:"window_start"`
	WindowEnd       time.Time `json:"window_end"`
	MsgCount        float64   `json:"msg_count"`
	UniqueUsers     float64   `json:"unique_users"`
	AvgMsgLen       float64   `json:"avg_msg_len"`
	MaxRepeatCount  float64   `json:"max_repeat_count"`
	UniqueNormMsgs  float64   `json:"unique_norm_msgs"`
	EntropyRaw      float64   `json:"entropy_raw"`
	HypeScore       float64   `json:"hype_score"`
	RepeatRatio     float64   `json:"repeat_ratio"`
}

// Values returns the 8 features in FEATURE_ORDER for model input shape (1, 8).
func (f FeatureVector) Values() []float64 {
	return []float64{
		f.MsgCount,
		f.UniqueUsers,
		f.AvgMsgLen,
		f.MaxRepeatCount,
		f.UniqueNormMsgs,
		f.EntropyRaw,
		f.HypeScore,
		f.RepeatRatio,
	}
}
