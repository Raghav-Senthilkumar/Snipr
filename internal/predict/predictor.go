package predict

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	xgb "github.com/Elvenson/xgboost-go"
	"github.com/Elvenson/xgboost-go/activation"
	"github.com/Elvenson/xgboost-go/inference"
	"github.com/Elvenson/xgboost-go/mat"
)

const (
	// ProbThreshold matches ValSparks predict.py (prediction = 1 if prob >= 0.7).
	ProbThreshold = 0.7
	// DefaultModelPath is the dump_model JSON used by the Go loader.
	DefaultModelPath = "models/mymodel.json"
	// DefaultNativeModel is the original ValSparks save_model JSON (for base_score).
	DefaultNativeModel = "mymodel.json"
	// DefaultFeatureMap maps named splits → indices (optional; dump uses f0..f7).
	DefaultFeatureMap = "models/feature_map.txt"
)

// Predictor scores an 8-feature window and returns P(hype).
type Predictor interface {
	PredictProba(features []float64) (float64, error)
	Ready() bool
}

// XGBPredictor loads dump_model JSON via Elvenson/xgboost-go (pure Go, no CGO).
type XGBPredictor struct {
	ensemble       *inference.Ensemble
	baseScoreLogit float64 // logit(base_score) from native save_model; 0 if unknown
}

// LoadXGB opens modelPath (dump_model JSON). Missing file → NoopPredictor.
func LoadXGB(modelPath string) (Predictor, error) {
	if modelPath == "" {
		modelPath = resolveModelPath()
	}
	if _, err := os.Stat(modelPath); err != nil {
		if os.IsNotExist(err) {
			return NewNoop(fmt.Sprintf("model not found at %s", modelPath)), nil
		}
		return nil, err
	}

	if err := ensureDumpFormat(modelPath); err != nil {
		return nil, err
	}

	// Raw margins so we can add base_score (dump_model omits it; Elvenson Logistic would be wrong).
	ens, err := xgb.LoadXGBoostFromJSON(modelPath, "", 1, 0, &activation.Raw{})
	if err != nil {
		return nil, fmt.Errorf("load xgboost json: %w", err)
	}

	baseLogit := loadBaseScoreLogit(DefaultNativeModel)
	return &XGBPredictor{ensemble: ens, baseScoreLogit: baseLogit}, nil
}

func resolveModelPath() string {
	candidates := []string{DefaultModelPath, "mymodel.json"}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil && isDumpFormat(p) {
			return p
		}
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return DefaultModelPath
}

func isDumpFormat(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	var probe any
	if err := json.NewDecoder(f).Decode(&probe); err != nil {
		return false
	}
	_, ok := probe.([]any)
	return ok
}

func ensureDumpFormat(path string) error {
	if isDumpFormat(path) {
		return nil
	}
	return fmt.Errorf(
		"%s looks like native XGBoost save_model JSON (has learner/version), not dump_model.\n"+
			"Use models/mymodel.json (already converted) or re-run dump_model — see models/README.md",
		path,
	)
}

// loadBaseScoreLogit reads base_score from native save_model JSON and returns logit(p).
func loadBaseScoreLogit(nativePath string) float64 {
	data, err := os.ReadFile(nativePath)
	if err != nil {
		return 0
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		return 0
	}
	learner, _ := root["learner"].(map[string]any)
	if learner == nil {
		return 0
	}
	params, _ := learner["learner_model_param"].(map[string]any)
	if params == nil {
		return 0
	}
	raw, _ := params["base_score"].(string)
	raw = strings.Trim(raw, "[]")
	p, err := strconv.ParseFloat(raw, 64)
	if err != nil || p <= 0 || p >= 1 {
		return 0
	}
	return math.Log(p / (1 - p))
}

func (p *XGBPredictor) Ready() bool { return p != nil && p.ensemble != nil }

func (p *XGBPredictor) PredictProba(features []float64) (float64, error) {
	if !p.Ready() {
		return 0, fmt.Errorf("predictor not ready")
	}
	if len(features) != 8 {
		return 0, fmt.Errorf("expected 8 features, got %d", len(features))
	}

	row := make(mat.SparseVector, len(features))
	for i, v := range features {
		row[i] = float32(v)
	}
	sm := mat.SparseMatrix{Vectors: []mat.SparseVector{row}}

	// Raw leaf-sum margins (activation.Raw).
	preds, err := p.ensemble.PredictProba(sm)
	if err != nil {
		return 0, fmt.Errorf("predict: %w", err)
	}
	if len(preds.Vectors) == 0 || preds.Vectors[0] == nil || len(*preds.Vectors[0]) == 0 {
		return 0, fmt.Errorf("empty prediction")
	}
	margin := float64((*preds.Vectors[0])[0]) + p.baseScoreLogit
	return sigmoid(margin), nil
}

func sigmoid(x float64) float64 {
	return 1 / (1 + math.Exp(-x))
}

// NoopPredictor is used when the model file is not present yet.
type NoopPredictor struct {
	ReasonMsg string
}

func NewNoop(reason string) *NoopPredictor {
	return &NoopPredictor{ReasonMsg: reason}
}

func (n *NoopPredictor) Ready() bool { return false }

func (n *NoopPredictor) PredictProba(_ []float64) (float64, error) {
	return 0, fmt.Errorf("no model loaded: %s", n.ReasonMsg)
}

func (n *NoopPredictor) Reason() string { return n.ReasonMsg }

func Decide(prob float64) int {
	if prob >= ProbThreshold {
		return 1
	}
	return 0
}

func AbsPath(p string) string {
	a, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return a
}
