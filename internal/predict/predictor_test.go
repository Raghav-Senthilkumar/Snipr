package predict_test

import (
	"path/filepath"
	"testing"

	"github.com/Raghav-Senthilkumar/Snipr/internal/predict"
)

func chdirRoot(t *testing.T) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	// t.Chdir restores cwd after the test (avoids breaking sibling tests).
	t.Chdir(root)
}

func TestLoadRealModel(t *testing.T) {
	chdirRoot(t)

	p, err := predict.LoadXGB("models/mymodel.json")
	if err != nil {
		t.Fatal(err)
	}
	if !p.Ready() {
		t.Fatal("model not ready")
	}

	quiet, err := p.PredictProba([]float64{3, 2, 20, 1, 3, 1.5, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	hype, err := p.PredictProba([]float64{80, 40, 15, 20, 25, 3.0, 8, 0.4})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("quiet=%.6f hype=%.6f", quiet, hype)

	// Match Python xgboost on the same vectors (approx).
	if quiet > 1e-3 {
		t.Fatalf("quiet prob too high: %v", quiet)
	}
	if hype < 0.05 || hype > 0.10 {
		t.Fatalf("hype prob want ~0.0705, got %v", hype)
	}
}

func TestRejectNativeSaveModel(t *testing.T) {
	chdirRoot(t)
	_, err := predict.LoadXGB("mymodel.json")
	if err == nil {
		t.Fatal("expected error loading native save_model JSON")
	}
}
