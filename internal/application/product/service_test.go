package product_test

import (
	"testing"

	productApp "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
)

// Reads through the service are proven at the HTTP boundary (httpapi TestListProducts_Handler,
// TestGetProduct_Handler) and end to end (TestProductsAPI_Live).
func TestProductService_Constructor(t *testing.T) {
	t.Parallel()

	svc, err := productApp.NewService(nil)
	if err == nil {
		t.Fatal("expected error when repo is nil, got nil")
	}
	if svc != nil {
		t.Fatalf("expected nil service, got %v", svc)
	}
}
