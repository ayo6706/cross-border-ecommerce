package product

const (
	AggregateTypeProduct    = "product"
	EventTypeProductChanged = "product.changed"
)

type ProductChanged struct {
	ProductID     ID
	VersionID     string
	VersionNumber int
	Fingerprint   string
	ChangeType    ChangeType
	ChangedFields []string
}
