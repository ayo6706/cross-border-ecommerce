package product

type ProductChanged struct {
	EventID       string
	ProductID     ID
	VersionID     string
	VersionNumber int
	Fingerprint   string
	ChangeType    ChangeType
	ChangedFields []string
}
