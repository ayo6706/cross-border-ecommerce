package product

type ProductChanged struct {
	ProductID     ID
	VersionID     string
	VersionNumber int
	Fingerprint   string
	ChangeType    ChangeType
	ChangedFields []string
}
