package model

// TransactionTemplate represents the code structure of a transaction type
// (e.g., "Payment Transaction") rather than a runtime instance.
type TransactionTemplate struct {
	Name         string
	StaticReads  map[string]struct{} // Set of static read keys
	StaticWrites map[string]struct{} // Set of static write keys
}

// NewTransactionTemplate creates a new transaction template.
func NewTransactionTemplate(name string) *TransactionTemplate {
	return &TransactionTemplate{
		Name:         name,
		StaticReads:  make(map[string]struct{}),
		StaticWrites: make(map[string]struct{}),
	}
}

// AddRead adds a static read key to the template.
func (t *TransactionTemplate) AddRead(key string) {
	t.StaticReads[key] = struct{}{}
}

// AddWrite adds a static write key to the template.
func (t *TransactionTemplate) AddWrite(key string) {
	t.StaticWrites[key] = struct{}{}
}

// HasRead checks if the template reads the given key.
func (t *TransactionTemplate) HasRead(key string) bool {
	_, ok := t.StaticReads[key]
	return ok
}

// HasWrite checks if the template writes the given key.
func (t *TransactionTemplate) HasWrite(key string) bool {
	_, ok := t.StaticWrites[key]
	return ok
}

// ReadKeys returns all static read keys.
func (t *TransactionTemplate) ReadKeys() []string {
	keys := make([]string, 0, len(t.StaticReads))
	for k := range t.StaticReads {
		keys = append(keys, k)
	}
	return keys
}

// WriteKeys returns all static write keys.
func (t *TransactionTemplate) WriteKeys() []string {
	keys := make([]string, 0, len(t.StaticWrites))
	for k := range t.StaticWrites {
		keys = append(keys, k)
	}
	return keys
}
