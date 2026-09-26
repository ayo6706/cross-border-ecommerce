package backoff

import "os"

// ZZProbe is a throwaway ENG-057 F4 probe: its unchecked error must fail the lint gate.
func ZZProbe() { os.Remove("zz-probe") }
