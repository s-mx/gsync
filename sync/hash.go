package sync

type PolynomialHash struct {
	Value uint64
}

func (h PolynomialHash) Append(ch int32, options PolynomialHashOptions) PolynomialHash {
	hash := h
	hash.Value *= uint64(options.parameter)
	if hash.Value >= uint64(options.modulo) {
		hash.Value %= uint64(options.modulo)
	}

	hash.Value += uint64(ch)
	if hash.Value >= uint64(options.modulo) {
		hash.Value %= uint64(options.modulo)
	}

	return hash
}

func (h PolynomialHash) PopLeft(ch int32, options PolynomialHashOptions) PolynomialHash {
	value := h.Value
	value -= (uint64(ch) * options.leftTerm) % options.modulo
	if value >= options.modulo {
		// value must be in the ring [0, modulo), so we return it in the ring in this branch.
		value += options.modulo
	}

	return PolynomialHash{value}
}

type PolynomialHashOptions struct {
	parameter	uint64
	modulo		uint64

	leftTerm		uint64
}

// binaryPow() function returns (a ^ power) % mod.
// a, mod values should fit in int32.
func binaryPow(a, power, mod int64) int64 {
	if power == 0 {
		return 1
	} else if power % 2 == 0 {
		tmp := binaryPow(a, power/2, mod)
		return (tmp * tmp) % mod
	} else {
		return (binaryPow(a, power-1, mod) * a) % mod
	}
}

func CreatePolynomialHashOptions(param, mod, windowLen int) PolynomialHashOptions {
	leftTerm := binaryPow(int64(param), int64(windowLen-1), int64(mod))

	return PolynomialHashOptions{
		parameter: uint64(param),
		modulo:    uint64(mod),
		leftTerm:  uint64(leftTerm),
	}
}

func CalculatePolynomialHashFromDataBlock(data string, options PolynomialHashOptions) PolynomialHash {
	hash := PolynomialHash{0}
	for _, ch := range data {
		hash = hash.Append(ch, options)
	}

	return hash
}

