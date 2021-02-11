package sync

type PolynomialHash struct {
	value	uint64
}

func (h PolynomialHash) Append(ch int32, options PolynomialHashOptions) PolynomialHash {
	hash := h
	hash.value *= uint64(options.parameter)
	if hash.value >= uint64(options.modulo) {
		hash.value %= uint64(options.modulo)
	}

	hash.value += uint64(ch)
	if hash.value >= uint64(options.modulo) {
		hash.value %= uint64(options.modulo)
	}

	return hash
}

func (h PolynomialHash) PopLeft(ch int32, options PolynomialHashOptions) PolynomialHash {
	return h
}

type PolynomialHashOptions struct {
	parameter	uint32
	modulo		uint32
}

func CalculatePolynomialHashFromDataBlock(data string, options PolynomialHashOptions) PolynomialHash {
	hash := PolynomialHash{0}
	for _, ch := range data {
		hash = hash.Append(ch, options)
	}

	return hash
}

