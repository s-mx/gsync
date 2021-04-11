package sync

import "time"

var WindowLength int = 5

var HashOptions PolynomialHashOptions = CreatePolynomialHashOptions(1234, 1000000000 + 7, WindowLength)

var DestinationOptions DestinationTransmitterOptions = DestinationTransmitterOptions{
	hashOptions: HashOptions,
	N:           WindowLength,
	Timeout:     time.Millisecond * 10,
}
