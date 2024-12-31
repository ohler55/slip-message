module github.com/ohler55/slip-message

go 1.23

toolchain go1.23.2

require (
	github.com/nats-io/nats-server/v2 v2.10.12
	github.com/nats-io/nats.go v1.34.1
	github.com/ohler55/ojg v1.26.0
	github.com/ohler55/slip v0.9.8
)

require (
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/nats-io/jwt/v2 v2.5.5 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/term v0.21.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	golang.org/x/time v0.5.0 // indirect
)

replace github.com/ohler55/slip => ../slip
