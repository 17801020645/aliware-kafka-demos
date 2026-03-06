GO_FLAGS = -mod=mod -tags dynamic

.PHONY: producer consumer all

producer:
	go run $(GO_FLAGS) producer/producer.go

consumer:
	go run $(GO_FLAGS) consumer/consumer.go

all:
	$(MAKE) -j 2 producer consumer

