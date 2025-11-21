LOCAL_BIN:=$(CURDIR)/bin

install-dephs:
	GOBIN=$(LOCAL_BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	GOBIN=$(LOCAL_BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest


go-get-dephs:
	go get -u google.golang.org/protobuf/cmd/protoc-gen-go
	go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc
	go get -u google.golang.org/grpc
	go get -u google.golang.org/grpc/codes
	go get -u google.golang.org/grpc/status 
	go get github.com/redis/go-redis/v9
	go get -u google.golang.org/protobuf/reflect/protoreflect
	go get -u google.golang.org/protobuf/runtime/protoimpl
	go get -u google.golang.org/protobuf/types/known/durationpb
	go get -u google.golang.org/protobuf/types/known/timestamppb


generate-proto:
	mkdir -p pb/cron_v1
	protoc --proto_path proto/cron_v1 \
	--go_out=pb/cron_v1 --go_opt=paths=source_relative \
	--plugin=protoc-gen-go=bin/protoc-gen-go \
	--go-grpc_out=pb/cron_v1 --go-grpc_opt=paths=source_relative \
	--plugin=protoc-gen-go-grpc=bin/protoc-gen-go-grpc \
	proto/cron_v1/cron.proto