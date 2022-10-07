package beppo

import (
	"reflect"
	"testing"

	"cloud.google.com/go/storage"
)

func TestNewClient(t *testing.T) {
	type args struct {
		storageClient *storage.Client
	}
	tests := []struct {
		name string
		args args
		want *Client
	}{
		{"No storage client", args{}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewClient(tt.args.storageClient); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewClient() = %v, want %v", got, tt.want)
			}
		})
	}
}
