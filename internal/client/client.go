package client

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin" // added
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	"github.com/let-mil-go/proto/mulberrypb"
	"github.com/let-mil-go/proto/shardpb"
)

type Client struct {
	conn   *grpc.ClientConn
	client mulberrypb.MulberryServiceClient
}

// NewClient creates a new client connection to the mulberry service
func NewClient(addresses []string) (*Client, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}

	target := fmt.Sprintf("%s:///%s", mulberryScheme, strings.Join(addresses, ","))

	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return &Client{
		conn:   conn,
		client: mulberrypb.NewMulberryServiceClient(conn),
	}, nil
}

// Close closes the connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// BeginTransaction starts a new transaction
func (c *Client) BeginTransaction(ctx context.Context, isolation shardpb.IsolationLevel) (string, error) {
	resp, err := c.client.BeginTransaction(ctx, &mulberrypb.BeginTransactionRequest{
		IsolationLevel: isolation,
	})
	if err != nil {
		return "", err
	}
	return resp.TxId, nil
}

// Read reads a value for a key within a transaction
func (c *Client) Read(ctx context.Context, txID, key string) (string, bool, error) {
	resp, err := c.client.Read(ctx, &mulberrypb.ReadRequest{
		TxId: txID,
		Key:  key,
	})
	if err != nil {
		return "", false, err
	}
	return resp.Value, resp.Found, nil
}

// Write writes a value for a key within a transaction
func (c *Client) Write(ctx context.Context, txID, key, value string) error {
	_, err := c.client.Write(ctx, &mulberrypb.WriteRequest{
		TxId:  txID,
		Key:   key,
		Value: value,
	})
	return err
}

// Delete deletes a key within a transaction
func (c *Client) Delete(ctx context.Context, txID, key string) error {
	_, err := c.client.Delete(ctx, &mulberrypb.DeleteRequest{
		TxId: txID,
		Key:  key,
	})
	return err
}

// Commit commits the transaction
func (c *Client) Commit(ctx context.Context, txID string) error {
	_, err := c.client.Commit(ctx, &mulberrypb.CommitRequest{
		TxId: txID,
	})
	return err
}

// Abort aborts the transaction
func (c *Client) Abort(ctx context.Context, txID string) error {
	_, err := c.client.Abort(ctx, &mulberrypb.AbortRequest{
		TxId: txID,
	})
	return err
}

// Resolver implementation
const mulberryScheme = "mulberry"

func init() {
	resolver.Register(&mulberryBuilder{})
}

type mulberryBuilder struct{}

func (*mulberryBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &mulberryResolver{
		cc: cc,
	}
	r.start(target.Endpoint())
	return r, nil
}

func (*mulberryBuilder) Scheme() string { return mulberryScheme }

type mulberryResolver struct {
	cc resolver.ClientConn
}

func (r *mulberryResolver) start(endpoint string) {
	addrStrs := strings.Split(endpoint, ",")
	var addrs []resolver.Address
	for _, addr := range addrStrs {
		if addr != "" {
			addrs = append(addrs, resolver.Address{Addr: addr})
		}
	}
	err := r.cc.UpdateState(resolver.State{Addresses: addrs})
	if err != nil {
		return
	}
}

func (*mulberryResolver) ResolveNow(o resolver.ResolveNowOptions) {}
func (*mulberryResolver) Close()                                  {}
