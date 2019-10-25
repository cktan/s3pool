/*
 *  S3pool - S3 cache on local disk
 *  Copyright (c) 2019 CK Tan
 *  cktanx@gmail.com
 *
 *  S3Pool can be used for free under the GNU General Public License
 *  version 3, where anything released into public must be open source,
 *  or under a commercial license. The commercial license does not
 *  cover derived or ported versions created by third parties under
 *  GPL. To inquire about commercial license, please send email to
 *  cktan@gmail.com.
 */
package tcp_server

import (
	"bufio"
	"crypto/tls"
	"log"
	"net"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// Client holds info about connection
type Client struct {
	conn   net.Conn
	Server *server
}

// TCP server
type server struct {
	address  string // Address to open connection: localhost:9999
	config   *tls.Config
	callback func(c *Client, message string)
}

// Read client data from channel
func (c *Client) accepted() {
	defer c.conn.Close()
	c.conn.SetReadDeadline(time.Now().Add(time.Second))
	reader := bufio.NewReader(c.conn)
	req, _ := reader.ReadString('\n')
	req = strings.Trim(req, " \n\t\r")
	// ignore empty request
	if req != "" {
		c.Server.callback(c, req)
	}
}

// Send text message to client
func (c *Client) Send(message string) error {
	_, err := c.conn.Write([]byte(message))
	return err
}

// Send bytes to client
func (c *Client) SendBytes(b []byte) error {
	_, err := c.conn.Write(b)
	return err
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Listen and serve
func (s *server) Loop() error {
	// ignore sigpipe
	signal.Ignore(syscall.SIGPIPE)

	var listener net.Listener
	var err error
	if s.config == nil {
		listener, err = net.Listen("tcp", s.address)
	} else {
		listener, err = tls.Listen("tcp", s.address, s.config)
	}
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		conn, _ := listener.Accept()
		//syscall.SetsockoptInt(conn, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		client := &Client{
			conn:   conn,
			Server: s,
		}
		go client.accepted()
	}
	return nil
}

// Creates new tcp server instance
func New(address string, callback func(c *Client, message string)) *server {
	log.Println("Starting server at", address)
	server := &server{
		address: address,
		config:  nil,
	}

	server.callback = callback

	return server
}

func NewWithTLS(address string, certFile string, keyFile string,
	callback func(c *Client, message string)) *server {
	log.Println("Starting server at", address)
	cert, _ := tls.LoadX509KeyPair(certFile, keyFile)
	config := tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	server := &server{
		address: address,
		config:  &config,
	}

	server.callback = callback

	return server
}
