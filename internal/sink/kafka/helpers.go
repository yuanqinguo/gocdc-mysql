package kafka

import (
	"crypto/x509"
	"os"
)

// 本文件聚合 os/crypto 等小工具，保持 kafka.go 聚焦语义。

func osGetenv(k string) string { return os.Getenv(k) }

func osReadFile(p string) ([]byte, error) { return os.ReadFile(p) }

func certPool() *x509.CertPool { return x509.NewCertPool() }
