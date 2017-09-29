package utils

import (
	"github.com/gobwas/glob"
	"net"
	"strings"
)

func SplitUrl(url, defaultPort string) (string, string) {
	parts := strings.Split(url, ":")

	if len(parts) == 1 {
		return parts[0], defaultPort
	}

	if len(parts) == 2 {
		return parts[0], parts[1]
	}

	//unknown url format
	return "", ""
}

func AddPort(ips []string, port string) []string {
	res := make([]string, len(ips))

	for _, ip := range ips {
		res = append(res, ip+":"+port)
	}

	return res
}

func Contains(values []string, value string) bool {
	for _, v := range values {
		if value == v {
			return true
		}
	}

	return false
}

func AnyMatches(patterns []glob.Glob, value string) bool {

	for _, glob := range patterns {
		if glob.Match(value) {
			return true
		}
	}

	return false
}

func FindIPs() ([]string, error) {

	result := make([]string, 1)

	result[0] = "localhost"

	ifaces, err := net.Interfaces()

	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()

		if err != nil {
			return nil, err
		}

		for _, addr := range addrs {
			var ip net.IP

			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil {
				continue
			}

			ip = ip.To4()

			if ip == nil {
				continue // not an ipv4 address
			}

			result = append(result, ip.String())
		}
	}

	return result, nil
}
