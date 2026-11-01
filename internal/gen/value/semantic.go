package value

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"github.com/genadigeno/data-feed/internal/gen/unique"
	"github.com/genadigeno/data-feed/internal/record"
)

// Semantic values are synthetic by construction: reserved domains (RFC 2606), reserved
// documentation IP ranges (RFC 5737, RFC 3849), fictional phone numbers, and published
// card test numbers. The name lists are invented combinations, not people.

func pick(c *Ctx, list []string) string { return list[c.Rand.IntN(len(list))] }

type listGen []string

func (g listGen) Generate(c *Ctx, out *record.Value) error {
	out.SetString(pick(c, g))
	return nil
}

func emailLocal(c *Ctx) string {
	return strings.ToLower(pick(c, firstNames) + "." + pick(c, lastNames))
}

type emailGen struct{}

func (emailGen) Generate(c *Ctx, out *record.Value) error {
	out.SetString(emailLocal(c) + strconv.Itoa(c.Rand.IntN(1000)) + "@" + pick(c, reservedDomains))
	return nil
}

func (emailGen) UniqueSpace() uint64 { return 0 }

// At is injective: the part after the last dot before @ holds p, and only p.
func (emailGen) At(c *Ctx, p uint64, out *record.Value) error {
	buf := append(c.Scratch[:0], emailLocal(c)...)
	buf = append(buf, '.')
	buf = unique.AppendBase36(buf, p, 0)
	buf = append(buf, '@')
	buf = append(buf, pick(c, reservedDomains)...)
	out.SetString(string(buf))
	c.Scratch = buf[:0]
	return nil
}

type urlGen struct{}

func (urlGen) Generate(c *Ctx, out *record.Value) error {
	out.SetString("https://" + pick(c, reservedDomains) + "/" + pick(c, urlWords) + "/" + pick(c, urlWords))
	return nil
}

func (urlGen) UniqueSpace() uint64 { return 0 }

func (urlGen) At(c *Ctx, p uint64, out *record.Value) error {
	buf := append(c.Scratch[:0], "https://"...)
	buf = append(buf, pick(c, reservedDomains)...)
	buf = append(buf, '/')
	buf = append(buf, pick(c, urlWords)...)
	buf = append(buf, '/')
	buf = unique.AppendBase36(buf, p, 0)
	out.SetString(string(buf))
	c.Scratch = buf[:0]
	return nil
}

const (
	phoneAreaCodes   = 800 // area codes 200 to 999
	phoneLineNumbers = 100 // 555-0100 to 555-0199, reserved for fiction
)

// phoneGen uses the North American numbers reserved for fictional use.
type phoneGen struct{}

func (phoneGen) Generate(c *Ctx, out *record.Value) error {
	return phoneGen{}.At(c, c.Rand.Uint64N(phoneAreaCodes*phoneLineNumbers), out)
}

func (phoneGen) UniqueSpace() uint64 { return phoneAreaCodes * phoneLineNumbers }

func (phoneGen) At(_ *Ctx, p uint64, out *record.Value) error {
	out.SetString(fmt.Sprintf("+1-%03d-555-01%02d", 200+p/phoneLineNumbers, p%phoneLineNumbers))
	return nil
}

// The three IPv4 networks reserved for documentation.
var ipv4Documentation = [...][3]byte{{192, 0, 2}, {198, 51, 100}, {203, 0, 113}}

type ipv4Gen struct{ any bool }

func (g ipv4Gen) Generate(c *Ctx, out *record.Value) error {
	return g.At(c, c.Rand.Uint64N(g.UniqueSpace()), out)
}

func (g ipv4Gen) UniqueSpace() uint64 {
	if g.any {
		return 1 << 32
	}
	return uint64(len(ipv4Documentation)) * 256
}

func (g ipv4Gen) At(_ *Ctx, p uint64, out *record.Value) error {
	var a [4]byte
	if g.any {
		binary.BigEndian.PutUint32(a[:], uint32(p))
	} else {
		n := ipv4Documentation[p/256]
		a = [4]byte{n[0], n[1], n[2], byte(p % 256)}
	}
	out.SetString(netip.AddrFrom4(a).String())
	return nil
}

// ipv6Gen uses 2001:db8::/32, reserved for documentation, unless any is set. ip_address
// mixes in IPv4 when drawing freely; unique values stay IPv6, which never run out.
type ipv6Gen struct {
	any   bool
	mixed bool
}

func (g ipv6Gen) Generate(c *Ctx, out *record.Value) error {
	if g.mixed && c.Rand.Uint64()&1 == 1 {
		return ipv4Gen{any: g.any}.Generate(c, out)
	}
	return g.At(c, c.Rand.Uint64(), out)
}

func (ipv6Gen) UniqueSpace() uint64 { return 0 }

func (g ipv6Gen) At(c *Ctx, p uint64, out *record.Value) error {
	var a [16]byte
	binary.BigEndian.PutUint64(a[:8], c.Rand.Uint64())
	binary.BigEndian.PutUint64(a[8:], p)
	if !g.any {
		a[0], a[1], a[2], a[3] = 0x20, 0x01, 0x0d, 0xb8
	}
	out.SetString(netip.AddrFrom16(a).String())
	return nil
}

type postalGen struct{}

func (postalGen) Generate(c *Ctx, out *record.Value) error {
	out.SetString(fmt.Sprintf("%05d", c.Rand.IntN(100000)))
	return nil
}

type fullNameGen struct{}

func (fullNameGen) Generate(c *Ctx, out *record.Value) error {
	out.SetString(pick(c, firstNames) + " " + pick(c, lastNames))
	return nil
}

type companyGen struct{}

func (companyGen) Generate(c *Ctx, out *record.Value) error {
	out.SetString(pick(c, companyWords) + " " + pick(c, companyWords) + " " + pick(c, companySuffixes))
	return nil
}

type addressGen struct{}

func (addressGen) Generate(c *Ctx, out *record.Value) error {
	out.SetString(strconv.Itoa(1+c.Rand.IntN(9999)) + " " + pick(c, streetNames) + " " + pick(c, streetTypes))
	return nil
}

var (
	firstNames = []string{
		"Ana", "Ben", "Carla", "David", "Elena", "Farid", "Gia", "Hugo", "Ines", "Jonas", "Kira", "Luca", "Maya", "Nico",
		"Olga", "Pavel", "Quinn", "Rosa", "Sami", "Tara", "Uma", "Viktor", "Wen", "Ximena", "Yusuf", "Zoe", "Amir", "Bea",
		"Cyrus", "Dara", "Emil", "Freya", "Goran", "Hana", "Ivan", "Jada", "Kofi", "Lena", "Milo", "Nora",
	}
	lastNames = []string{
		"Adler", "Baptiste", "Castillo", "Demir", "Eriksen", "Fontaine", "Garcia", "Haddad", "Ivanov", "Jensen", "Kovacs",
		"Larsen", "Moreau", "Nakamura", "Okafor", "Petrov", "Quintero", "Rossi", "Silva", "Tanaka", "Ueda", "Vargas",
		"Weber", "Xu", "Yilmaz", "Zhou", "Abara", "Bianchi", "Costa", "Dubois", "Evans", "Fischer", "Gomez", "Horvath",
	}
	reservedDomains = []string{"example.com", "example.net", "example.org", "mail.example", "shop.test"}
	urlWords        = []string{
		"products", "orders", "account", "help", "news", "about", "items", "search", "docs", "profile", "cart", "offers",
	}
	countries = []string{
		"Argentina", "Australia", "Austria", "Belgium", "Brazil", "Canada", "Chile", "Denmark", "Egypt", "Finland", "France",
		"Georgia", "Germany", "Greece", "India", "Italy", "Japan", "Kenya", "Mexico", "Morocco", "Netherlands", "Norway",
		"Poland", "Portugal", "Spain", "Sweden", "Thailand", "Turkey", "Ukraine", "Uruguay",
	}
	cities = []string{
		"Amsterdam", "Athens", "Barcelona", "Berlin", "Bogota", "Cairo", "Chicago", "Dublin", "Geneva", "Istanbul", "Kyoto",
		"Lagos", "Lima", "Lisbon", "London", "Madrid", "Melbourne", "Montreal", "Mumbai", "Nairobi", "Oslo", "Paris",
		"Prague", "Santiago", "Seoul", "Stockholm", "Tbilisi", "Tokyo", "Vienna", "Warsaw",
	}
	companyWords = []string{
		"Acme", "Blue", "Cedar", "Delta", "Everest", "Falcon", "Global", "Harbor", "Ion", "Juniper", "Keystone", "Lumen",
		"Meridian", "North", "Orchid", "Pioneer", "Quartz", "River", "Summit", "Terra",
	}
	companySuffixes = []string{"Labs", "Systems", "Group", "Holdings", "Industries", "Partners", "Logistics", "Foods", "Media", "Works"}
	streetNames     = []string{
		"Maple", "Oak", "Cedar", "Pine", "Elm", "Lake", "Hill", "Park", "River", "Sunset", "Station", "Church", "Market",
		"Garden", "Forest", "Harbor", "Mill", "Spring", "Bridge", "Meadow",
	}
	streetTypes = []string{"St", "Ave", "Rd", "Blvd", "Ln", "Way", "Ct", "Pl"}
	currencies  = []string{"USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "SEK", "NOK", "DKK", "PLN", "GEL", "BRL", "INR", "ZAR"}
	// testCardNumbers are the numbers card networks publish for testing. They authorize
	// nothing: never random numbers that might belong to someone.
	testCardNumbers = []string{
		"4111111111111111", "4012888888881881", "4242424242424242", "5555555555554444", "5105105105105100",
		"378282246310005", "371449635398431", "6011111111111117", "6011000990139424", "3530111333300000",
	}
	paymentStatuses = []string{"PENDING", "AUTHORIZED", "CAPTURED", "FAILED", "REFUNDED", "CANCELLED"}
	orderStatuses   = []string{"CREATED", "PAID", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"}
)
