package schema

// TypeName names a type in the type system.
type TypeName string

// Primitive types.
const (
	TypeString    TypeName = "string"
	TypeInteger   TypeName = "integer"
	TypeLong      TypeName = "long"
	TypeDecimal   TypeName = "decimal"
	TypeFloat     TypeName = "float"
	TypeBoolean   TypeName = "boolean"
	TypeNull      TypeName = "null"
	TypeUUID      TypeName = "uuid"
	TypeDate      TypeName = "date"
	TypeTime      TypeName = "time"
	TypeDateTime  TypeName = "datetime"
	TypeTimestamp TypeName = "timestamp"
	TypeBinary    TypeName = "binary"
)

// Semantic types.
const (
	TypeEmail         TypeName = "email"
	TypePhone         TypeName = "phone"
	TypeURL           TypeName = "url"
	TypeIPAddress     TypeName = "ip_address"
	TypeIPv4          TypeName = "ipv4"
	TypeIPv6          TypeName = "ipv6"
	TypeCountry       TypeName = "country"
	TypeCity          TypeName = "city"
	TypePostalCode    TypeName = "postal_code"
	TypeFirstName     TypeName = "first_name"
	TypeLastName      TypeName = "last_name"
	TypeFullName      TypeName = "full_name"
	TypeCompanyName   TypeName = "company_name"
	TypeAddress       TypeName = "address"
	TypeCurrency      TypeName = "currency"
	TypeMoney         TypeName = "money"
	TypeCreditCard    TypeName = "credit_card_test_value"
	TypePaymentStatus TypeName = "payment_status"
	TypeOrderStatus   TypeName = "order_status"
)

// Composite types.
const (
	TypeObject       TypeName = "object"
	TypeArray        TypeName = "array"
	TypeEnum         TypeName = "enum"
	TypeMap          TypeName = "map"
	TypeNestedSchema TypeName = "nested_schema"
	TypeReference    TypeName = "reference"
)

type typeClass uint8

const (
	classText     typeClass = iota // free or pattern text
	classNumeric                   // ordered numbers
	classTemporal                  // ordered points in time
	classBool
	classNull
	classUUID
	classBinary
	classSemantic // generated text with its own format
	classIP
	classEnum
	classCollection // array, map
	classStruct     // object, nested_schema
	classReference
)

// typeInfo lists every known type. Adding a type starts here.
var typeInfo = map[TypeName]typeClass{
	TypeString: classText,

	TypeInteger: classNumeric,
	TypeLong:    classNumeric,
	TypeDecimal: classNumeric,
	TypeFloat:   classNumeric,
	TypeMoney:   classNumeric,

	TypeDate:      classTemporal,
	TypeTime:      classTemporal,
	TypeDateTime:  classTemporal,
	TypeTimestamp: classTemporal,

	TypeBoolean: classBool,
	TypeNull:    classNull,
	TypeUUID:    classUUID,
	TypeBinary:  classBinary,

	TypeEmail:         classSemantic,
	TypePhone:         classSemantic,
	TypeURL:           classSemantic,
	TypeCountry:       classSemantic,
	TypeCity:          classSemantic,
	TypePostalCode:    classSemantic,
	TypeFirstName:     classSemantic,
	TypeLastName:      classSemantic,
	TypeFullName:      classSemantic,
	TypeCompanyName:   classSemantic,
	TypeAddress:       classSemantic,
	TypeCurrency:      classSemantic,
	TypeCreditCard:    classSemantic,
	TypePaymentStatus: classSemantic,
	TypeOrderStatus:   classSemantic,

	TypeIPAddress: classIP,
	TypeIPv4:      classIP,
	TypeIPv6:      classIP,

	TypeEnum:         classEnum,
	TypeArray:        classCollection,
	TypeMap:          classCollection,
	TypeObject:       classStruct,
	TypeNestedSchema: classStruct,
	TypeReference:    classReference,
}

// KnownType reports whether t is in the type system.
func KnownType(t TypeName) bool {
	_, ok := typeInfo[t]
	return ok
}

func (t TypeName) class() typeClass { return typeInfo[t] }

func (t TypeName) isOrdered() bool {
	c := t.class()
	return c == classNumeric || c == classTemporal
}

// isScalar reports whether a value of this type is a single column in tabular output.
func (t TypeName) isScalar() bool {
	switch t.class() {
	case classCollection, classStruct:
		return false
	}
	return true
}

// randomTypes are the types random generation can draw.
var randomTypes = map[TypeName]bool{
	TypeString: true, TypeInteger: true, TypeLong: true, TypeDecimal: true, TypeFloat: true,
	TypeBoolean: true, TypeNull: true, TypeUUID: true, TypeDate: true, TypeDateTime: true,
	TypeTimestamp: true, TypeObject: true, TypeArray: true,
}
