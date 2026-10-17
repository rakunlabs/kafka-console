package kafka

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
)

const testAvroSchema = `{"type":"record","name":"Order","namespace":"shop","fields":[
	{"name":"id","type":"long"},
	{"name":"count","type":"int"},
	{"name":"note","type":["null","string"],"default":null},
	{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
	{"name":"day","type":{"type":"int","logicalType":"date"}},
	{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
	{"name":"id2","type":{"type":"fixed","name":"UUID","size":16,"logicalType":"uuid"}},
	{"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},
	{"name":"hash","type":{"type":"fixed","name":"Hash","size":2}},
	{"name":"payload","type":["null","bytes"],"default":null},
	{"name":"wait","type":{"type":"fixed","name":"Wait","size":12,"logicalType":"duration"}},
	{"name":"tags","type":{"type":"array","items":"string"},"default":[]},
	{"name":"addr","type":["null","Address"],"default":null},
	{"name":"history","type":{"type":"array","items":"Address"},"default":[]}
]}`

const testAddressSchema = `{"type":"record","name":"Address","namespace":"shop","fields":[
	{"name":"city","type":"string"},
	{"name":"geo","type":["null",{"type":"fixed","name":"Geo","size":2}],"default":null}
]}`

func testCodec(t *testing.T) *avroCodec {
	t.Helper()

	c, err := parseAvro(testAvroSchema, []namedSchema{{name: "shop.Address", schema: sr.Schema{Schema: testAddressSchema}}})
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func roundTrip(t *testing.T, c *avroCodec, in string) map[string]any {
	t.Helper()

	var doc any
	if err := decodeJSON([]byte(in), &doc); err != nil {
		t.Fatal(err)
	}

	b, err := c.encode(doc)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	out, err := c.decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	// The UI shows the decoded value as JSON, so compare in that form.
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	if err := decodeJSON(raw, &m); err != nil {
		t.Fatal(err)
	}

	return m
}

const uuid = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

func TestAvroRoundTrip(t *testing.T) {
	c := testCodec(t)

	in := `{"id":9007199254740993,"count":3,"note":"hi","ts":"2026-10-17T10:00:00Z","day":"2026-10-17",
		"amount":"12.5","id2":"` + uuid + `","status":"PAID","hash":"AQI=","payload":"/wA=",
		"wait":{"months":1,"days":2,"milliseconds":3},
		"addr":{"city":"Istanbul","geo":"AQI="},"history":[{"city":"Ankara"}]}`

	out := roundTrip(t, c, in)

	checks := map[string]string{
		"id":      "9007199254740993",
		"amount":  `"12.50"`,
		"id2":     `"` + uuid + `"`,
		"day":     `"2026-10-17"`,
		"ts":      `"2026-10-17T10:00:00Z"`,
		"hash":    `"AQI="`,
		"payload": `"/wA="`,
	}
	for k, want := range checks {
		if got := jsonText(out[k]); got != want {
			t.Errorf("%s: got %s want %s", k, got, want)
		}
	}

	if got := jsonText(out["wait"]); got != `{"days":2,"milliseconds":3,"months":1}` {
		t.Errorf("duration: %s", got)
	}

	addr, _ := out["addr"].(map[string]any)
	if addr["city"] != "Istanbul" || addr["geo"] != "AQI=" {
		t.Errorf("referenced record: %v", out["addr"])
	}

	// What decoding shows must be accepted back for producing.
	raw, _ := json.Marshal(out)
	again := roundTrip(t, c, string(raw))
	if jsonText(again) != jsonText(out) {
		t.Errorf("display form does not round trip:\n%s\n%s", jsonText(out), jsonText(again))
	}
}

func TestAvroTaggedUnions(t *testing.T) {
	c := testCodec(t)

	out := roundTrip(t, c, `{"id":1,"count":1,"note":{"string":"x"},"ts":5,"day":0,"amount":3,
		"id2":"`+uuid+`","status":"NEW","hash":"AQI=","payload":{"bytes":"AQI="},
		"wait":{"months":0,"days":0,"milliseconds":0},"addr":{"shop.Address":{"city":"a"}}}`)

	if out["note"] != "x" || out["payload"] != "AQI=" {
		t.Errorf("tagged unions: %v %v", out["note"], out["payload"])
	}

	if addr, _ := out["addr"].(map[string]any); addr["city"] != "a" {
		t.Errorf("tagged record: %v", out["addr"])
	}
}

func TestAvroErrors(t *testing.T) {
	c := testCodec(t)

	base := map[string]string{
		"id": `1`, "count": `1`, "ts": `1`, "day": `0`, "amount": `1`, "id2": `"` + uuid + `"`,
		"status": `"NEW"`, "hash": `"AQI="`, "wait": `{}`,
	}

	for name, tc := range map[string]struct{ field, value, want string }{
		"int range":  {"count", `3000000000`, "count"},
		"fraction":   {"id", `1.5`, "id"},
		"enum":       {"status", `"X"`, "status"},
		"fixed size": {"hash", `"AQID"`, "hash"},
		"scale":      {"amount", `"1.234"`, "amount"},
		"missing":    {"id", ``, "id"},
		"bad nested": {"addr", `{"city":5}`, "addr"},
	} {
		parts := []string{}
		for k, v := range base {
			if k == tc.field {
				continue
			}

			parts = append(parts, `"`+k+`":`+v)
		}

		if tc.value != "" {
			parts = append(parts, `"`+tc.field+`":`+tc.value)
		}

		var doc any
		if err := decodeJSON([]byte("{"+strings.Join(parts, ",")+"}"), &doc); err != nil {
			t.Fatal(err)
		}

		_, err := c.encode(doc)
		if err == nil {
			t.Errorf("%s: expected error", name)
		} else if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error should name %q: %v", name, tc.want, err)
		}
	}
}

func jsonText(v any) string {
	b, _ := json.Marshal(v)

	return string(b)
}
