package config

import "testing"

func TestFeaturesDefaultOn(t *testing.T) {
	on, off := true, false

	tests := []struct {
		name string
		in   *bool
		want bool
	}{
		{"unset", nil, true},
		{"true", &on, true},
		{"false", &off, false},
	}

	checks := map[string]func(*bool) bool{
		"security":       func(b *bool) bool { return Features{Security: b}.SecurityEnabled() },
		"create_topic":   func(b *bool) bool { return Features{CreateTopic: b}.CreateTopicEnabled() },
		"delete_topic":   func(b *bool) bool { return Features{DeleteTopic: b}.DeleteTopicEnabled() },
		"alter_topic":    func(b *bool) bool { return Features{AlterTopic: b}.AlterTopicEnabled() },
		"delete_records": func(b *bool) bool { return Features{DeleteRecords: b}.DeleteRecordsEnabled() },
		"delete_group":   func(b *bool) bool { return Features{DeleteGroup: b}.DeleteGroupEnabled() },
	}

	for feature, enabled := range checks {
		for _, tt := range tests {
			t.Run(feature+"/"+tt.name, func(t *testing.T) {
				if got := enabled(tt.in); got != tt.want {
					t.Fatalf("enabled = %v, want %v", got, tt.want)
				}
			})
		}
	}
}
