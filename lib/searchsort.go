// Copyright 2013 Matthew Baird
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elastigo

import "encoding/json"

// SortDsl accepts any number of Sort commands
//
//     Query().Sort(
//         Sort("last_name").Desc(),
//         Sort("age"),
//     )
func Sort(field string) *SortDsl {
	return &SortDsl{Name: field}
}

type SortBody []interface{}
type SortDsl struct {
	Name    string
	IsDesc  bool
	Missing string
}

func (s *SortDsl) Desc() *SortDsl {
	s.IsDesc = true
	return s
}
func (s *SortDsl) Asc() *SortDsl {
	s.IsDesc = false
	return s
}

func (s *SortDsl) MarshalJSON() ([]byte, error) {
	order := map[string]string{}

	if s.IsDesc {
		order["order"] = "desc"
	} else {
		order["order"] = "asc"
	}

	if s.Missing == "_first" || s.Missing == "_last" {
		order["missing"] = s.Missing
	}

	return json.Marshal(map[string]interface{}{s.Name: order})
}
