/*
Copyright 2024 eventbus Author(s)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package eventbus

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidSubject = errors.New("sublist: invalid subject")
	ErrNotSupport     = errors.New("sublist: not support")
	ErrSublistNil     = errors.New("sublist: sublist is nil")
	ErrNotFound       = errors.New("sublist: not found")
	ErrSlowConsumer   = errors.New("subjection: slow consumer")
)

type subjectError struct {
	subject string
	err     error
}

// Error
func (e *subjectError) Error() string {
	return fmt.Sprintf("subject %s error: %v", e.subject, e.err)
}

// Is
func (e *subjectError) Is(target error) bool {
	return errors.Is(e.err, target)
}

type slowConsumerErr struct {
	count int
}

// Error
func (e slowConsumerErr) Error() string {
	return fmt.Sprintf("%s count %d", ErrSlowConsumer.Error(), e.count)
}

// Is
func (e slowConsumerErr) Is(target error) bool {
	return errors.Is(target, ErrSlowConsumer)
}
