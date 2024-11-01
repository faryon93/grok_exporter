// Copyright 2016-2019 The grok_exporter Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tailer

import (
	"errors"
	configuration "github.com/fstab/grok_exporter/config/v3"
	"github.com/fstab/grok_exporter/tailer/fswatcher"
	"github.com/sirupsen/logrus"
	"gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mcuadros/go-syslog.v2/format"
	"log"
)

var (
	ErrNoContent = errors.New("no content field")
)

type rsyslogTailer struct {
	lines  chan *fswatcher.Line
	errors chan fswatcher.Error

	syslog *syslog.Server
}

func (r *rsyslogTailer) Lines() chan *fswatcher.Line {
	return r.lines
}

func (r *rsyslogTailer) Errors() chan fswatcher.Error {
	return r.errors
}

func (r *rsyslogTailer) Close() {
	err := r.syslog.Kill()
	if err != nil {
		logrus.Errorln("failed to kill syslog server:", err.Error())
		return
	}
}

func (r *rsyslogTailer) Handle(message format.LogParts, t int64, syslogErr error) {
	// parse the syslog message and make sure everything exists
	content, ok := message["content"].(string)
	if !ok {
		r.errors <- fswatcher.NewError(fswatcher.NotSpecified, ErrNoContent, "")
		return
	}

	log.Println(content)

	r.lines <- &fswatcher.Line{Line: content}
}

func RunRsyslogTailer(cfg *configuration.InputConfig) fswatcher.FileTailer {
	tailer := &rsyslogTailer{
		lines:  make(chan *fswatcher.Line),
		errors: make(chan fswatcher.Error),
	}

	// configure the syslog server
	tailer.syslog = syslog.NewServer()
	tailer.syslog.SetFormat(syslog.RFC3164)
	tailer.syslog.SetHandler(tailer)

	// TODO: make listen addr configurable
	err := tailer.syslog.ListenUDP("0.0.0.0:3219")
	if err != nil {
		logrus.Errorln("failed to listen:", err.Error())
		return tailer
	}

	err = tailer.syslog.Boot()
	if err != nil {
		logrus.Errorln("failed to boot syslog server:", err.Error())
		return tailer
	}

	return tailer
}
