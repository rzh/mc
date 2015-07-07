package parser

import (
	"io/ioutil"
	"log"
	"regexp"
	"strconv"
	"strings"
)

type YCSBResult struct {
	CmdLine    string
	Threads    string
	Throughput float64
}

//
// Command line: -db com.yahoo.ycsb.db.MongoDbClient -s -P workloads/workloadShardDirect -p mongodb.url=10.2.1.99:27017 -threads 64 -t
// ...
// [OVERALL], Throughput(ops/sec), 58782.55451347149

func ProcessYCSBResult(file string) (throughput float64, cmdline string, threads string) {
	f, err := ioutil.ReadFile(file)

	if err != nil {
		log.Fatal("cannot open file " + file)
	}

	lines := strings.Split(string(f), "\n")

	for i := 0; i < len(lines); i++ {
		if strings.Index(lines[i], "Command line:") >= 0 {
			cmdline = lines[i]

			re := regexp.MustCompile("-threads ([1-9][0-9]*)")
			matches := re.FindStringSubmatch(cmdline)

			if len(matches) == 0 {
				log.Fatal("Cannot part YCSB command line: " + cmdline)
			}

			threads = matches[1]

		} else if strings.Index(lines[i], "[OVERALL], Throughput(ops/sec)") >= 0 {
			s := strings.Split(lines[i], ",")
			t, err := strconv.ParseFloat(strings.Trim(s[2], " "), 64)

			if err == nil {
				throughput = t

				// found throughtput for Overall, can return now

				return
			} else {
				log.Fatal("failed to parse throughput for YCSB: " + lines[i])
			}
		}
	}

	log.Fatal("failed to find results for YCSB run")
	return
}
