package opaxos

import (
	"github.com/ailidani/paxi"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

const TestConfigLocation = "../bin/configs/config_opaxos_test.json"

func TestNormalRun(t *testing.T) {
	*paxi.IsLogStdOut = true
	*paxi.ConfigFile = TestConfigLocation
	paxi.Init()

	id1 := paxi.NewID(1,1)
	id2 := paxi.NewID(1,2)
	id3 := paxi.NewID(1,3)
	id4 := paxi.NewID(1,4)
	id5 := paxi.NewID(1,5)

	node1 := NewReplica(id1)
	node2 := NewReplica(id2)
	node3 := NewReplica(id3)
	node4 := NewReplica(id4)
	node5 := NewReplica(id5)

	go node1.RunWithWorker()
	go node2.RunWithWorker()
	go node3.RunWithWorker()
	go node4.RunWithWorker()
	go node5.RunWithWorker()

	chosenValueCommand := paxi.DBCommandPut{
		CommandID: 0,
		SentAt:    time.Now().UnixNano(),
		Key:       313,
		Value:     []byte("secret value"),
	}
	chosenValueBuffer := chosenValueCommand.Serialize()
	shares, ssTime, err := node1.defaultSSWorker.SecretShareCommand(chosenValueBuffer)
	if err != nil {
		t.Error(err)
	}

	// node1 starting phase-1
	node1.Prepare()
	time.Sleep(1 * time.Second)
	if !node1.isLeader {
		t.Errorf("node1 should be a leader")
	}

	// node1 starting phase-2
	node1.Propose(&SecretSharedCommand{
		ClientCommand: &paxi.ClientCommand{
			CommandType: paxi.TypeDBPutCommand,
			RawCommand:  chosenValueBuffer,
			ReplyStream: nil,
		},
		SSTime:        ssTime,
		Shares:        shares,
	})

	// wait until command is committed and executed in node-2
	time.Sleep(1 * time.Second)

	// read the committed value from node-2
	committedValue := node2.Get(313)
	if string(committedValue) != "secret value" {
		t.Errorf("wrong comitted value %s", string(committedValue))
	}
}

// TestRecoveryProcess run a scenario with 5 nodes, the first 2 nodes are trusted
// and the remaining are not. Events in the test:
// - node1 becomes the leader
// - node1 proposes a secret value to slot=0,
//   all the nodes receive the proposed secret-share, but they don't respond back to node1
// - node2 runs phase1, then recover the previously proposed secret-value from node1
func TestRecoveryProcess(t *testing.T) {
	*paxi.IsLogStdOut = true
	*paxi.ConfigFile = TestConfigLocation
	paxi.Init()

	id1 := paxi.NewID(1,1)
	id2 := paxi.NewID(1,2)
	id3 := paxi.NewID(1,3)
	id4 := paxi.NewID(1,4)
	id5 := paxi.NewID(1,5)

	node1 := NewReplica(id1)
	node2 := NewReplica(id2)
	node3 := NewReplica(id3)
	node4 := NewReplica(id4)
	node5 := NewReplica(id5)

	go node1.RunWithWorker()
	go node2.RunWithWorker()
	go node3.RunWithWorker()
	go node4.RunWithWorker()
	go node5.RunWithWorker()

	chosenValueCommand := paxi.DBCommandPut{
		CommandID: 0,
		SentAt:    time.Now().UnixNano(),
		Key:       313,
		Value:     []byte("secret value"),
	}
	chosenValueBuffer := chosenValueCommand.Serialize()
	shares, ssTime, err := node1.defaultSSWorker.SecretShareCommand(chosenValueBuffer)
	if err != nil {
		t.Error(err)
	}

	// node1 does leader election (phase 1)
	node1.Prepare()
	time.Sleep(1 * time.Second)
	if !node1.isLeader {
		t.Errorf("node1 should be a leader")
	}

	// drop all messages from other nodes to node 1
	node2.Drop(id1, 2)
	node3.Drop(id1, 2)
	node4.Drop(id1, 2)
	node5.Drop(id1, 2)

	// node1 starting phase-2
	node1.Propose(&SecretSharedCommand{
		ClientCommand: &paxi.ClientCommand{
			CommandType: paxi.TypeDBPutCommand,
			RawCommand:  chosenValueBuffer,
			ReplyStream: nil,
		},
		SSTime:        ssTime,
		Shares:        shares,
	})

	// wait until command is proposed to node 2-5
	time.Sleep(1 * time.Second)

	// node2 starting leader election
	node2.Prepare()

	// waiting until the recovery process is done in node2
	time.Sleep(3 * time.Second)

	// read the committed value from node2
	committedValue := node2.Get(313)
	if string(committedValue) != "secret value" {
		t.Errorf("wrong comitted value %s", string(committedValue))
	}
}

// TestRecoveryMultipleSlots is similar as TestRecoveryProcess, but in this test
// node1 proposes secret-values to multiple slots. In the end, node2 should be
// able to recover secret-value in all slots.
func TestRecoveryMultipleSlots(t *testing.T) {
	*paxi.IsLogStdOut = true
	*paxi.ConfigFile = TestConfigLocation
	paxi.Init()

	id1 := paxi.NewID(1,1)
	id2 := paxi.NewID(1,2)
	id3 := paxi.NewID(1,3)
	id4 := paxi.NewID(1,4)
	id5 := paxi.NewID(1,5)

	node1 := NewReplica(id1)
	node2 := NewReplica(id2)
	node3 := NewReplica(id3)
	node4 := NewReplica(id4)
	node5 := NewReplica(id5)

	go node1.RunWithWorker()
	go node2.RunWithWorker()
	go node3.RunWithWorker()
	go node4.RunWithWorker()
	go node5.RunWithWorker()

	// preparing to-be proposed commands
	chosenValueCommand1 := paxi.DBCommandPut{
		CommandID: 0,
		SentAt:    time.Now().UnixNano(),
		Key:       313,
		Value:     []byte("secret value"),
	}
	chosenValueCommand2 := paxi.DBCommandPut{
		CommandID: 1,
		SentAt:    time.Now().UnixNano(),
		Key:       354,
		Value:     []byte("another secret value, but longer..."),
	}
	chosenValueBuffer1 := chosenValueCommand1.Serialize()
	chosenValueBuffer2 := chosenValueCommand2.Serialize()
	shares1, ssTime1, _ := node1.defaultSSWorker.SecretShareCommand(chosenValueBuffer1)
	shares2, ssTime2, _ := node1.defaultSSWorker.SecretShareCommand(chosenValueBuffer2)
	secretSharedCommand1 := &SecretSharedCommand{
		ClientCommand: &paxi.ClientCommand{
			CommandType: paxi.TypeDBPutCommand,
			RawCommand:  chosenValueBuffer1,
			ReplyStream: nil,
		},
		SSTime:        ssTime1,
		Shares:        shares1,
	}
	secretSharedCommand2 := &SecretSharedCommand{
		ClientCommand: &paxi.ClientCommand{
			CommandType: paxi.TypeDBPutCommand,
			RawCommand:  chosenValueBuffer2,
			ReplyStream: nil,
		},
		SSTime:        ssTime2,
		Shares:        shares2,
	}

	// node1 does leader election (phase 1)
	node1.Prepare()
	time.Sleep(1 * time.Second)
	if !node1.isLeader {
		t.Errorf("node1 should be a leader")
	}

	// drop all messages from other nodes to node 1
	node2.Drop(id1, 2)
	node3.Drop(id1, 2)
	node4.Drop(id1, 2)
	node5.Drop(id1, 2)

	// node1 starting phase-2
	node1.Propose(secretSharedCommand1)
	node1.Propose(secretSharedCommand2)
	node1.Propose(secretSharedCommand1)
	node1.Propose(secretSharedCommand2)

	// wait until command is proposed to node 2-5
	time.Sleep(3 * time.Second)

	// node2 starting leader election
	node2.Prepare()

	// waiting until the recovery process is done in node2
	time.Sleep(5 * time.Second)

	// read the committed value from node2
	committedValue := node2.Get(313)
	if string(committedValue) != "secret value" {
		t.Errorf("wrong comitted value: %s", string(committedValue))
	}
	committedValue = node2.Get(354)
	if string(committedValue) != "another secret value, but longer..." {
		t.Errorf("wrong comitted value: %s", string(committedValue))
	}

	// read the committed value from node1
	committedValue = node1.Get(313)
	if string(committedValue) != "secret value" {
		t.Errorf("wrong comitted value: %s", string(committedValue))
	}
}

func TestShamirWorkerThroughput(t *testing.T)  {
	numWorker := runtime.NumCPU()
	wg := sync.WaitGroup{}
	n := 1_000_000

	workers := make([]SecretSharingWorker, numWorker)
	for i := 0; i < numWorker; i++ {
		workers[i] = NewWorker("shamir", 5, 2)
	}

	byte50 := make([]byte, 50)
	rand.Read(byte50)

	startTime := time.Now()
	for i := 0; i < numWorker; i++ {
		wg.Add(1)
		go func(w *SecretSharingWorker, buff []byte) {
			defer wg.Done()
			for j := 0; j < n; j++ {
				var result []SecretShare
				result, _, _ = w.SecretShareCommand(byte50)
				if result == nil {
					t.Error("empty result")
					break
				}
			}
		}(&workers[i], byte50)
	}

	wg.Wait()
	duration := time.Since(startTime)
	t.Logf("throughput for 50b value: %.5f ops/s, numWorker=%d", float64(n*numWorker)/duration.Seconds(), numWorker)
}

func TestSSMSWorkerThroughput(t *testing.T)  {
	numWorker := runtime.NumCPU()
	wg := sync.WaitGroup{}
	n := 1_000_000

	workers := make([]SecretSharingWorker, numWorker)
	for i := 0; i < numWorker; i++ {
		workers[i] = NewWorker("ssms", 5, 2)
	}

	byte50 := make([]byte, 50)
	rand.Read(byte50)

	startTime := time.Now()
	for i := 0; i < numWorker; i++ {
		wg.Add(1)
		go func(w *SecretSharingWorker, buff []byte) {
			defer wg.Done()
			for j := 0; j < n; j++ {
				var result []SecretShare
				result, _, _ = w.SecretShareCommand(byte50)
				if result == nil {
					t.Error("empty result")
					break
				}
			}
		}(&workers[i], byte50)
	}

	wg.Wait()
	duration := time.Since(startTime)
	t.Logf("throughput for 50b value: %.5f ops/s, numWorker=%d", float64(n*numWorker)/duration.Seconds(), numWorker)
}