package paxi

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/ailidani/paxi/log"
	"github.com/vmihailenco/msgpack/v5"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const ExecSpeedUpFactor = 1000

// EmulatedCommand is used for the emulated TPCC operation.
// This is sent by the client to the consensus leader, the leader will then
// execute the command (in emulation) before doing agreement on the state diffs.
// The state diffs are obtained from the prerecorded csv tracefile which the
// user provide with --req-tracefile <file-location> flag when running paxos or opaxos.
type EmulatedCommand struct {
	CommandID   uint32 `msgpack:"i"` // the CommandID read from the csv tracefile
	CommandType string `msgpack:"c"` // the possible CommandType are 'DELIVERY', 'NEW_ORDER', 'ORDER_STATUS', 'PAYMENT', or 'STOCK_LEVEL'
	Queries     string `msgpack:"q"` // Queries contains string of sql queries (transaction)
	SentAt      int64  `msgpack:"t"` // SentAt uses the unix nano representation of time
}

// EmulatedCommandData contains both the emulated command,
// the to-be emulated execution time, and the resulting state diffs
// after executing the command (in emulation).
type EmulatedCommandData struct {
	*EmulatedCommand
	ExecTime   time.Duration
	StateDiffs []EmulatedStateDiff
}

// EmulatedStateDiff contains the state diffs,
// recorded from pwrite syscall contained in the csv tracefile
type EmulatedStateDiff struct {
	FileID  uint32 // FileID is the recorded file descriptor from the tracefile
	Address string // Address of write op to the file
	Offset  uint32 // Offset from to write Address
	Length  uint32 // Length of data written at Address + Offset
}

// Serialize implement SerializableCommand interface for EmulatedCommand
func (c *EmulatedCommand) Serialize() []byte {
	ret, _ := msgpack.Marshal(c)
	return ret
}

// GetCommandType implement SerializableCommand interface for EmulatedCommand
func (c *EmulatedCommand) GetCommandType() byte {
	return TypeEmulatedCommand
}

// UnmarshalEmulatedCommand converts bytes into EmulatedCommand
// using msgpack
func UnmarshalEmulatedCommand(buffer []byte) (*EmulatedCommand, error) {
	cmd := &EmulatedCommand{}
	err := msgpack.Unmarshal(buffer, cmd)
	return cmd, err
}

// ReadEmulatedCommandsFromTracefile read a csv tracefile generated using
// https://github.com/fadhilkurnia/tpcc-tracefile-gen.
// The expected header: tx_id,tx_type,queries,exec_time(us),state_diff_list
func ReadEmulatedCommandsFromTracefile(tracefile string) (map[uint32]*EmulatedCommandData, error) {
	log.Infof("begin reading the csv tracefile %s", tracefile)
	expectedNumCols := 5
	f, err := os.Open(tracefile)
	if err != nil {
		log.Fatalf("failed to open the csv tracefile: %s", err)
		return nil, err
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	csvReader.Read()
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatalf("failed to parse the csv tracefile: %s", err)
		return nil, err
	}

	if len(records) == 0 || len(records[0]) != expectedNumCols {
		errStr := fmt.Sprintf("empty tracefile or wrong tracefile format")
		log.Fatal(errStr)
		return nil, errors.New(errStr)
	}

	ret := map[uint32]*EmulatedCommandData{}
	for i, r := range records {
		if len(r) != expectedNumCols {
			log.Fatalf("wrong tracefile format in row %d", i)
		}

		curCmd := &EmulatedCommandData{
			EmulatedCommand: &EmulatedCommand{},
			ExecTime:        0,
			StateDiffs:      nil,
		}

		txIDStr := r[0]
		txType := r[1]
		queries := r[2]
		execTimeStr := r[3]
		stateDiffsRaw := r[4]

		txID, errParse := strconv.ParseUint(txIDStr, 10, 32)
		if errParse != nil {
			log.Fatalf("failed to parse transaction id (%s) in row %d: %s", txIDStr, i, errParse)
		}
		if !isValidTxType(txType) {
			log.Fatalf("unexpected transaction type %s in row %d", txType, i)
		}
		execTime, errParse := strconv.ParseFloat(execTimeStr, 64)
		if errParse != nil {
			log.Fatalf("failed to parse execution time %s in row %d: %s", execTimeStr, i, errParse)
		}
		execTime = execTime / ExecSpeedUpFactor // speed up the execution by 1000 times
		execTimeDuration, err3 := time.ParseDuration(fmt.Sprintf("%fus", execTime))
		if err3 != nil {
			log.Fatalf("failed to parse execution time %s as duration in row %d: %s", execTimeStr, i, errParse)
		}

		if len(stateDiffsRaw) > 0 {
			stateDiffStr := strings.Split(stateDiffsRaw, "|")
			stateDiffs := make([]EmulatedStateDiff, len(stateDiffStr))
			for j, s := range stateDiffStr {
				parsedStateDiff, err2 := parseStateDiff(s)
				if err2 != nil {
					log.Fatalf("failed to parse state diff in row %d: %s", i, err2)
				}
				stateDiffs[j] = *parsedStateDiff
			}
			(*curCmd).StateDiffs = stateDiffs
		}

		curCmd.CommandID = uint32(txID)
		curCmd.CommandType = txType
		curCmd.Queries = queries
		curCmd.ExecTime = execTimeDuration

		ret[uint32(txID)] = curCmd
	}

	log.Infof("finish reading the csv tracefile")
	return ret, nil
}

func isValidTxType(txType string) bool {
	expectedTxTypes := []string{"DELIVERY", "NEW_ORDER", "ORDER_STATUS", "PAYMENT", "STOCK_LEVEL"}
	for _, t := range expectedTxTypes {
		if t == txType {
			return true
		}
	}
	return false
}

func parseStateDiff(stateDiffRaw string) (*EmulatedStateDiff, error) {
	// example of a state diff:
	// "file_id:3;address:0x561b105a0c78;length:4096;offset:33472512"
	members := strings.Split(stateDiffRaw, ";")
	if len(members) != 4 {
		return nil, errors.New("incorrect state diff, expecting 4 attributes")
	}

	// parsing file ID
	fileIDRaw := members[0]
	fileIDRawMem := strings.Split(fileIDRaw, ":")
	if len(fileIDRawMem) != 2 {
		return nil, errors.New("incorrect file_id in the state diff")
	}
	fileID, errParse := strconv.ParseUint(fileIDRawMem[1], 10, 32)
	if errParse != nil {
		return nil, errors.New(fmt.Sprintf("failed to parse file_id: %s", errParse))
	}

	// parsing address
	addressRaw := members[1]
	addressRawMem := strings.Split(addressRaw, ":")
	if len(addressRawMem) != 2 {
		return nil, errors.New("incorrect address in the state diff")
	}
	address := addressRawMem[1]

	// parsing length
	lengthRaw := members[2]
	lengthRawMem := strings.Split(lengthRaw, ":")
	if len(lengthRawMem) != 2 {
		return nil, errors.New("incorrect length in the state diff")
	}
	lengthStr := lengthRawMem[1]
	length, errParse := strconv.ParseUint(lengthStr, 10, 32)
	if errParse != nil {
		return nil, errors.New(fmt.Sprintf("failed to parse length: %s", errParse))
	}

	// parsing offset
	offsetRaw := members[3]
	offsetRawMem := strings.Split(offsetRaw, ":")
	if len(offsetRawMem) != 2 {
		return nil, errors.New("incorrect offset in the state diff")
	}
	offsetStr := offsetRawMem[1]
	offset, errParse := strconv.ParseUint(offsetStr, 10, 32)
	if errParse != nil {
		return nil, errors.New(fmt.Sprintf("failed to parse offset: %s", errParse))
	}

	ret := &EmulatedStateDiff{
		FileID:  uint32(fileID),
		Address: address,
		Offset:  uint32(offset),
		Length:  uint32(length),
	}

	return ret, nil
}

// GenerateStateDiffsData converts []StateDiffs which each
// only contains (address, offset, and length) into an actual random state diff
func (c *EmulatedCommandData) GenerateStateDiffsData() []byte {
	byteLen := uint32(0)
	for _, diff := range c.StateDiffs {
		byteLen += 4           // the address: 32-bit pointer (4 bytes)
		byteLen += 4           // the file id: 32-bit pointer (4 bytes)
		byteLen += 4           // the offset : 32-bit int (4 bytes)
		byteLen += 4           // the length : 32-bit int (4 bytes)
		byteLen += diff.Length // the actual data
	}
	stateDiffData := make([]byte, byteLen)
	rand.Read(stateDiffData)
	return stateDiffData
}
