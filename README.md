# Illumio-Assessment
This repo contains the code for the take home assessment.

## Description
The script uses a lookup table to parse the VPC flow logs and extract the tag counts and port-protocol counts. The repo also contains the script to generate and test the log parser with synthetic data.

## Usage
- To test the script with generated lookup and flow logs -
```
python3 test_and_assert.py
```
- To Run the script with custom lookup table and log file -
```
python3 main.py -i <log file> -l <lookup table>

 - logfile - This is the vpc flow log file which is in tsv format.
 - lookup - This is the lookup table file which is in csv format.
```

## Assumptions
- The protocol is lowercase string in the lookup table and logs.
- The lookup table has unique port, protocol combinations. That is no two same port-protocol combinations are present.
- Lookup table csv format -
```
dstport,dstprotocol,tag
22,tcp,sv_P1
23,udp,SV_P2
45,tcp,sv_P2
5369,tcp,sv_p3
22,udp,SV_P2
```
- VPC flow log tsv format is based on the VPC flow log format of AWS mentioned [here]( https://docs.aws.amazon.com/vpc/latest/userguide/flow-log-records.html). The 6 and 7 column contain the dstport and protocol respectively.
```
2   123456789010    eni-1235b8ca123456789   172.31.16.139   172.31.16.21    20641   22  tcp 20  4249    1418530010  1418530070  ACCEPT  OK
```

## Output
The script generates two output files.
- The `flow_log_tag_insights.tsv` file, which contains the tags and the corresponding match counts from the flow logs.
```
Tag	Count
SV_P2	1
UNTAGGED	4
sv_P1	1
```
- The `flow_log_port_protocol_insights.tsv` file, which contains the port-protocol combination and the corresponding match counts from the flow logs.
```
Port	Protocol	Count
22	tcp	1
23	udp	1
25	tcp	1
68	udp	1
2220	tcp	1
5639	tcp	1
```

## Profiling
- Used the gtime library on Mac to profile the parser using the `test_and_assert.py` script.
> The script generates a log_file of close to 14MB with about 100000 Lines.

> The test was run on an M1 Mac with 32 GB memory.
```
    gtime -v python3 test_and_assert.py
    
	Command being timed: "python3 test_and_assert.py"
	User time (seconds): 0.52
	System time (seconds): 0.29
	Percent of CPU this job got: 74%
	Elapsed (wall clock) time (h:mm:ss or m:ss): 0:01.10
	Average shared text size (kbytes): 0
	Average unshared data size (kbytes): 0
	Average stack size (kbytes): 0
	Average total size (kbytes): 0
	Maximum resident set size (kbytes): 22528
	Average resident set size (kbytes): 0
	Major (requiring I/O) page faults: 96
	Minor (reclaiming a frame) page faults: 1820
	Voluntary context switches: 923
	Involuntary context switches: 753
	Swaps: 0
	File system inputs: 0
	File system outputs: 0
	Socket messages sent: 0
	Socket messages received: 0
	Signals delivered: 0
	Page size (bytes): 16384
	Exit status: 0
```
