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
