# Illumio-Assessment
This repo contains the code for the take home assessment. The script uses a lookup table to parse the VPC flow logs and extract the tag counts and port-protocol counts.

## Usage
```
python3 main.py -i <log file> -l <lookup table>

 - logfile - This is the vpc flow log file which is in tsv format.
 - lookup - This is the lookup table file which is in csv format.
```

## Assumptions
- The protocol is lowercase string in the lookup table and logs.
- The lookup table has unique port, protocol combinations. That is no two port-portocol combinations are present.
