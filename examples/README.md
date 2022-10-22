# Transcoder examples

*_Note_: Some of the examples below assume that [gcloud](https://cloud.google.com/sdk/docs/install), [jq](https://stedolan.github.io/jq/download/), and [wget](https://www.gnu.org/software/wget/) are installed on the system.*

## Ingest the contents of a CME Group Datamine packet capture file into BigQuery

This example extracts and transcodes messages embedded within a CME Group Datamine packet capture file. A CME Group SBE schema is used to decode the messages. Console output of decoded messages is suppressed. The schema and output files are written to BigQuery. Please note that usage of CME Group data is subject to your licensing agreement with CME Group.


### CLI

```
bin/txcode  \
  --factory cme \
  --schema_file templates_FixBinary_v12.xml \
  --source_file 314-20221007-e \
  --source_file_format_type cme_binary_packet \
  --continue_on_error \
  --message_skip_bytes 2 \
  --source_file_endian little \
  --output_type bigquery \
  --quiet \
  --message_type_inclusions MDIncrementalRefreshBook46 \
  --destination_project_id $(gcloud config get-value project) \
  --destination_dataset_id datamine \
  --lazy_create_resources
```

### Show table in BigQuery

```
bq ls datamine
```

### Output

```
           tableId             Type    Labels   Time Partitioning   Clustered Fields  
 ---------------------------- ------- -------- ------------------- ------------------ 
  MDIncrementalRefreshBook46   TABLE      
```

### Sample records from BigQuery
```
bq --format=json head datamine.MDIncrementalRefreshBook46 | head -1 | jq .[0]
```

### Output

```
{
  "match_event_indicator": "LastQuoteMsg",
  "no_md_entries": [
    {
      "md_entry_px": {
        "exponent": "-9",
        "mantissa": "11173000000000"
      },
      "md_entry_size": "43",
      "md_entry_type": "Offer",
      "md_price_level": "7",
      "md_update_action": "Change",
      "number_of_orders": "16",
      "rpt_seq": "6011055",
      "security_id": "190915",
      "tradeable_size": "0"
    }
  ],
  "no_order_id_entries": [
    {
      "md_display_qty": "1",
      "md_order_priority": "353701821378.0",
      "order_id": "8813654790160.0",
      "order_update_action": "Update",
      "reference_id": "1"
    }
  ],
  "transact_time": "1.6651188000004639e+18"
}
```

## Process messages from a packet capture file (PCAP)

This example extracts and transcodes messages embedded within a PCAP file. A CME Group SBE schema is used to decode the messages. The first 16 bytes of each payload  is stripped off before the decoded message is manufactured by and returned from the message factory. The schema and output files are written locally in Avro format. Please note that usage of CME Group data is subject to your licensing agreement with CME Group.

### CLI

```
# download SBE template XML
wget 'ftp://ftp.cmegroup.com/SBEFix/Production/Templates/templates_FixBinary_v12.xml'

# download example PCAP to local file system
wget 'https://github.com/Open-Markets-Initiative/Data/blob/main/Cme/Mdp3.Sbe.v1.12/SnapshotFullRefreshTcpLongQty.68.Tcp.pcap?raw=true' -O SnapshotFullRefreshTcpLongQty.pcap

bin/txcode \
  --source_file SnapshotFullRefreshTcpLongQty.pcap \         
  --schema_file templates_FixBinary_v12.xml \                
  --factory cme \                                            
  --source_file_format_type pcap \                           
  --message_skip_bytes 16 \                                  
  --message_type_inclusions SnapshotFullRefreshTCPLongQty68 \
  --output_path avroOut \                                    
  --lazy_create_resources                                    
```

### Output

```
{"transact_time": 1631080313799842417, "match_event_indicator": "EndOfEvent", "security_id": 11827320, "high_limit_price": {"mantissa": null, "exponent": -9}, "low_limit_price": {"mantissa": 50000, "exponent": -9}, "max_price_variation": {"mantissa": 3000000, "exponent": -9}, "no_md_entries": [{"md_entry_px": {"mantissa": 1183650000, "exponent": -9}, "md_entry_size": 1000000, "number_of_orders": null, "md_price_level": 1, "open_close_settl_flag": null, "md_entry_type": "Market Best Bid"}, {"md_entry_px": {"mantissa": 1183750000, "exponent": -9}, "md_entry_size": 1000000, "number_of_orders": null, "md_price_level": 1, "open_close_settl_flag": null, "md_entry_type": "Market Best Offer"}, {"md_entry_px": {"mantissa": 1183650000, "exponent": -9}, "md_entry_size": 1000000, "number_of_orders": null, "md_price_level": 1, "open_close_settl_flag": null, "md_entry_type": "Bid"}, {"md_entry_px": {"mantissa": 1180000000, "exponent": -9}, "md_entry_size": 1000000, "number_of_orders": null, "md_price_level": 2, "open_close_settl_flag": null, "md_entry_type": "Bid"}, {"md_entry_px": {"mantissa": 1183750000, "exponent": -9}, "md_entry_size": 1000000, "number_of_orders": null, "md_price_level": 1, "open_close_settl_flag": null, "md_entry_type": "Offer"}, {"md_entry_px": {"mantissa": 1207900000, "exponent": -9}, "md_entry_size": 2000000, "number_of_orders": null, "md_price_level": 2, "open_close_settl_flag": null, "md_entry_type": "Offer"}, {"md_entry_px": {"mantissa": 1208000000, "exponent": -9}, "md_entry_size": 2000000, "number_of_orders": null, "md_price_level": 3, "open_close_settl_flag": null, "md_entry_type": "Offer"}, {"md_entry_px": {"mantissa": 1208050000, "exponent": -9}, "md_entry_size": 17000000, "number_of_orders": null, "md_price_level": 4, "open_close_settl_flag": null, "md_entry_type": "Offer"}, {"md_entry_px": {"mantissa": 1208100000, "exponent": -9}, "md_entry_size": 13000000, "number_of_orders": null, "md_price_level": 5, "open_close_settl_flag": null, "md_entry_type": "Offer"}, {"md_entry_px": {"mantissa": 1207300000, "exponent": -9}, "md_entry_size": 0, "number_of_orders": null, "md_price_level": null, "open_close_settl_flag": null, "md_entry_type": "Trade"}]}
INFO:root:Message type inclusions: ['SnapshotFullRefreshTCPLongQty68']
INFO:root:Source record count: 1
INFO:root:Processed record count: 1
INFO:root:Processed schema count: 1
INFO:root:Summary of message counts: {'SnapshotFullRefreshTCPLongQty68': 1}
INFO:root:Summary of error message counts: {}
INFO:root:Message rate: 151.998784 per second
INFO:root:Total runtime in seconds: 0.006579
INFO:root:Total runtime in minutes: 0.00011
```

### View transcoded Avro
```
# download JAR to inspect Avro files 
wget https://dlcdn.apache.org/avro/stable/java/avro-tools-1.11.1.jar
java -jar avro-tools-1.11.1.jar tojson --pretty avroOut/SnapshotFullRefreshTcpLongQty.68.Tcp-SnapshotFullRefreshTCPLongQty68.avro 
```

### Output
```
{
  "transact_time" : {
    "double" : 1.6310803137998423E18
  },
  "match_event_indicator" : {
    "string" : "EndOfEvent"
  },
  "security_id" : {
    "long" : 11827320
  },
  "high_limit_price" : {
    "mantissa" : null,
    "exponent" : {
      "int" : -9
    }
  },
  "low_limit_price" : {
    "mantissa" : {
      "long" : 50000
    },
    "exponent" : {
      "int" : -9
    }
  },
  "max_price_variation" : {
    "mantissa" : {
      "long" : 3000000
    },
    "exponent" : {
      "int" : -9
    }
  },
  "no_md_entries" : {
    "array" : [ {
      "md_entry_px" : {
        "mantissa" : {
          "long" : 1183650000
        },
        "exponent" : {
          "int" : -9
        }
      },
      "md_entry_size" : {
        "double" : 1000000.0
      },
      "number_of_orders" : null,
      "md_price_level" : {
        "int" : 1
      },
      "open_close_settl_flag" : null,
      "md_entry_type" : {
        "string" : "Market Best Bid"
      }
    }, {
      "md_entry_px" : {
        "mantissa" : {
          "long" : 1183750000
        },
        "exponent" : {
          "int" : -9
        }
      },
      "md_entry_size" : {
        "double" : 1000000.0
      },
      "number_of_orders" : null,
      "md_price_level" : {
        "int" : 1
      },
      "open_close_settl_flag" : null,
      "md_entry_type" : {
        "string" : "Market Best Offer"
      }
    }, {
      "md_entry_px" : {
        "mantissa" : {
          "long" : 1183650000
        },
        "exponent" : {
          "int" : -9
        }
      },
      "md_entry_size" : {
        "double" : 1000000.0
      },
      "number_of_orders" : null,
      "md_price_level" : {
        "int" : 1
      },
      "open_close_settl_flag" : null,
      "md_entry_type" : {
        "string" : "Bid"
      }
    }, {
      "md_entry_px" : {
        "mantissa" : {
          "long" : 1180000000
        },
        "exponent" : {
          "int" : -9
        }
      },
      "md_entry_size" : {
        "double" : 1000000.0
      },
      "number_of_orders" : null,
      "md_price_level" : {
        "int" : 2
      },
      "open_close_settl_flag" : null,
      "md_entry_type" : {
        "string" : "Bid"
      }
    }, {
      "md_entry_px" : {
        "mantissa" : {
          "long" : 1183750000
        },
        "exponent" : {
          "int" : -9
        }
      },
      "md_entry_size" : {
        "double" : 1000000.0
      },
      "number_of_orders" : null,
      "md_price_level" : {
        "int" : 1
      },
      "open_close_settl_flag" : null,
      "md_entry_type" : {
        "string" : "Offer"
      }
    }, {
      "md_entry_px" : {
        "mantissa" : {
          "long" : 1207900000
        },
        "exponent" : {
          "int" : -9
        }
      },
      "md_entry_size" : {
        "double" : 2000000.0
      },
      "number_of_orders" : null,
      "md_price_level" : {
        "int" : 2
      },
      "open_close_settl_flag" : null,
      "md_entry_type" : {
        "string" : "Offer"
      }
    }, {
      "md_entry_px" : {
        "mantissa" : {
          "long" : 1208000000
        },
        "exponent" : {
          "int" : -9
        }
      },
      "md_entry_size" : {
        "double" : 2000000.0
      },
      "number_of_orders" : null,
      "md_price_level" : {
        "int" : 3
      },
      "open_close_settl_flag" : null,
      "md_entry_type" : {
        "string" : "Offer"
      }
    }, {
      "md_entry_px" : {
        "mantissa" : {
          "long" : 1208050000
        },
        "exponent" : {
          "int" : -9
        }
      },
      "md_entry_size" : {
        "double" : 1.7E7
      },
      "number_of_orders" : null,
      "md_price_level" : {
        "int" : 4
      },
      "open_close_settl_flag" : null,
      "md_entry_type" : {
        "string" : "Offer"
      }
    }, {
      "md_entry_px" : {
        "mantissa" : {
          "long" : 1208100000
        },
        "exponent" : {
          "int" : -9
        }
      },
      "md_entry_size" : {
        "double" : 1.3E7
      },
      "number_of_orders" : null,
      "md_price_level" : {
        "int" : 5
      },
      "open_close_settl_flag" : null,
      "md_entry_type" : {
        "string" : "Offer"
      }
    }, {
      "md_entry_px" : {
        "mantissa" : {
          "long" : 1207300000
        },
        "exponent" : {
          "int" : -9
        }
      },
      "md_entry_size" : {
        "double" : 0.0
      },
      "number_of_orders" : null,
      "md_price_level" : null,
      "open_close_settl_flag" : null,
      "md_entry_type" : {
        "string" : "Trade"
      }
    } ]
  }
}
```

## Ingest the contents of an ASX 24 Derivatives MDP Historical Binary file into BigQuery

This example extracts and transcodes messages embedded within an ASX 24 Derivatives MDP Historical Binary file. An ASX MDP schema is used to decode the messages. Console output of decoded messages is suppressed. The schema and output files are written to BigQuery. Please note that usage of ASX Datasphere data is subject to your licensing agreement with ASX.

### CLI

```
bin/txcode  \
  --factory asx \
  --schema_file asx-mdp.xml \
  --source_file NTP_220306_1646551229.log \
  --source_file_format_type length_delimited \
  --continue_on_error \
  --source_file_endian little \
  --output_type bigquery \
  --output_encoding binary \
  --quiet \
  --message_type_inclusions future_symbol_directory \
  --destination_project_id $PROJECT_ID \
  --destination_dataset_id asx_datasphere \
  --message_handlers TimestampPullForwardHandler
```

### Output
```
INFO:root:Message type inclusions: ['future_symbol_directory']
INFO:root:Source record count: 37404855
INFO:root:Processed record count: 2999
INFO:root:Processed schema count: 1
INFO:root:Summary of message counts: {'future_symbol_directory': 2999}
INFO:root:Summary of error message counts: {'future_symbol_directory': 17}
INFO:root:Message rate: 50223.824304 per second
INFO:root:Total runtime in seconds: 744.763178
INFO:root:Total runtime in minutes: 12.41272
```

### Show table in BigQuery

```
bq ls asx_datasphere
```

### Output
```
          tableId           Type    Labels   Time Partitioning   Clustered Fields  
 ------------------------- ------- -------- ------------------- ------------------ 
  future_symbol_directory   TABLE
```

### Sample records from BigQuery
```
bq --format=json head asx_datasphere.future_symbol_directory | head -1| jq .[0]
```

### Output
```
{
  "symbol_name": "APH2",
  "timestamp": "278724000",
  "timestamp_seconds": null,
  "trade_date": "19058",
  "tradeable_instrument_id": "196081"
}
```

## Publish FIX messages stored in a file to Pub/Sub as Avro binary

This example reads a file containing a single FIX message per line and a FIX schema XML file, creates topics in Pub/Sub for a single message type, then publishes each message in the file to the corresponding topic. A JSON representation of the transcoded message is sent to the console.

### CLI

```
wget 'https://raw.githubusercontent.com/SunGard-Labs/fix2json/master/testfiles/42_order_single.txt'
wget 'https://raw.githubusercontent.com/SunGard-Labs/fix2json/master/dict/FIX42.xml'
bin/txcode \
  --source_file 42_order_single.txt \
  --schema_file ../fix2json/dict/FIX42.xml \
  --factory fix \
  --source_file_format_type line_delimited \
  --destination_project_id $(gcloud config get-value project) \
  --continue_on_error \
  --output_type pubsub \
  --lazy_create_resources \
  --message_type_inclusions NewOrderSingle
```

### Output

```
{"BeginString": "FIX.4.2", "BodyLength": 153, "MsgType": "ORDER_SINGLE", "SenderCompID": "BLP", "TargetCompID": "SCHB", "MsgSeqNum": 1, "SenderSubID": "30737", "PossResend": "YES", "SendingTime": "20000809-20:20:50", "ClOrdID": "90001008", "Account": "10030003", "HandlInst": "AUTOMATED_EXECUTION_ORDER_PUBLIC_BROKER_INTERVENTION_OK", "Symbol": "TESTA", "Side": "BUY", "OrderQty": 4000.0, "OrdType": "LIMIT", "TimeInForce": "DAY", "Price": 30.0, "Rule80A": "INDIVIDUAL_INVESTOR_SINGLE_ORDER", "TransactTime": "20000809-18:20:32", "CheckSum": "061"}
INFO:root:Message type inclusions: ['NewOrderSingle']
INFO:root:Source record count: 1
INFO:root:Processed record count: 1
INFO:root:Processed schema count: 2
INFO:root:Summary of message counts: {'NewOrderSingle': 1}
INFO:root:Summary of error message counts: {}
INFO:root:Message rate: 8.435331 per second
INFO:root:Total runtime in seconds: 0.118549
INFO:root:Total runtime in minutes: 0.001976
```

### Validate the topic for NewOrderSingle was created

```
gcloud pubsub topics list --format json | jq .[].name | grep NewOrderSingle |tr -d \" | cut -f4 -d/
```


## Create a topic from a schema, subscribe to the topic, publish JSON-encoded Avro to the topic

### Manufacture topic

```
bin/txcode \
  --source_file /dev/null  \
  --schema_file ../fix2json/dict/FIX42.xml \
  --factory fix \
  --source_file_format_type line_delimited \
  --destination_project_id $(gcloud config get-value project) \
  --continue_on_error \
  --output_encoding json \
  --output_type pubsub \
  --message_type_inclusions NewOrderSingle
```

### Subscribe to topic

```
npm install -g pulltop
pulltop NewOrderSingle
```

### Transcode and publish
```
bin/txcode \
  --source_file ../fix2json/testfiles/42_order_single.txt \
  --schema_file ../fix2json/dict/FIX42.xml \
  --factory fix \
  --source_file_format_type line_delimited \
  --destination_project_id $(gcloud config get-value project) \
  --continue_on_error \
  --output_type pubsub \
  --output_encoding json \
  --lazy_create_resources \
  --message_type_inclusions NewOrderSingle
```

Upon the transcoder's publishing of the message, the payload contents are displayed in the console running `pulltop`.
