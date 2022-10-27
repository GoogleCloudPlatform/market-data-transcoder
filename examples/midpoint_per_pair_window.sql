#!/bin/bash
#
# Copyright 2022 Google LLC
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

WITH
  best_bid_offer AS (
  SELECT
    MDReqID,
    RecordTemplate,
    NoMDEntries[
  OFFSET
    (0)].Symbol,
    NoMDEntries[
  OFFSET
    (0)].SecurityIDSource,
    NoMDEntries[
  OFFSET
    (0)].SecurityID,
    NoMDEntries[
  OFFSET
    (2)].MDEntryPx,
    DATETIME(CAST( NoMDEntries[
      OFFSET
        (0)].MDEntryDate AS DATE FORMAT 'YYYYMMDD'), CAST(NoMDEntries[
      OFFSET
        (0)].MDEntryTime AS TIME FORMAT 'HH24:MI:SS') ) transact_time
  FROM
    `icap.MarketDataIncrementalRefresh`
  WHERE
    ARRAY_LENGTH(NoMDEntries)=3)
SELECT
  Symbol,
  ARRAY_AGG(bbo.MDEntryPx
  ORDER BY
    transact_time ASC
  LIMIT
    1)[
OFFSET
  (0)] AS open,
  MAX(bbo.MDEntryPx) high,
  MIN(bbo.MDEntryPx) low,
  ARRAY_AGG(bbo.MDEntryPx
  ORDER BY
    transact_time DESC
  LIMIT
    1)[
OFFSET
  (0)] AS close
FROM
  best_bid_offer bbo
WHERE
  transact_time BETWEEN '2022-10-04T12:28:00'
  AND '2022-10-04T13:28:00'
GROUP BY
  bbo.Symbol
ORDER BY
  bbo.Symbol
