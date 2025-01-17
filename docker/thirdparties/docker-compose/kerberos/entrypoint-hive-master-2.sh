#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

echo "Copying kerberos keytabs to /keytabs/"
mkdir -p /etc/hadoop-init.d/
cp /etc/trino/conf/hive-presto-master.keytab /keytabs/other-hive-presto-master.keytab
cp /etc/trino/conf/presto-server.keytab /keytabs/other-presto-server.keytab
cp /keytabs/update-location.sh /etc/hadoop-init.d/update-location.sh
/usr/local/hadoop-run.sh &

# check healthy hear
echo "Waiting for hadoop to be healthy"

for i in {1..10}; do
    if /usr/local/health.sh; then
        echo "Hadoop is healthy"
        break
    fi
    echo "Hadoop is not healthy yet. Retrying in 20 seconds..."
    sleep 20
done

if [ $i -eq 10 ]; then
    echo "Hadoop did not become healthy after 120 attempts. Exiting."
    exit 1
fi

echo "Init kerberos test data"
kinit -kt /etc/hive/conf/hive.keytab hive/hadoop-master-2@OTHERREALM.COM
hive  -f /usr/local/sql/create_kerberos_hive_table.sql
touch /mnt/SUCCESS

tail -f /dev/null
