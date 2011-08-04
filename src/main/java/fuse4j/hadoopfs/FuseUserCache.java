package fuse4j.hadoopfs;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import fuse.PasswordEntry;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

class FuseUserCache implements UserCache {
    private final Charset charset;
    private Map<String, PasswordEntry> users = new HashMap<String, PasswordEntry>();
    private Map<Integer, PasswordEntry> uids = new HashMap<Integer, PasswordEntry>();

    FuseUserCache() {
        this.charset = Charset.forName("UTF8");
    }

    private synchronized PasswordEntry lookupUid(String name) {
        PasswordEntry entry = users.get(name);

        if(!users.containsKey(name)) {
            entry = PasswordEntry.lookupByUsername(charset, name);
            users.put(name, entry);
        }

        return entry;
    }

    private synchronized PasswordEntry lookupUsername(int uid) {
      PasswordEntry entry = uids.get(uid);

      if(!uids.containsKey(uid)) {
          entry = PasswordEntry.lookupByUid(charset, uid);
          uids.put(uid, entry);
      }

      return entry;
    }

    @Override
    public int getUid(String name) throws Exception {
        final PasswordEntry entry = lookupUid(name);

        if(entry == null) {
          throw new Exception ("Unknown username:" + name);
        }

        return entry.uid;
    }

    @Override
    public String getUsername(int uid) throws Exception {
      final PasswordEntry entry = lookupUsername(uid);

      if(entry == null) {
          throw new Exception ("Unknown uid:" + uid);
      }

      return entry.username;
    }
}
