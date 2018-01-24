/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;


public class HdfsNameServiceResolver {
    private static final Logger              LOG                       = LoggerFactory.getLogger(HdfsNameServiceResolver.class);
    private static final String              NS_ID_FOR_PATH            = "nsIdForPath$";
    private static final String              PATH_WITH_NSID            = "pathWithNSId$";
    private final        Map<String, String> reverseNameServiceMapping = new HashMap<>();

    // Need non-final instance in order initialize the logger first
    private static HdfsNameServiceResolver INSTANCE;

    // Cached results for faster subsequent lookups
    private static final ThreadLocal<Map<String, String>> cache = ThreadLocal.withInitial(HashMap::new);

    private HdfsNameServiceResolver() {
        init(new HdfsConfiguration(true));
    }

    public static HdfsNameServiceResolver getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new HdfsNameServiceResolver();
        }
        return INSTANCE;
    }

    public String getNameServiceID(String host, String port) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HDFSUtil.getNameServiceID({}, {})", host, port);
        }

        String ret = reverseNameServiceMapping.getOrDefault(host + ":" + port, "");

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HDFSUtil.getNameServiceID({}, {}) = {}", host, port, ret);
        }

        return ret;
    }

    public String getNameServiceID(String host) {
        return getNameServiceID(host, Constants.DEFAULT_PORT);
    }

    public String getPathWithNameServiceID(String path) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HdfsNameServiceResolver.getPathWithNameServiceID({})", path);
        }

        String ret = path;

        // Only handle URLs that begin with hdfs://
        if (path.indexOf(Constants.HDFS_SCHEME) == 0) {
            Optional<String> cacheResult = cacheLookup(PATH_WITH_NSID + path);

            if (cacheResult.isPresent()) {
                ret = cacheResult.get();
            } else {
                URI uri = new Path(path).toUri();

                String nsId;
                if (uri.getPort() != -1) {
                    nsId = reverseNameServiceMapping.get(uri.getAuthority());
                } else {
                    nsId = reverseNameServiceMapping.get(uri.getHost() + ":" + Constants.DEFAULT_PORT);
                }

                if (nsId != null) {
                    ret = path.replace(uri.getAuthority(), nsId);
                }

                cache(PATH_WITH_NSID + path, ret);
            }
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HdfsNameServiceResolver.getPathWithNameServiceID()");
        }

        return ret;
    }

    public String getNameServiceIDForPath(String path) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HdfsNameServiceResolver.getNameServiceIDForPath({})", path);
        }

        String ret = "";

        // Only handle path URLs that begin with hdfs://
        if (path.indexOf(Constants.HDFS_SCHEME) == 0) {
            Optional<String> cacheResult = cacheLookup(NS_ID_FOR_PATH + path);

            if (cacheResult.isPresent()) {
                ret = cacheResult.get();
            } else {
                try {
                    URI uri = new Path(path).toUri();

                    if (uri != null) {
                        // URI can contain host and port
                        if (uri.getPort() != -1) {
                            ret = getNameServiceID(uri.getHost(), String.valueOf(uri.getPort()));
                        } else {
                            // No port information present, it means the path might contain only host or the nameservice id itself
                            // Try resolving using default port
                            ret = getNameServiceID(uri.getHost(), Constants.DEFAULT_PORT);
                            // If not resolved yet, then the path must contain nameServiceId
                            if (StringUtils.isEmpty(ret) && reverseNameServiceMapping.containsValue(uri.getHost())) {
                                ret = uri.getHost();
                            }
                        }

                        // Cache the result for quick lookup on subsequent calls
                        cache(NS_ID_FOR_PATH + path, ret);
                    }
                } catch (IllegalArgumentException ignored) {
                    // No need to do anything
                }
            }
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HdfsNameServiceResolver.getNameServiceIDForPath() : {}", ret);
        }

        return ret;
    }

    private void init(final HdfsConfiguration hdfsConfiguration) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HDFSUtil.init()");
        }

        // Determine all available nameServiceIDs
        String[] nameServiceIDs = hdfsConfiguration.getTrimmedStrings(Constants.HDFS_NAMESERVICE_PROPERTY_KEY);
        if (Objects.isNull(nameServiceIDs) || nameServiceIDs.length == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("NSID not found for {}, looking under {}", Constants.HDFS_NAMESERVICE_PROPERTY_KEY, Constants.HDFS_INTERNAL_NAMESERVICE_PROPERTY_KEY);
            }
            // Attempt another lookup using internal name service IDs key
            nameServiceIDs = hdfsConfiguration.getTrimmedStrings(Constants.HDFS_INTERNAL_NAMESERVICE_PROPERTY_KEY);
        }

        if (Objects.nonNull(nameServiceIDs) && nameServiceIDs.length > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("NSIDs = {}", nameServiceIDs);
            }

            for (String nameServiceID : nameServiceIDs) {
                // Find NameNode addresses and map to the NameServiceID
                String[] nameNodes = hdfsConfiguration.getTrimmedStrings(Constants.HDFS_NAMENODES_HA_NODES_PREFIX + nameServiceID);
                for (String nameNode : nameNodes) {
                    String nameNodeMappingKey = String.format(Constants.HDFS_NAMENODE_ADDRESS_TEMPLATE, nameServiceID, nameNode);
                    String nameNodeAddress    = hdfsConfiguration.get(nameNodeMappingKey, "");

                    // Add a mapping only if found
                    if (StringUtils.isNotEmpty(nameNodeAddress)) {
                        reverseNameServiceMapping.put(nameNodeAddress, nameServiceID);
                    }
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No NSID could be resolved");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HDFSUtil.init()");
        }
    }

    private void cache(String key, String val) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HdfsNameServiceResolver.cache({}, {})", key, val);
        }

        cache.get().put(key, val);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HdfsNameServiceResolver.cache()");
        }
    }

    private Optional<String> cacheLookup(String key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HdfsNameServiceResolver.cacheLookup({})", key);
        }

        String ret = cache.get().get(key);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HdfsNameServiceResolver.cacheLookup() : {}", ret);
        }
        return Optional.ofNullable(ret);
    }

    public static class Constants {
        public static final String DEFAULT_PORT                           = "8020";
        public static final String HDFS_SCHEME                            = "hdfs://";
        public static final String HDFS_NAMESERVICE_PROPERTY_KEY          = "dfs.nameservices";
        public static final String HDFS_INTERNAL_NAMESERVICE_PROPERTY_KEY = "dfs.internal.nameservices";
        public static final String HDFS_NAMENODES_HA_NODES_PREFIX         = "dfs.ha.namenodes.";
        public static final String HDFS_NAMENODE_ADDRESS_TEMPLATE         = "dfs.namenode.rpc-address.%s.%s";
    }
}
