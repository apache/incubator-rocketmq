#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
cmake_minimum_required(VERSION 2.6.4)

project(rocketmq CXX)

if (POLICY CMP0003)
    cmake_policy(SET CMP0003 NEW)
endif ()

if (POLICY CMP0042)
    cmake_policy(SET CMP0042 NEW)
endif ()

if (POLICY CMP0064)
    cmake_policy(SET CMP0064 NEW)
endif ()

find_package(JNI REQUIRED)

include_directories(SYSTEM ${JNI_INCLUDE_DIRS})

add_library(rocketmq_util_all SHARED org_apache_rocketmq_common_UtilAll.cpp)
target_link_libraries(rocketmq_util_all ${JAVA_JVM_LIBRARY})
set_target_properties(rocketmq_util_all
                      PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../../resources)