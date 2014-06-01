##_DISCLAIMER_
#### This project is still under construction. Beta testers are more than welcome.

# LevelCache

A Big, Fast, In-Memory Key/Value Cache backed by Java OffHeap Memory, based on a variant of LSM([Log Structured Merge Tree](http://en.wikipedia.org/wiki/Log-structured_merge-tree)) algorithm, inspired by [Google LevelDB](http://code.google.com/p/leveldb/).

For a similar cache with persistence support, please refer to [SessDB](https://github.com/ctriposs/sessdb).


## Feature Highlight:
1. **High Read/Write Performance**: read/write performance close to O(1) direct memory access, tailored for session data scenarios, also suitable for caching data scenarios.
2. **Efficient Memory Usage**: uses only a small amount of heap memory, leverages a hierarchical storage mechanism, only a small amount of most recently inserted fresh keys reside on heap memory, a big amount of key/value data resides on offheap memory. hierarchical sotarge ensures high read/write performance, while heap GC has no big performance impact.
3. **Thread Safe**: supporting multi-threads concurrent and non-blocking access.
4. **Expiration & Compaction**: automatic expired and deleted data cleanup, avoiding memory space waste.
5. **Light in Design & Implementation**: simple Map like interface, only supports Get/Put/Delete operations, cross platform Java based, small codebase size, embeddable.

## Performance Highlight:
TODO


## The Architecture
![levelcache architecture](https://raw.githubusercontent.com/ctriposs/levelcache/master/doc/lcache_arch.png)


## How to Use
TODO

* Sample Usage
TODO


## Docs
TODO


## Version History
TODO

##Copyright and License
Copyright 2014 ctriposs

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in compliance with the License. You may obtain a copy of the License in the LICENSE file, or at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 